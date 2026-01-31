"""
Cliente de API para QuickBooks Online
Maneja paginacion, rate limits, reintentos y extraccion de datos
"""
import requests
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Generator, Optional

from utils.qbo_auth import get_qbo_authenticator


class QBOClient:
    """
    Cliente para interactuar con la API de QuickBooks Online
    Implementa paginacion, rate limiting y reintentos con backoff exponencial
    """

    # Configuracion de rate limiting y reintentos
    MAX_RETRIES = 5
    INITIAL_BACKOFF = 1  # segundos
    MAX_BACKOFF = 60  # segundos
    PAGE_SIZE = 100  # maximo permitido por QBO

    # Limite de requests por minuto (QBO permite ~500/minuto)
    RATE_LIMIT_REQUESTS = 400
    RATE_LIMIT_WINDOW = 60  # segundos

    def __init__(self):
        """Inicializa el cliente con autenticador"""
        self.auth = get_qbo_authenticator()
        self.request_timestamps: List[float] = []
        self.total_requests = 0
        self.total_retries = 0

    @property
    def base_url(self):
        """URL base de la API"""
        return f"{self.auth.api_base_url}/v3/company/{self.auth.realm_id}"

    def _wait_for_rate_limit(self):
        """
        Espera si es necesario para respetar el rate limit
        Implementa ventana deslizante
        """
        now = time.time()
        # Limpiar timestamps viejos (fuera de la ventana)
        self.request_timestamps = [
            ts for ts in self.request_timestamps
            if now - ts < self.RATE_LIMIT_WINDOW
        ]

        if len(self.request_timestamps) >= self.RATE_LIMIT_REQUESTS:
            # Calcular tiempo de espera
            oldest = min(self.request_timestamps)
            wait_time = self.RATE_LIMIT_WINDOW - (now - oldest) + 1
            if wait_time > 0:
                print(f"[RATE LIMIT] Esperando {wait_time:.1f}s para respetar limites...")
                time.sleep(wait_time)

        self.request_timestamps.append(time.time())

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Realiza una request con reintentos y backoff exponencial

        Args:
            endpoint: Endpoint de la API (ej: /query)
            params: Parametros de la query

        Returns:
            dict: Respuesta JSON de la API

        Raises:
            Exception: Si se agotan los reintentos
        """
        url = f"{self.base_url}{endpoint}"
        backoff = self.INITIAL_BACKOFF

        for attempt in range(self.MAX_RETRIES):
            self._wait_for_rate_limit()

            try:
                headers = self.auth.get_headers()
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=60
                )

                self.total_requests += 1

                # Exito
                if response.status_code == 200:
                    return response.json()

                # Rate limit excedido (429) o error temporal del servidor (5xx)
                if response.status_code == 429 or response.status_code >= 500:
                    self.total_retries += 1
                    wait_time = min(backoff * (2 ** attempt), self.MAX_BACKOFF)
                    print(f"[RETRY {attempt + 1}/{self.MAX_RETRIES}] "
                          f"Status {response.status_code}. Esperando {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                # Error del cliente (4xx) - no reintentar excepto 401
                if response.status_code == 401:
                    # Token expirado, forzar renovacion
                    print("[AUTH] Token expirado, renovando...")
                    self.auth.access_token = None
                    continue

                # Otros errores del cliente
                error_msg = f"Error de API: {response.status_code} - {response.text}"
                print(f"[ERROR] {error_msg}")
                raise Exception(error_msg)

            except requests.exceptions.Timeout:
                self.total_retries += 1
                wait_time = min(backoff * (2 ** attempt), self.MAX_BACKOFF)
                print(f"[TIMEOUT] Reintentando en {wait_time}s...")
                time.sleep(wait_time)
                continue

            except requests.exceptions.RequestException as e:
                self.total_retries += 1
                wait_time = min(backoff * (2 ** attempt), self.MAX_BACKOFF)
                print(f"[REQUEST ERROR] {str(e)}. Reintentando en {wait_time}s...")
                time.sleep(wait_time)
                continue

        raise Exception(f"Se agotaron los reintentos ({self.MAX_RETRIES}) para {endpoint}")

    def query(self, query_string: str) -> Dict:
        """
        Ejecuta una consulta SQL-like en QBO

        Args:
            query_string: Consulta en formato QBO Query Language

        Returns:
            dict: Respuesta de la API
        """
        return self._make_request('/query', params={'query': query_string})

    def fetch_entity_paginated(
        self,
        entity: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        date_field: str = 'MetaData.LastUpdatedTime'
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Extrae todos los registros de una entidad con paginacion

        Args:
            entity: Nombre de la entidad (Invoice, Customer, Item)
            start_date: Fecha inicio ISO format (UTC)
            end_date: Fecha fin ISO format (UTC)
            date_field: Campo de fecha para filtrar

        Yields:
            dict: Registro individual con metadatos de pagina
        """
        start_position = 1
        page_number = 1
        total_fetched = 0

        print(f"\n[EXTRACT] Iniciando extraccion de {entity}")
        print(f"  Ventana: {start_date} -> {end_date}")
        print(f"  Tamano de pagina: {self.PAGE_SIZE}")

        while True:
            # Construir query con filtros y paginacion
            query = f"SELECT * FROM {entity}"

            # Agregar filtros de fecha si se proporcionan
            conditions = []
            if start_date:
                conditions.append(f"{date_field} >= '{start_date}'")
            if end_date:
                conditions.append(f"{date_field} <= '{end_date}'")

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += f" STARTPOSITION {start_position} MAXRESULTS {self.PAGE_SIZE}"

            print(f"\n[PAGE {page_number}] Ejecutando: {query[:100]}...")

            response = self.query(query)

            # Obtener los registros de la respuesta
            query_response = response.get('QueryResponse', {})
            records = query_response.get(entity, [])

            if not records:
                print(f"[PAGE {page_number}] No hay mas registros.")
                break

            # Yield cada registro con metadatos de pagina
            for record in records:
                yield {
                    'record': record,
                    'page_number': page_number,
                    'page_size': self.PAGE_SIZE,
                    'position_in_page': records.index(record) + 1
                }
                total_fetched += 1

            print(f"[PAGE {page_number}] Obtenidos: {len(records)} registros. "
                  f"Total acumulado: {total_fetched}")

            # Verificar si hay mas paginas
            if len(records) < self.PAGE_SIZE:
                print(f"[COMPLETE] Ultima pagina alcanzada.")
                break

            # Avanzar a siguiente pagina
            start_position += self.PAGE_SIZE
            page_number += 1

        print(f"\n[SUMMARY] Extraccion completada:")
        print(f"  Total registros: {total_fetched}")
        print(f"  Total paginas: {page_number}")
        print(f"  Total requests: {self.total_requests}")
        print(f"  Total reintentos: {self.total_retries}")


def get_qbo_client():
    """
    Factory function para obtener una instancia del cliente

    Returns:
        QBOClient: Instancia configurada del cliente
    """
    return QBOClient()
