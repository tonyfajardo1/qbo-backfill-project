"""
Utilidades de base de datos para PostgreSQL
Maneja conexiones, upserts e idempotencia
"""
import os
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import execute_values, Json


def get_secret_value(key):
    """Obtiene secretos desde variables de entorno"""
    return os.environ.get(key)


class PostgresClient:
    """
    Cliente para interactuar con PostgreSQL
    Implementa upserts idempotentes y logging de ejecuciones
    """

    def __init__(self):
        """Inicializa el cliente cargando credenciales de Mage Secrets"""
        self.host = get_secret_value('PG_HOST') or 'postgres'
        self.port = int(get_secret_value('PG_PORT') or '5432')
        self.database = get_secret_value('PG_DATABASE') or 'qbo_database'
        self.user = get_secret_value('PG_USER') or 'qbo_user'
        self.password = get_secret_value('PG_PASSWORD')
        self.connection = None

    def connect(self):
        """Establece conexion con PostgreSQL"""
        if self.connection is None or self.connection.closed:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print(f"[DB] Conectado a PostgreSQL: {self.host}:{self.port}/{self.database}")
        return self.connection

    def close(self):
        """Cierra la conexion"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            print("[DB] Conexion cerrada")

    def upsert_records(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        window_start: str,
        window_end: str,
        request_payload: Optional[Dict] = None
    ) -> Dict[str, int]:
        """
        Inserta o actualiza registros de forma idempotente (UPSERT)

        Args:
            table_name: Nombre de la tabla (ej: raw.qb_invoices)
            records: Lista de registros con 'record' y metadatos de pagina
            window_start: Inicio de ventana de extraccion (ISO format)
            window_end: Fin de ventana de extraccion (ISO format)
            request_payload: Payload de la solicitud original

        Returns:
            dict: Contadores de registros insertados/actualizados
        """
        if not records:
            return {'inserted': 0, 'updated': 0}

        conn = self.connect()
        cursor = conn.cursor()

        inserted = 0
        updated = 0
        ingested_at = datetime.now(timezone.utc)

        # Preparar datos para upsert
        values = []
        for item in records:
            record = item['record']
            record_id = record.get('Id')

            if not record_id:
                print(f"[WARN] Registro sin ID, omitiendo: {record}")
                continue

            values.append((
                str(record_id),
                Json(record),
                ingested_at,
                window_start,
                window_end,
                item.get('page_number'),
                item.get('page_size'),
                Json(request_payload) if request_payload else None
            ))

        if not values:
            return {'inserted': 0, 'updated': 0}

        # Query de UPSERT (INSERT ... ON CONFLICT UPDATE)
        upsert_query = f"""
            INSERT INTO {table_name} (
                id,
                payload,
                ingested_at_utc,
                extract_window_start_utc,
                extract_window_end_utc,
                page_number,
                page_size,
                request_payload
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                payload = EXCLUDED.payload,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                extract_window_start_utc = EXCLUDED.extract_window_start_utc,
                extract_window_end_utc = EXCLUDED.extract_window_end_utc,
                page_number = EXCLUDED.page_number,
                page_size = EXCLUDED.page_size,
                request_payload = EXCLUDED.request_payload
            RETURNING (xmax = 0) AS inserted
        """

        try:
            # Ejecutar upsert en batch
            result = execute_values(
                cursor,
                upsert_query,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s)",
                fetch=True
            )

            # Contar inserciones vs actualizaciones
            for row in result:
                if row[0]:  # xmax = 0 significa INSERT
                    inserted += 1
                else:
                    updated += 1

            conn.commit()
            print(f"[DB] Upsert completado en {table_name}: "
                  f"{inserted} insertados, {updated} actualizados")

        except Exception as e:
            conn.rollback()
            print(f"[DB ERROR] Error en upsert: {str(e)}")
            raise

        finally:
            cursor.close()

        return {'inserted': inserted, 'updated': updated}

    def log_backfill_start(
        self,
        entity_name: str,
        window_start: str,
        window_end: str
    ) -> int:
        """
        Registra el inicio de una ejecucion de backfill

        Returns:
            int: ID del registro de log
        """
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            INSERT INTO raw.backfill_log (
                entity_name, window_start_utc, window_end_utc,
                status, started_at_utc
            )
            VALUES (%s, %s, %s, 'running', %s)
            RETURNING id
        """

        cursor.execute(query, (
            entity_name,
            window_start,
            window_end,
            datetime.now(timezone.utc)
        ))

        log_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()

        print(f"[LOG] Iniciado backfill log ID: {log_id}")
        return log_id

    def log_backfill_complete(
        self,
        log_id: int,
        records_read: int,
        records_inserted: int,
        records_updated: int,
        pages_processed: int,
        duration_seconds: float,
        status: str = 'completed',
        error_message: Optional[str] = None
    ):
        """
        Actualiza el registro de log con los resultados finales
        """
        conn = self.connect()
        cursor = conn.cursor()

        query = """
            UPDATE raw.backfill_log
            SET records_read = %s,
                records_inserted = %s,
                records_updated = %s,
                pages_processed = %s,
                duration_seconds = %s,
                status = %s,
                error_message = %s,
                completed_at_utc = %s
            WHERE id = %s
        """

        cursor.execute(query, (
            records_read,
            records_inserted,
            records_updated,
            pages_processed,
            duration_seconds,
            status,
            error_message,
            datetime.now(timezone.utc),
            log_id
        ))

        conn.commit()
        cursor.close()

        print(f"[LOG] Backfill log ID {log_id} actualizado: {status}")

    def get_record_count(self, table_name: str) -> int:
        """Obtiene el conteo de registros en una tabla"""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()

        return count


def get_postgres_client():
    """
    Factory function para obtener una instancia del cliente

    Returns:
        PostgresClient: Instancia configurada del cliente
    """
    return PostgresClient()
