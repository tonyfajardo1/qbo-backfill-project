"""
Modulo de autenticacion OAuth 2.0 para QuickBooks Online
Maneja la obtencion y renovacion de Access Tokens
"""
import os
import requests
import base64
import time
from datetime import datetime, timezone

# Importar funcion de Mage Secrets
try:
    from mage_ai.data_preparation.shared.secrets import get_secret_value
except ImportError:
    # Fallback para desarrollo local fuera de Mage
    def get_secret_value(key, **kwargs):
        """Fallback: obtiene secretos desde variables de entorno"""
        return os.environ.get(key)


class QBOAuthenticator:
    """
    Clase para manejar autenticacion OAuth 2.0 con QuickBooks Online
    """

    # URLs de la API de Intuit
    TOKEN_URL_SANDBOX = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    TOKEN_URL_PROD = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    API_BASE_SANDBOX = "https://sandbox-quickbooks.api.intuit.com"
    API_BASE_PROD = "https://quickbooks.api.intuit.com"

    def __init__(self):
        """Inicializa el autenticador cargando secretos de Mage Secrets"""
        self.client_id = get_secret_value('QBO_CLIENT_ID')
        self.client_secret = get_secret_value('QBO_CLIENT_SECRET')
        self.realm_id = get_secret_value('QBO_REALM_ID')
        self.refresh_token = get_secret_value('QBO_REFRESH_TOKEN')
        self.environment = get_secret_value('QBO_ENVIRONMENT') or 'sandbox'

        self.access_token = None
        self.token_expiry = None

    @property
    def api_base_url(self):
        """Retorna la URL base segun el entorno"""
        if self.environment == 'production':
            return self.API_BASE_PROD
        return self.API_BASE_SANDBOX

    @property
    def token_url(self):
        """Retorna la URL de tokens segun el entorno"""
        if self.environment == 'production':
            return self.TOKEN_URL_PROD
        return self.TOKEN_URL_SANDBOX

    def _get_auth_header(self):
        """Genera el header de autorizacion Basic para obtener tokens"""
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def get_access_token(self):
        """
        Obtiene un nuevo Access Token usando el Refresh Token

        Returns:
            str: Access Token valido

        Raises:
            Exception: Si falla la autenticacion
        """
        # Si el token actual es valido, reutilizarlo
        if self.access_token and self.token_expiry:
            if datetime.now(timezone.utc) < self.token_expiry:
                return self.access_token

        headers = {
            'Authorization': self._get_auth_header(),
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }

        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token
        }

        print(f"[{datetime.now(timezone.utc).isoformat()}] Obteniendo nuevo Access Token...")

        response = requests.post(
            self.token_url,
            headers=headers,
            data=data,
            timeout=30
        )

        if response.status_code != 200:
            error_msg = f"Error de autenticacion: {response.status_code} - {response.text}"
            print(f"[ERROR] {error_msg}")
            raise Exception(error_msg)

        token_data = response.json()
        self.access_token = token_data['access_token']

        # El token expira en 'expires_in' segundos (tipicamente 3600 = 1 hora)
        expires_in = token_data.get('expires_in', 3600)
        self.token_expiry = datetime.now(timezone.utc).replace(
            microsecond=0
        )
        # Restar 5 minutos como margen de seguridad
        from datetime import timedelta
        self.token_expiry = self.token_expiry + timedelta(seconds=expires_in - 300)

        # IMPORTANTE: Si se recibe un nuevo refresh_token, deberia actualizarse
        # en Mage Secrets manualmente (QBO rota refresh tokens periodicamente)
        new_refresh = token_data.get('refresh_token')
        if new_refresh and new_refresh != self.refresh_token:
            print(f"[ADVERTENCIA] Se recibio un nuevo Refresh Token. "
                  f"Actualiza QBO_REFRESH_TOKEN en Mage Secrets.")

        print(f"[{datetime.now(timezone.utc).isoformat()}] Access Token obtenido exitosamente. "
              f"Expira en {expires_in} segundos.")

        return self.access_token

    def get_headers(self):
        """
        Retorna los headers necesarios para llamadas a la API de QBO

        Returns:
            dict: Headers con Authorization y Accept
        """
        token = self.get_access_token()
        return {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }


def get_qbo_authenticator():
    """
    Factory function para obtener una instancia del autenticador

    Returns:
        QBOAuthenticator: Instancia configurada del autenticador
    """
    return QBOAuthenticator()
