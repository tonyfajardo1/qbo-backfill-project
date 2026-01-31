import requests
import base64
from datetime import datetime, timezone

from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


class QBOClient:
    TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    API_BASE = "https://sandbox-quickbooks.api.intuit.com"
    PAGE_SIZE = 100

    def __init__(self):
        self.client_id = get_secret_value('QBO_CLIENT_ID')
        self.client_secret = get_secret_value('QBO_CLIENT_SECRET')
        self.realm_id = get_secret_value('QBO_REALM_ID')
        self.refresh_token = get_secret_value('QBO_REFRESH_TOKEN')
        self.access_token = None

    def get_access_token(self):
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()

        response = requests.post(
            self.TOKEN_URL,
            headers={
                'Authorization': f'Basic {encoded}',
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            data={
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token,
            },
        )

        if response.status_code != 200:
            raise Exception(f"Auth error: {response.status_code} - {response.text}")

        self.access_token = response.json()['access_token']
        print("[AUTH] Token obtenido exitosamente")
        return self.access_token

    def query(self, query_string):
        if not self.access_token:
            self.get_access_token()

        url = f"{self.API_BASE}/v3/company/{self.realm_id}/query"
        response = requests.get(
            url,
            headers={
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json',
            },
            params={'query': query_string},
        )

        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code} - {response.text}")

        return response.json()

    def fetch_all_invoices(self):
        records = []
        start_position = 1
        page = 1

        while True:
            query = (
                f"SELECT * FROM Invoice "
                f"STARTPOSITION {start_position} "
                f"MAXRESULTS {self.PAGE_SIZE}"
            )

            print(f"[PAGE {page}] Ejecutando query...")
            response = self.query(query)

            invoices = response.get('QueryResponse', {}).get('Invoice', [])
            if not invoices:
                break

            for invoice in invoices:
                records.append(
                    {
                        'record': invoice,
                        'page_number': page,
                        'page_size': self.PAGE_SIZE,
                    }
                )

            print(f"[PAGE {page}] Obtenidos: {len(invoices)} registros")

            if len(invoices) < self.PAGE_SIZE:
                break

            start_position += self.PAGE_SIZE
            page += 1

        return records


@data_loader
def load_data(*args, **kwargs):
    fecha_inicio = kwargs.get('fecha_inicio', '2024-01-01T00:00:00Z')
    fecha_fin = kwargs.get('fecha_fin', '2025-12-31T23:59:59Z')

    print("=" * 50)
    print("EXTRACCION DE INVOICES - QBO")
    print("=" * 50)
    print(f"Fecha inicio: {fecha_inicio}")
    print(f"Fecha fin: {fecha_fin}")

    client = QBOClient()
    records = client.fetch_all_invoices()

    for r in records:
        r['extract_window_start'] = fecha_inicio
        r['extract_window_end'] = fecha_fin

    print(f"\nTotal extraidos: {len(records)}")
    print("=" * 50)

    return records


@test
def test_output(output, *args):
    assert output is not None, 'Sin datos'
