"""
Data Loader: Extrae Customers de QuickBooks Online
Pipeline: qb_customers_backfill
"""
import sys
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def extract_customers(*args, **kwargs) -> List[Dict[str, Any]]:
    """
    Extrae todos los clientes de QBO dentro de la ventana de fechas.
    """
    from utils.qbo_client import get_qbo_client

    fecha_inicio = kwargs.get('fecha_inicio', '2024-01-01T00:00:00Z')
    fecha_fin = kwargs.get('fecha_fin', '2024-12-31T23:59:59Z')

    print("=" * 60)
    print("EXTRACCION DE CUSTOMERS - QBO BACKFILL")
    print("=" * 60)
    print(f"Fecha inicio (UTC): {fecha_inicio}")
    print(f"Fecha fin (UTC):    {fecha_fin}")
    print("=" * 60)

    client = get_qbo_client()
    records = []
    start_time = datetime.now(timezone.utc)

    try:
        for item in client.fetch_entity_paginated(
            entity='Customer',
            start_date=fecha_inicio,
            end_date=fecha_fin,
            date_field='MetaData.LastUpdatedTime'
        ):
            item['extract_window_start'] = fecha_inicio
            item['extract_window_end'] = fecha_fin
            records.append(item)

    except Exception as e:
        print(f"[ERROR] Fallo en extraccion: {str(e)}")
        raise

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    print("\n" + "=" * 60)
    print(f"Total registros extraidos: {len(records)}")
    print(f"Duracion: {duration:.2f} segundos")
    print("=" * 60)

    return records


@test
def test_output(output, *args) -> None:
    assert output is not None, 'La salida es None'
    assert isinstance(output, list), 'La salida debe ser una lista'
    print(f"[TEST OK] Extraccion valida con {len(output)} registros")
