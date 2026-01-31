"""
Data Loader: Extrae Invoices de QuickBooks Online
Pipeline: qb_invoices_backfill
"""
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

# Agregar path de utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def extract_invoices(*args, **kwargs) -> List[Dict[str, Any]]:
    """
    Extrae todas las facturas de QBO dentro de la ventana de fechas especificada.
    Implementa paginacion, rate limiting y reintentos.

    Variables del pipeline:
        fecha_inicio: Fecha inicio en formato ISO (UTC)
        fecha_fin: Fecha fin en formato ISO (UTC)

    Returns:
        List[Dict]: Lista de registros con payload y metadatos
    """
    from utils.qbo_client import get_qbo_client

    # Obtener parametros del pipeline
    fecha_inicio = kwargs.get('fecha_inicio', '2024-01-01T00:00:00Z')
    fecha_fin = kwargs.get('fecha_fin', '2024-12-31T23:59:59Z')

    print("=" * 60)
    print("EXTRACCION DE INVOICES - QBO BACKFILL")
    print("=" * 60)
    print(f"Fecha inicio (UTC): {fecha_inicio}")
    print(f"Fecha fin (UTC):    {fecha_fin}")
    print("=" * 60)

    # Iniciar cliente de QBO
    client = get_qbo_client()

    # Extraer con paginacion
    records = []
    start_time = datetime.now(timezone.utc)

    try:
        for item in client.fetch_entity_paginated(
            entity='Invoice',
            start_date=fecha_inicio,
            end_date=fecha_fin,
            date_field='MetaData.LastUpdatedTime'
        ):
            # Agregar metadatos de extraccion
            item['extract_window_start'] = fecha_inicio
            item['extract_window_end'] = fecha_fin
            records.append(item)

    except Exception as e:
        print(f"[ERROR] Fallo en extraccion: {str(e)}")
        raise

    duration = (datetime.now(timezone.utc) - start_time).total_seconds()

    print("\n" + "=" * 60)
    print("RESUMEN DE EXTRACCION")
    print("=" * 60)
    print(f"Total registros extraidos: {len(records)}")
    print(f"Duracion: {duration:.2f} segundos")
    print(f"Requests totales: {client.total_requests}")
    print(f"Reintentos totales: {client.total_retries}")
    print("=" * 60)

    return records


@test
def test_output(output, *args) -> None:
    """
    Valida que la salida no este vacia y tenga la estructura correcta
    """
    assert output is not None, 'La salida es None'
    assert isinstance(output, list), 'La salida debe ser una lista'

    if len(output) > 0:
        sample = output[0]
        assert 'record' in sample, 'Falta campo record'
        assert 'page_number' in sample, 'Falta campo page_number'
        assert 'extract_window_start' in sample, 'Falta campo extract_window_start'

    print(f"[TEST OK] Extraccion valida con {len(output)} registros")
