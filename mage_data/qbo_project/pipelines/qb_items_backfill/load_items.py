"""
Data Exporter: Carga Items a PostgreSQL
Pipeline: qb_items_backfill
"""
import sys
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def load_items(data: List[Dict[str, Any]], *args, **kwargs) -> Dict[str, Any]:
    """Carga los Items validados a PostgreSQL con UPSERT."""
    from utils.db_utils import get_postgres_client

    print("=" * 60)
    print("CARGA DE ITEMS A POSTGRESQL")
    print("=" * 60)

    if not data:
        return {'status': 'completed', 'records_loaded': 0, 'inserted': 0, 'updated': 0}

    fecha_inicio = kwargs.get('fecha_inicio', data[0].get('extract_window_start'))
    fecha_fin = kwargs.get('fecha_fin', data[0].get('extract_window_end'))

    db = get_postgres_client()
    start_time = datetime.now(timezone.utc)

    log_id = db.log_backfill_start('items', fecha_inicio, fecha_fin)

    try:
        request_payload = {
            'entity': 'Item',
            'window_start': fecha_inicio,
            'window_end': fecha_fin
        }

        result = db.upsert_records(
            table_name='raw.qb_items',
            records=data,
            window_start=fecha_inicio,
            window_end=fecha_fin,
            request_payload=request_payload
        )

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        db.log_backfill_complete(
            log_id=log_id,
            records_read=len(data),
            records_inserted=result['inserted'],
            records_updated=result['updated'],
            pages_processed=len(set(item.get('page_number', 1) for item in data)),
            duration_seconds=duration,
            status='completed'
        )

        final_count = db.get_record_count('raw.qb_items')

        print(f"Insertados: {result['inserted']}, Actualizados: {result['updated']}")
        print(f"Total en tabla: {final_count}")
        print("=" * 60)

        return {
            'status': 'completed',
            'records_loaded': len(data),
            'inserted': result['inserted'],
            'updated': result['updated'],
            'total_in_table': final_count
        }

    except Exception as e:
        db.log_backfill_complete(log_id, len(data), 0, 0, 0, 0, 'failed', str(e))
        raise
    finally:
        db.close()


@test
def test_output(output, *args) -> None:
    assert output is not None and output.get('status') == 'completed'
    print(f"[TEST OK] Carga completada")
