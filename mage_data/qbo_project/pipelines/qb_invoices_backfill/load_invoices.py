"""
Data Exporter: Carga Invoices a PostgreSQL
Pipeline: qb_invoices_backfill
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
def load_invoices(data: List[Dict[str, Any]], *args, **kwargs) -> Dict[str, Any]:
    """
    Carga los Invoices validados a PostgreSQL con idempotencia (UPSERT).

    Args:
        data: Lista de registros transformados

    Returns:
        Dict: Resumen de la carga
    """
    from utils.db_utils import get_postgres_client

    print("=" * 60)
    print("CARGA DE INVOICES A POSTGRESQL")
    print("=" * 60)

    if not data:
        print("[WARN] No hay datos para cargar")
        return {
            'status': 'completed',
            'records_loaded': 0,
            'inserted': 0,
            'updated': 0
        }

    # Obtener parametros
    fecha_inicio = kwargs.get('fecha_inicio', data[0].get('extract_window_start'))
    fecha_fin = kwargs.get('fecha_fin', data[0].get('extract_window_end'))

    # Iniciar cliente de Postgres
    db = get_postgres_client()
    start_time = datetime.now(timezone.utc)

    # Registrar inicio de backfill
    log_id = db.log_backfill_start(
        entity_name='invoices',
        window_start=fecha_inicio,
        window_end=fecha_fin
    )

    total_inserted = 0
    total_updated = 0
    total_pages = set()

    try:
        # Obtener paginas unicas
        for item in data:
            total_pages.add(item.get('page_number', 1))

        # Preparar request payload para trazabilidad
        request_payload = {
            'entity': 'Invoice',
            'window_start': fecha_inicio,
            'window_end': fecha_fin,
            'extracted_at': datetime.now(timezone.utc).isoformat()
        }

        # Cargar en batch
        result = db.upsert_records(
            table_name='raw.qb_invoices',
            records=data,
            window_start=fecha_inicio,
            window_end=fecha_fin,
            request_payload=request_payload
        )

        total_inserted = result['inserted']
        total_updated = result['updated']

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Registrar finalizacion exitosa
        db.log_backfill_complete(
            log_id=log_id,
            records_read=len(data),
            records_inserted=total_inserted,
            records_updated=total_updated,
            pages_processed=len(total_pages),
            duration_seconds=duration,
            status='completed'
        )

        # Verificar conteo final
        final_count = db.get_record_count('raw.qb_invoices')

        print("\n" + "=" * 60)
        print("RESUMEN DE CARGA")
        print("=" * 60)
        print(f"Registros procesados: {len(data)}")
        print(f"Insertados:           {total_inserted}")
        print(f"Actualizados:         {total_updated}")
        print(f"Paginas procesadas:   {len(total_pages)}")
        print(f"Duracion:             {duration:.2f} segundos")
        print(f"Total en tabla:       {final_count}")
        print("=" * 60)

        return {
            'status': 'completed',
            'records_loaded': len(data),
            'inserted': total_inserted,
            'updated': total_updated,
            'pages': len(total_pages),
            'duration_seconds': duration,
            'total_in_table': final_count,
            'log_id': log_id
        }

    except Exception as e:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Registrar fallo
        db.log_backfill_complete(
            log_id=log_id,
            records_read=len(data),
            records_inserted=total_inserted,
            records_updated=total_updated,
            pages_processed=len(total_pages),
            duration_seconds=duration,
            status='failed',
            error_message=str(e)
        )

        print(f"[ERROR] Fallo en carga: {str(e)}")
        raise

    finally:
        db.close()


@test
def test_output(output, *args) -> None:
    """
    Valida que la carga fue exitosa
    """
    assert output is not None, 'La salida es None'
    assert output.get('status') == 'completed', f"Estado incorrecto: {output.get('status')}"
    assert output.get('records_loaded', 0) >= 0, 'Conteo de registros invalido'

    print(f"[TEST OK] Carga completada: {output.get('inserted')} insertados, "
          f"{output.get('updated')} actualizados")
