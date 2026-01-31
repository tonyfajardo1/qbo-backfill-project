"""
Transformer: Valida y prepara Customers para carga
Pipeline: qb_customers_backfill
"""
from typing import List, Dict, Any
from datetime import datetime, timezone

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_customers(data: List[Dict[str, Any]], *args, **kwargs) -> List[Dict[str, Any]]:
    """Valida y transforma los registros de Customers."""
    print("=" * 60)
    print("TRANSFORMACION Y VALIDACION DE CUSTOMERS")
    print("=" * 60)

    if not data:
        print("[WARN] No hay datos para transformar")
        return []

    valid_records = []
    seen_ids = set()

    for item in data:
        record = item.get('record', {})
        record_id = record.get('Id')

        if not record_id or record_id in seen_ids:
            continue

        seen_ids.add(record_id)
        item['transformed_at_utc'] = datetime.now(timezone.utc).isoformat()
        valid_records.append(item)

    print(f"Total recibidos: {len(data)}, Validos: {len(valid_records)}")
    print("=" * 60)

    return valid_records


@test
def test_output(output, *args) -> None:
    assert output is not None, 'La salida es None'
    print(f"[TEST OK] Transformacion valida con {len(output)} registros")
