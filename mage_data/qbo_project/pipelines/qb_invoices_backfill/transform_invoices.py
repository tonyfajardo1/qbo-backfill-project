"""
Transformer: Valida y prepara Invoices para carga
Pipeline: qb_invoices_backfill
"""
from typing import List, Dict, Any
from datetime import datetime, timezone

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_invoices(data: List[Dict[str, Any]], *args, **kwargs) -> List[Dict[str, Any]]:
    """
    Valida y transforma los registros de Invoices.

    Validaciones:
    - Verificar que cada registro tenga ID
    - Detectar y reportar registros invalidos
    - Agregar metricas de calidad

    Args:
        data: Lista de registros extraidos

    Returns:
        List[Dict]: Registros validados listos para carga
    """
    print("=" * 60)
    print("TRANSFORMACION Y VALIDACION DE INVOICES")
    print("=" * 60)

    if not data:
        print("[WARN] No hay datos para transformar")
        return []

    valid_records = []
    invalid_records = []
    duplicate_ids = set()
    seen_ids = set()

    for item in data:
        record = item.get('record', {})
        record_id = record.get('Id')

        # Validacion 1: ID presente y no nulo
        if not record_id:
            invalid_records.append({
                'reason': 'ID nulo o faltante',
                'record': record
            })
            continue

        # Validacion 2: ID no duplicado
        if record_id in seen_ids:
            duplicate_ids.add(record_id)
            continue
        seen_ids.add(record_id)

        # Validacion 3: Payload no vacio
        if not record:
            invalid_records.append({
                'reason': 'Payload vacio',
                'id': record_id
            })
            continue

        # Registro valido
        valid_records.append(item)

    # Reportar metricas de calidad
    print(f"\nMetricas de calidad:")
    print(f"  Total recibidos:    {len(data)}")
    print(f"  Registros validos:  {len(valid_records)}")
    print(f"  Registros invalidos:{len(invalid_records)}")
    print(f"  IDs duplicados:     {len(duplicate_ids)}")

    if invalid_records:
        print(f"\n[WARN] Registros invalidos detectados:")
        for inv in invalid_records[:5]:  # Mostrar solo primeros 5
            print(f"  - {inv.get('reason')}: {inv.get('id', 'N/A')}")
        if len(invalid_records) > 5:
            print(f"  ... y {len(invalid_records) - 5} mas")

    if duplicate_ids:
        print(f"\n[WARN] IDs duplicados: {list(duplicate_ids)[:10]}")

    # Agregar timestamp de transformacion
    transform_time = datetime.now(timezone.utc).isoformat()
    for item in valid_records:
        item['transformed_at_utc'] = transform_time

    print("=" * 60)
    print(f"Registros listos para carga: {len(valid_records)}")
    print("=" * 60)

    return valid_records


@test
def test_output(output, *args) -> None:
    """
    Valida que los registros transformados sean correctos
    """
    assert output is not None, 'La salida es None'
    assert isinstance(output, list), 'La salida debe ser una lista'

    # Verificar que no hay IDs duplicados
    ids = [item['record']['Id'] for item in output if 'record' in item]
    assert len(ids) == len(set(ids)), 'Se encontraron IDs duplicados'

    print(f"[TEST OK] Transformacion valida con {len(output)} registros")
