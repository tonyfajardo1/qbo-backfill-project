from datetime import datetime, timezone

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("=" * 50)
    print("VALIDACION DE INVOICES")
    print("=" * 50)

    if not data:
        print("[WARN] No hay datos")
        return []

    valid_records = []
    seen_ids = set()

    for item in data:
        record = item.get('record', {})
        record_id = record.get('Id')

        if not record_id or record_id in seen_ids:
            continue

        seen_ids.add(record_id)
        item['transformed_at'] = datetime.now(timezone.utc).isoformat()
        valid_records.append(item)

    print(f"Recibidos: {len(data)}")
    print(f"Validos: {len(valid_records)}")
    print("=" * 50)

    return valid_records


@test
def test_output(output, *args):
    assert output is not None
