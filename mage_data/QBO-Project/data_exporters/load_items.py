from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values, Json

from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_data(data, *args, **kwargs):
    print("=" * 50)
    print("CARGA DE ITEMS A POSTGRESQL")
    print("=" * 50)

    if not data:
        print("[WARN] No hay datos para cargar")
        return {'status': 'completed', 'inserted': 0, 'updated': 0}

    conn = psycopg2.connect(
        host=get_secret_value('PG_HOST') or 'postgres',
        port=get_secret_value('PG_PORT') or '5432',
        database=get_secret_value('PG_DATABASE') or 'qbo_database',
        user=get_secret_value('PG_USER') or 'qbo_user',
        password=get_secret_value('PG_PASSWORD') or 'qbo_password_change_me',
    )

    cursor = conn.cursor()
    ingested_at = datetime.now(timezone.utc)

    values = []
    for item in data:
        record = item['record']
        values.append(
            (
                str(record['Id']),
                Json(record),
                ingested_at,
                item.get('extract_window_start'),
                item.get('extract_window_end'),
                item.get('page_number'),
                item.get('page_size'),
                Json({'entity': 'Item'}),
            )
        )

    upsert_query = """
        INSERT INTO raw.qb_items (
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

    result = execute_values(
        cursor,
        upsert_query,
        values,
        template="(%s, %s, %s, %s, %s, %s, %s, %s)",
        fetch=True,
    )

    inserted = sum(1 for r in result if r[0])
    updated = len(result) - inserted

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM raw.qb_items")
    total = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    print(f"Insertados: {inserted}")
    print(f"Actualizados: {updated}")
    print(f"Total en tabla: {total}")
    print("=" * 50)

    return {'inserted': inserted, 'updated': updated, 'total': total}


@test
def test_output(output, *args):
    assert output is not None
