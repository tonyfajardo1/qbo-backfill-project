-- ============================================
-- REPORTE DE VOLUMETRIA - QBO BACKFILL
-- Ejecutar en PgAdmin para generar evidencia
-- ============================================

-- 1. CONTEO TOTAL POR ENTIDAD
SELECT '=== CONTEO TOTAL POR ENTIDAD ===' as reporte;

SELECT
    'qb_invoices' as entidad,
    COUNT(*) as total_registros
FROM raw.qb_invoices
UNION ALL
SELECT
    'qb_customers' as entidad,
    COUNT(*) as total_registros
FROM raw.qb_customers
UNION ALL
SELECT
    'qb_items' as entidad,
    COUNT(*) as total_registros
FROM raw.qb_items;

-- 2. VERIFICACION DE DUPLICADOS (debe retornar 0 filas)
SELECT '=== VERIFICACION DE DUPLICADOS ===' as reporte;

SELECT 'qb_invoices' as tabla, id, COUNT(*) as duplicados
FROM raw.qb_invoices
GROUP BY id
HAVING COUNT(*) > 1
UNION ALL
SELECT 'qb_customers' as tabla, id, COUNT(*) as duplicados
FROM raw.qb_customers
GROUP BY id
HAVING COUNT(*) > 1
UNION ALL
SELECT 'qb_items' as tabla, id, COUNT(*) as duplicados
FROM raw.qb_items
GROUP BY id
HAVING COUNT(*) > 1;

-- 3. RESUMEN DE INGESTA POR VENTANA DE EXTRACCION
SELECT '=== RESUMEN POR VENTANA DE EXTRACCION ===' as reporte;

SELECT
    'qb_invoices' as entidad,
    extract_window_start_utc,
    extract_window_end_utc,
    COUNT(*) as registros,
    MIN(ingested_at_utc) as primera_ingesta,
    MAX(ingested_at_utc) as ultima_ingesta
FROM raw.qb_invoices
GROUP BY extract_window_start_utc, extract_window_end_utc
UNION ALL
SELECT
    'qb_customers' as entidad,
    extract_window_start_utc,
    extract_window_end_utc,
    COUNT(*) as registros,
    MIN(ingested_at_utc) as primera_ingesta,
    MAX(ingested_at_utc) as ultima_ingesta
FROM raw.qb_customers
GROUP BY extract_window_start_utc, extract_window_end_utc
UNION ALL
SELECT
    'qb_items' as entidad,
    extract_window_start_utc,
    extract_window_end_utc,
    COUNT(*) as registros,
    MIN(ingested_at_utc) as primera_ingesta,
    MAX(ingested_at_utc) as ultima_ingesta
FROM raw.qb_items
GROUP BY extract_window_start_utc, extract_window_end_utc;

-- 4. ESTADISTICAS POR PAGINA
SELECT '=== ESTADISTICAS POR PAGINA ===' as reporte;

SELECT
    'qb_invoices' as entidad,
    page_number,
    COUNT(*) as registros_en_pagina
FROM raw.qb_invoices
GROUP BY page_number
ORDER BY page_number;

SELECT
    'qb_customers' as entidad,
    page_number,
    COUNT(*) as registros_en_pagina
FROM raw.qb_customers
GROUP BY page_number
ORDER BY page_number;

SELECT
    'qb_items' as entidad,
    page_number,
    COUNT(*) as registros_en_pagina
FROM raw.qb_items
GROUP BY page_number
ORDER BY page_number;

-- 5. MUESTRA DE DATOS (primeros 3 registros por entidad)
SELECT '=== MUESTRA DE DATOS ===' as reporte;

SELECT 'qb_invoices' as entidad, id, ingested_at_utc, page_number
FROM raw.qb_invoices LIMIT 3;

SELECT 'qb_customers' as entidad, id, ingested_at_utc, page_number
FROM raw.qb_customers LIMIT 3;

SELECT 'qb_items' as entidad, id, ingested_at_utc, page_number
FROM raw.qb_items LIMIT 3;
