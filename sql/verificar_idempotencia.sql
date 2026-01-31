-- ============================================
-- VERIFICACION DE IDEMPOTENCIA
-- Ejecutar ANTES y DESPUES de re-ejecutar un pipeline
-- para demostrar que no se crean duplicados
-- ============================================

-- PASO 1: Ejecutar ANTES de re-ejecutar el pipeline
-- Guardar estos resultados como "ANTES"
SELECT '=== CONTEO ANTES DE RE-EJECUCION ===' as paso;

SELECT
    'qb_customers' as entidad,
    COUNT(*) as total_registros,
    MAX(ingested_at_utc) as ultima_ingesta
FROM raw.qb_customers;

-- PASO 2: Re-ejecutar el pipeline qb_customers_backfill en Mage

-- PASO 3: Ejecutar DESPUES de re-ejecutar el pipeline
-- Comparar con los resultados de "ANTES"
SELECT '=== CONTEO DESPUES DE RE-EJECUCION ===' as paso;

SELECT
    'qb_customers' as entidad,
    COUNT(*) as total_registros,
    MAX(ingested_at_utc) as ultima_ingesta
FROM raw.qb_customers;

-- PASO 4: Verificar que NO hay duplicados
SELECT '=== VERIFICACION DE DUPLICADOS ===' as paso;

SELECT id, COUNT(*) as veces
FROM raw.qb_customers
GROUP BY id
HAVING COUNT(*) > 1;

-- Si retorna 0 filas = IDEMPOTENCIA VERIFICADA
-- El conteo total debe ser IGUAL antes y despues
-- Solo cambia ingested_at_utc (se actualiza, no se duplica)

-- ============================================
-- RESULTADO ESPERADO:
-- - total_registros ANTES = total_registros DESPUES
-- - 0 duplicados encontrados
-- - ingested_at_utc actualizado (mas reciente)
-- ============================================
