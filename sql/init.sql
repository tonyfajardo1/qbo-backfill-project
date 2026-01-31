-- ============================================
-- SCRIPT DE INICIALIZACION - QBO BACKFILL
-- Crea el esquema RAW y las tablas por entidad
-- ============================================

-- Crear esquema raw
CREATE SCHEMA IF NOT EXISTS raw;

-- ============================================
-- TABLA: raw.qb_invoices
-- Almacena facturas de QuickBooks Online
-- ============================================
CREATE TABLE IF NOT EXISTS raw.qb_invoices (
    id VARCHAR(50) PRIMARY KEY,                          -- ID de la factura en QBO
    payload JSONB NOT NULL,                              -- Payload completo de la API
    ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,   -- Timestamp de carga
    extract_window_start_utc TIMESTAMP WITH TIME ZONE,   -- Inicio de ventana de extraccion
    extract_window_end_utc TIMESTAMP WITH TIME ZONE,     -- Fin de ventana de extraccion
    page_number INTEGER,                                 -- Numero de pagina
    page_size INTEGER,                                   -- Tamano de pagina
    request_payload JSONB                                -- Payload de la solicitud
);

-- Indice para busquedas por fecha de ingesta
CREATE INDEX IF NOT EXISTS idx_invoices_ingested_at
ON raw.qb_invoices(ingested_at_utc);

-- ============================================
-- TABLA: raw.qb_customers
-- Almacena clientes de QuickBooks Online
-- ============================================
CREATE TABLE IF NOT EXISTS raw.qb_customers (
    id VARCHAR(50) PRIMARY KEY,                          -- ID del cliente en QBO
    payload JSONB NOT NULL,                              -- Payload completo de la API
    ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,   -- Timestamp de carga
    extract_window_start_utc TIMESTAMP WITH TIME ZONE,   -- Inicio de ventana de extraccion
    extract_window_end_utc TIMESTAMP WITH TIME ZONE,     -- Fin de ventana de extraccion
    page_number INTEGER,                                 -- Numero de pagina
    page_size INTEGER,                                   -- Tamano de pagina
    request_payload JSONB                                -- Payload de la solicitud
);

-- Indice para busquedas por fecha de ingesta
CREATE INDEX IF NOT EXISTS idx_customers_ingested_at
ON raw.qb_customers(ingested_at_utc);

-- ============================================
-- TABLA: raw.qb_items
-- Almacena items/productos de QuickBooks Online
-- ============================================
CREATE TABLE IF NOT EXISTS raw.qb_items (
    id VARCHAR(50) PRIMARY KEY,                          -- ID del item en QBO
    payload JSONB NOT NULL,                              -- Payload completo de la API
    ingested_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,   -- Timestamp de carga
    extract_window_start_utc TIMESTAMP WITH TIME ZONE,   -- Inicio de ventana de extraccion
    extract_window_end_utc TIMESTAMP WITH TIME ZONE,     -- Fin de ventana de extraccion
    page_number INTEGER,                                 -- Numero de pagina
    page_size INTEGER,                                   -- Tamano de pagina
    request_payload JSONB                                -- Payload de la solicitud
);

-- Indice para busquedas por fecha de ingesta
CREATE INDEX IF NOT EXISTS idx_items_ingested_at
ON raw.qb_items(ingested_at_utc);

-- ============================================
-- TABLA: raw.backfill_log
-- Registro de ejecuciones del backfill
-- ============================================
CREATE TABLE IF NOT EXISTS raw.backfill_log (
    id SERIAL PRIMARY KEY,
    entity_name VARCHAR(50) NOT NULL,                    -- invoices, customers, items
    window_start_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    records_read INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    pages_processed INTEGER DEFAULT 0,
    duration_seconds NUMERIC(10,2),
    status VARCHAR(20) NOT NULL,                         -- running, completed, failed
    error_message TEXT,
    started_at_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at_utc TIMESTAMP WITH TIME ZONE,
    created_at_utc TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Comentarios de documentacion
COMMENT ON SCHEMA raw IS 'Esquema RAW para datos crudos de QuickBooks Online';
COMMENT ON TABLE raw.qb_invoices IS 'Facturas extraidas de QBO con payload completo';
COMMENT ON TABLE raw.qb_customers IS 'Clientes extraidos de QBO con payload completo';
COMMENT ON TABLE raw.qb_items IS 'Items/productos extraidos de QBO con payload completo';
COMMENT ON TABLE raw.backfill_log IS 'Registro de ejecuciones del pipeline de backfill';
