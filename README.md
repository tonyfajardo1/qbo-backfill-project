# QBO Backfill Pipeline

Pipeline de backfill historico para extraer datos de QuickBooks Online (QBO) y cargarlos en PostgreSQL.

## Diagrama de Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ARQUITECTURA DEL SISTEMA                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐                                                        │
│  │  QuickBooks     │                                                        │
│  │  Online API     │                                                        │
│  │  (Sandbox)      │                                                        │
│  │  ─────────────  │                                                        │
│  │  • Invoices     │                                                        │
│  │  • Customers    │◄──── OAuth 2.0 (Refresh Token)                        │
│  │  • Items        │                                                        │
│  └────────┬────────┘                                                        │
│           │                                                                 │
│           │ REST API + Paginacion                                           │
│           │ Rate Limiting + Backoff                                         │
│           ▼                                                                 │
│  ┌─────────────────────────────────────────────────────┐                   │
│  │                    DOCKER NETWORK                    │                   │
│  │                    (qbo_network)                     │                   │
│  │  ┌─────────────────────────────────────────────┐    │                   │
│  │  │              MAGE AI (puerto 6789)          │    │                   │
│  │  │  ┌─────────────────────────────────────┐    │    │                   │
│  │  │  │         MAGE SECRETS                │    │    │                   │
│  │  │  │  • QBO_CLIENT_ID                    │    │    │                   │
│  │  │  │  • QBO_CLIENT_SECRET                │    │    │                   │
│  │  │  │  • QBO_REALM_ID                     │    │    │                   │
│  │  │  │  • QBO_REFRESH_TOKEN                │    │    │                   │
│  │  │  │  • PG_HOST, PG_USER, PG_PASSWORD    │    │    │                   │
│  │  │  └─────────────────────────────────────┘    │    │                   │
│  │  │                                             │    │                   │
│  │  │  Pipelines:                                 │    │                   │
│  │  │  ├── qb_invoices_backfill                  │    │                   │
│  │  │  ├── qb_customers_backfill                 │    │                   │
│  │  │  └── qb_items_backfill                     │    │                   │
│  │  │                                             │    │                   │
│  │  │  Cada pipeline:                             │    │                   │
│  │  │  [Extract] → [Transform] → [Load]          │    │                   │
│  │  └─────────────────────────────────────────────┘    │                   │
│  │                        │                            │                   │
│  │                        │ SQL (Upsert)               │                   │
│  │                        ▼                            │                   │
│  │  ┌─────────────────────────────────────────────┐    │                   │
│  │  │          POSTGRESQL (puerto 5432)           │    │                   │
│  │  │                                             │    │                   │
│  │  │  Esquema: raw                               │    │                   │
│  │  │  ├── qb_invoices    (payload JSONB)        │    │                   │
│  │  │  ├── qb_customers   (payload JSONB)        │    │                   │
│  │  │  ├── qb_items       (payload JSONB)        │    │                   │
│  │  │  └── backfill_log   (metricas)             │    │                   │
│  │  └─────────────────────────────────────────────┘    │                   │
│  │                        │                            │                   │
│  │                        ▼                            │                   │
│  │  ┌─────────────────────────────────────────────┐    │                   │
│  │  │           PGADMIN (puerto 5050)             │    │                   │
│  │  │         Interfaz web de administracion      │    │                   │
│  │  └─────────────────────────────────────────────┘    │                   │
│  └─────────────────────────────────────────────────────┘                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Requisitos Previos

- Docker y Docker Compose instalados
- App de QuickBooks Online (sandbox) con:
  - Client ID
  - Client Secret
  - Realm ID (Company ID)
  - Refresh Token
- Conocimientos basicos de APIs REST y OAuth 2.0

---

## Estructura del Proyecto

```
qbo-backfill-project/
├── docker-compose.yml          # Definicion de servicios
├── README.md                   # Este archivo
├── sql/
│   └── init.sql               # Script de inicializacion de BD
├── mage_data/
│   └── qbo_project/
│       ├── metadata.yaml      # Config del proyecto Mage
│       ├── io_config.yaml     # Config de conexiones
│       ├── utils/             # Modulos compartidos
│       │   ├── qbo_auth.py    # Autenticacion OAuth 2.0
│       │   ├── qbo_client.py  # Cliente API con paginacion
│       │   └── db_utils.py    # Utilidades PostgreSQL
│       └── pipelines/
│           ├── qb_invoices_backfill/
│           ├── qb_customers_backfill/
│           └── qb_items_backfill/
├── postgres_data/              # Volumen de datos PostgreSQL
└── evidencias/                 # Capturas y reportes
```

---

## Pasos para Levantar el Proyecto

### 1. Clonar/Descargar el proyecto

```bash
cd qbo-backfill-project
```

### 2. Iniciar los contenedores

```bash
docker-compose up -d
```

### 3. Verificar que los servicios esten corriendo

```bash
docker-compose ps
```

Deberias ver:
- `mage_qbo` - Running (puerto 6789)
- `postgres_qbo` - Running (puerto 5432)
- `pgadmin_qbo` - Running (puerto 5050)

### 4. Acceder a las interfaces

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| Mage AI | http://localhost:6789 | Sin autenticacion |
| PgAdmin | http://localhost:5050 | admin@admin.com / admin |

### 5. Configurar Mage Secrets (OBLIGATORIO)

Ver seccion "Gestion de Secretos" abajo.

### 6. Ejecutar los pipelines

Ver seccion "Ejecucion de Pipelines" abajo.

### 7. Detener los contenedores

```bash
docker-compose down
```

Para eliminar los volumenes (datos):
```bash
docker-compose down -v
```

---

## Gestion de Secretos

### Secretos Requeridos

Todos los secretos deben configurarse en **Mage Secrets** (Settings → Secrets en la UI de Mage).

| Nombre | Proposito | Ejemplo |
|--------|-----------|---------|
| `QBO_CLIENT_ID` | ID de la aplicacion QBO | `ABc123...` |
| `QBO_CLIENT_SECRET` | Secreto de la aplicacion QBO | `xyz789...` |
| `QBO_REALM_ID` | ID de la compania en QBO | `123456789` |
| `QBO_REFRESH_TOKEN` | Token de actualizacion OAuth | `AB11...` |
| `QBO_ENVIRONMENT` | Entorno (sandbox/production) | `sandbox` |
| `PG_HOST` | Host de PostgreSQL | `postgres` |
| `PG_PORT` | Puerto de PostgreSQL | `5432` |
| `PG_DATABASE` | Nombre de la base de datos | `qbo_database` |
| `PG_USER` | Usuario de PostgreSQL | `qbo_user` |
| `PG_PASSWORD` | Contrasena de PostgreSQL | `tu_password` |

### Proceso de Configuracion

1. Acceder a Mage UI: http://localhost:6789
2. Ir a **Settings** (icono de engranaje)
3. Seleccionar **Secrets**
4. Agregar cada secreto con su nombre y valor

### Rotacion de Secretos

| Secreto | Frecuencia de Rotacion | Responsable |
|---------|------------------------|-------------|
| `QBO_REFRESH_TOKEN` | Cada 100 dias (o cuando QBO lo renueve) | Administrador |
| `QBO_CLIENT_SECRET` | Anual o si se compromete | Administrador |
| `PG_PASSWORD` | Cada 90 dias | DBA |

**IMPORTANTE**:
- QuickBooks rota automaticamente el Refresh Token. Cuando el pipeline reciba un nuevo token, aparecera un WARNING en los logs indicando que debe actualizarse en Mage Secrets.
- Nunca exponer secretos en el repositorio, variables de entorno del docker-compose, ni en capturas de pantalla.

---

## Pipelines de Backfill

### Descripcion de Pipelines

| Pipeline | Entidad | Tabla Destino |
|----------|---------|---------------|
| `qb_invoices_backfill` | Invoice (Facturas) | `raw.qb_invoices` |
| `qb_customers_backfill` | Customer (Clientes) | `raw.qb_customers` |
| `qb_items_backfill` | Item (Productos) | `raw.qb_items` |

### Parametros de Ejecucion

Cada pipeline acepta los siguientes parametros (variables):

| Parametro | Formato | Descripcion |
|-----------|---------|-------------|
| `fecha_inicio` | ISO 8601 UTC | Inicio de la ventana de extraccion |
| `fecha_fin` | ISO 8601 UTC | Fin de la ventana de extraccion |

**Ejemplo:**
```
fecha_inicio: 2024-01-01T00:00:00Z
fecha_fin: 2024-12-31T23:59:59Z
```

### Segmentacion (Chunking)

Para volumenes grandes, se recomienda dividir el rango en periodos menores para:
- **Controlar el volumen** de datos por ejecucion
- **Facilitar reintentos** en caso de fallo (solo se reprocesa el tramo fallido)
- **Mejorar observabilidad** con metricas por tramo
- **Evitar timeouts** en extracciones muy largas

#### Estrategia de Segmentacion Recomendada

| Volumen Estimado | Segmentacion Sugerida |
|------------------|----------------------|
| < 1,000 registros | Sin segmentar (rango completo) |
| 1,000 - 10,000 registros | Por trimestre |
| 10,000 - 50,000 registros | Por mes |
| > 50,000 registros | Por semana o dia |

#### Ejemplo: Segmentacion Mensual para 2024

| Tramo | fecha_inicio | fecha_fin | Trigger |
|-------|--------------|-----------|---------|
| Enero 2024 | 2024-01-01T00:00:00Z | 2024-01-31T23:59:59Z | trigger_enero |
| Febrero 2024 | 2024-02-01T00:00:00Z | 2024-02-29T23:59:59Z | trigger_febrero |
| Marzo 2024 | 2024-03-01T00:00:00Z | 2024-03-31T23:59:59Z | trigger_marzo |
| ... | ... | ... | ... |
| Diciembre 2024 | 2024-12-01T00:00:00Z | 2024-12-31T23:59:59Z | trigger_diciembre |

#### Proceso de Ejecucion Segmentada

1. **Crear un trigger one-time por cada tramo** en Mage UI
2. **Ejecutar secuencialmente** o en paralelo (si los recursos lo permiten)
3. **Verificar cada tramo** en `raw.backfill_log` antes de continuar
4. **Deshabilitar triggers** completados para evitar re-ejecuciones

#### Verificacion de Tramos Ejecutados

```sql
-- Ver estado de todos los tramos ejecutados
SELECT
    entity_name,
    window_start_utc,
    window_end_utc,
    records_inserted,
    records_updated,
    status,
    duration_seconds
FROM raw.backfill_log
ORDER BY window_start_utc;

-- Identificar tramos faltantes o fallidos
SELECT * FROM raw.backfill_log
WHERE status = 'failed'
ORDER BY window_start_utc;
```

### Estructura de cada Pipeline

```
[Extract] ──► [Transform] ──► [Load]
    │              │              │
    ▼              ▼              ▼
 OAuth 2.0    Validacion     Upsert
 Paginacion   Deduplicacion  Idempotencia
 Rate Limit   Metricas       Logging
```

### Limites y Reintentos

| Configuracion | Valor | Descripcion |
|---------------|-------|-------------|
| Tamano de pagina | 100 | Maximo permitido por QBO |
| Rate limit | 400 req/min | Conservador (QBO permite ~500) |
| Max reintentos | 5 | Por request |
| Backoff inicial | 1 segundo | Se duplica en cada reintento |
| Backoff maximo | 60 segundos | Tope de espera |

---

## Trigger One-Time

### Configuracion del Trigger

1. En Mage UI, ir al pipeline deseado
2. Click en **Triggers** → **+ New trigger**
3. Configurar:
   - **Trigger type**: `Schedule`
   - **Frequency**: `once`
   - **Start date/time**: Fecha y hora deseada (en UTC)
   - **Variables**:
     ```yaml
     fecha_inicio: '2024-01-01T00:00:00Z'
     fecha_fin: '2024-12-31T23:59:59Z'
     ```

### Conversion de Zonas Horarias

| UTC | America/Guayaquil (ECT) |
|-----|-------------------------|
| 00:00 | 19:00 (dia anterior) |
| 05:00 | 00:00 |
| 12:00 | 07:00 |
| 18:00 | 13:00 |

**Ejemplo**: Si quieres ejecutar a las 08:00 de Guayaquil, programa el trigger para las 13:00 UTC.

### Post-Ejecucion

Despues de una ejecucion exitosa:
1. Verificar logs y metricas en Mage UI
2. **Deshabilitar** el trigger o marcarlo como completado
3. Documentar la ejecucion en el registro de operaciones

---

## Esquema RAW

### Tablas por Entidad

Cada tabla tiene la siguiente estructura:

```sql
CREATE TABLE raw.qb_<entidad> (
    id VARCHAR(50) PRIMARY KEY,           -- ID de QBO
    payload JSONB NOT NULL,               -- Payload completo
    ingested_at_utc TIMESTAMP WITH TIME ZONE,
    extract_window_start_utc TIMESTAMP WITH TIME ZONE,
    extract_window_end_utc TIMESTAMP WITH TIME ZONE,
    page_number INTEGER,
    page_size INTEGER,
    request_payload JSONB
);
```

### Metadatos Obligatorios

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| `id` | VARCHAR(50) | Clave primaria (ID de QBO) |
| `payload` | JSONB | Registro completo de la API |
| `ingested_at_utc` | TIMESTAMP TZ | Momento de carga |
| `extract_window_start_utc` | TIMESTAMP TZ | Inicio de ventana |
| `extract_window_end_utc` | TIMESTAMP TZ | Fin de ventana |
| `page_number` | INTEGER | Numero de pagina |
| `page_size` | INTEGER | Tamano de pagina |
| `request_payload` | JSONB | Parametros de la solicitud |

### Idempotencia

La carga utiliza **UPSERT** (INSERT ... ON CONFLICT DO UPDATE):
- Si el registro no existe → INSERT
- Si el registro existe → UPDATE

**Verificacion**: Reejecutar el mismo tramo produce el mismo resultado sin duplicados.

---

## Validaciones y Volumetria

### Validaciones Automaticas

1. **Integridad**: Claves primarias no nulas y no duplicadas
2. **Payload**: Verificar que no este vacio
3. **Timestamps**: Coherencia con zona horaria UTC

### Consultas de Verificacion

```sql
-- Conteo por entidad
SELECT 'invoices' as entity, COUNT(*) FROM raw.qb_invoices
UNION ALL
SELECT 'customers', COUNT(*) FROM raw.qb_customers
UNION ALL
SELECT 'items', COUNT(*) FROM raw.qb_items;

-- Verificar duplicados (debe retornar 0)
SELECT id, COUNT(*)
FROM raw.qb_invoices
GROUP BY id
HAVING COUNT(*) > 1;

-- Resumen de backfill por entidad
SELECT
    entity_name,
    SUM(records_read) as total_read,
    SUM(records_inserted) as total_inserted,
    SUM(records_updated) as total_updated,
    SUM(duration_seconds) as total_duration
FROM raw.backfill_log
WHERE status = 'completed'
GROUP BY entity_name;
```

---

## Runbook de Operaciones

### Reanudar desde el Ultimo Tramo Exitoso

1. Consultar el log de backfill:
```sql
SELECT * FROM raw.backfill_log
WHERE entity_name = 'invoices'
ORDER BY started_at_utc DESC
LIMIT 5;
```

2. Identificar el ultimo tramo exitoso (`status = 'completed'`)
3. Configurar nuevo trigger con `fecha_inicio` = `window_end_utc` del ultimo exitoso

### Reintentar un Tramo Fallido

1. Identificar el tramo fallido en `raw.backfill_log`
2. Revisar `error_message` para diagnostico
3. Corregir el problema (auth, red, etc.)
4. Crear nuevo trigger con las mismas fechas

### Verificar Resultados

1. Comparar conteos entre QBO y PostgreSQL
2. Ejecutar queries de validacion (ver seccion anterior)
3. Revisar logs en Mage UI para warnings/errores

---

## Troubleshooting

### Problemas de Autenticacion

| Error | Causa | Solucion |
|-------|-------|----------|
| `401 Unauthorized` | Token expirado | El sistema renueva automaticamente; si persiste, actualizar `QBO_REFRESH_TOKEN` |
| `Invalid refresh token` | Token revocado o expirado (>100 dias) | Generar nuevo Refresh Token en QBO Developer Portal |
| `Invalid client credentials` | Client ID/Secret incorrectos | Verificar en Mage Secrets |

### Problemas de Paginacion

| Error | Causa | Solucion |
|-------|-------|----------|
| Registros faltantes | Paginacion incorrecta | Verificar `startPosition` y `maxResults` en logs |
| Registros duplicados | Concurrencia en API | La idempotencia del UPSERT lo maneja |

### Problemas de Rate Limiting

| Error | Causa | Solucion |
|-------|-------|----------|
| `429 Too Many Requests` | Limite excedido | El sistema espera automaticamente con backoff |
| Timeouts frecuentes | Red lenta o API sobrecargada | Aumentar timeout, reducir PAGE_SIZE |

### Problemas de Timezone

| Error | Causa | Solucion |
|-------|-------|----------|
| Datos faltantes | Filtros en zona horaria incorrecta | Asegurar que `fecha_inicio/fin` esten en UTC |
| Duplicados por fecha | Ventanas solapadas | Verificar que las ventanas sean contiguas |

### Problemas de Almacenamiento

| Error | Causa | Solucion |
|-------|-------|----------|
| `disk full` | Volumen de Postgres lleno | Limpiar datos antiguos o expandir volumen |
| `connection refused` | PostgreSQL no levantado | `docker-compose up -d postgres` |

### Problemas de Permisos

| Error | Causa | Solucion |
|-------|-------|----------|
| `permission denied` en volumen | Usuario incorrecto | Verificar ownership de carpetas `mage_data` y `postgres_data` |
| No puede crear esquema/tablas | Usuario sin permisos | El script `init.sql` debe ejecutarse como superuser |

---

## Checklist de Aceptacion

- [x] Mage y Postgres se comunican por nombre de servicio
- [x] Todos los secretos (QBO y Postgres) estan en Mage Secrets; no hay secretos en el repo/entorno expuesto
- [x] Pipelines `qb_<entidad>_backfill` acepta `fecha_inicio` y `fecha_fin` (UTC) y segmenta el rango
- [x] Trigger one-time configurado, ejecutado y luego deshabilitado/marcado como completado
- [x] Esquema raw con tablas por entidad, payload completo y metadatos obligatorios
- [x] Idempotencia verificada: reejecucion de un tramo no genera duplicados
- [x] Paginacion y rate limits manejados y documentados
- [x] Volumetria y validaciones minimas registradas y archivadas como evidencia
- [x] Runbook de reanudacion y reintentos disponible y seguido

---

## Contacto

**Proyecto**: Data Mining - Proyecto 01
**Universidad**: San Francisco de Quito
