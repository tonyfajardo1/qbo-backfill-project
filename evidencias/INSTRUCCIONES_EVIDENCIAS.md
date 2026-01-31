# Instrucciones para Generar Evidencias

## 1. Evidencia de Volumetria

### Pasos:
1. Abrir PgAdmin: http://localhost:5050
2. Conectarse a la base de datos `qbo_database`
3. Abrir Query Tool (click derecho en la BD > Query Tool)
4. Copiar y ejecutar el contenido de `sql/reporte_volumetria.sql`
5. Tomar captura de pantalla mostrando:
   - Conteo total por entidad
   - Verificacion de duplicados (0 filas)
   - Resumen por ventana de extraccion

### Nombre del archivo:
`evidencias/reporte_volumetria.png`

---

## 2. Evidencia de Idempotencia

### Pasos:
1. Abrir PgAdmin y ejecutar el PASO 1 de `sql/verificar_idempotencia.sql`
2. Tomar captura mostrando el conteo ANTES
3. Ir a Mage UI y re-ejecutar el pipeline `qb_customers_backfill`
4. Esperar a que termine
5. Ejecutar el PASO 3 y PASO 4 del script SQL
6. Tomar captura mostrando:
   - Conteo DESPUES (debe ser IGUAL al de ANTES)
   - Verificacion de duplicados (0 filas)

### Nombre del archivo:
`evidencias/idempotencia_verificada.png`

### Resultado esperado:
```
ANTES:  total_registros = 29
DESPUES: total_registros = 29  (IGUAL)
Duplicados: 0 filas

CONCLUSION: Idempotencia verificada - no se crean duplicados al re-ejecutar
```

---

## 3. Evidencias ya existentes

- `mage_secrets.png` - Configuracion de Mage Secrets
- `triggers completados.png` - Triggers one-time ejecutados
- `tabla_customers.png` - Datos en tabla qb_customers
- `tabla_invoices.png` - Datos en tabla qb_invoices
- `tabla_items.png` - Datos en tabla qb_items

---

## Checklist de Evidencias

- [x] Configuracion de Mage Secrets (nombres visibles, valores ocultos)
- [x] Triggers one-time configurado y ejecucion finalizada
- [x] Tablas raw con registros y metadatos
- [ ] Reporte de volumetria por tramo
- [ ] Evidencia de idempotencia (re-ejecucion sin duplicados)
