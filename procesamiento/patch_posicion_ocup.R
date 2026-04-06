# =============================================================================
# PARCHE: Corrección swap códigos 7↔8 en posicion_ocup
# Proyecto: DAT4CCIÓN 2026 — EQUILAB
# Fecha: 2026-04-06
# =============================================================================
#
# PROBLEMA: En 02_armonizar.R, posicion_ocup se copia de P6430 sin armonizar.
# Los códigos 7 y 8 tienen significados opuestos entre marcos:
#
#   Marco 2005 (2019-2020): 7 = Sin remun. otras emp, 8 = Jornalero
#   Marco 2018 (2021-2025): 7 = Jornalero, 8 = Sin remun. otras emp
#
# SOLUCIÓN: Agregar este bloque en 02_armonizar.R JUSTO DESPUÉS de la
# asignación de posicion_ocup (buscar: "POSICIÓN OCUPACIONAL"):
#
# =============================================================================
# CÓDIGO A INSERTAR EN 02_armonizar.R (después de la línea ~201):

#   # CORRECCIÓN: swap códigos 7↔8 en Marco 2005 para armonizar al esquema Marco 2018
#   # Marco 2005: 7 = Sin remuneración otras emp, 8 = Jornalero
#   # Marco 2018: 7 = Jornalero, 8 = Sin remuneración otras emp
#   if (marco == "2005") {
#     dt[posicion_ocup == 7L, posicion_ocup := 80L]   # temporal
#     dt[posicion_ocup == 8L, posicion_ocup := 7L]
#     dt[posicion_ocup == 80L, posicion_ocup := 8L]
#     cat("  [FIX] Swap posicion_ocup 7↔8 aplicado\n")
#   }

# =============================================================================
# ALTERNATIVA: Si no quieres editar 02_armonizar.R, puedes correr este script
# directamente sobre el parquet existente para regenerarlo con la corrección.
# =============================================================================

library(data.table)
library(arrow)

cat("=== Aplicando corrección swap posicion_ocup 7↔8 ===\n\n")

# Ruta al parquet (ajustar si es necesario)
ruta_parquet <- "datos/geih_ocupados_2019_2025.parquet"

cat("Leyendo parquet...\n")
dt <- as.data.table(read_parquet(ruta_parquet))
cat(sprintf("  Filas totales: %s\n", format(nrow(dt), big.mark = ",")))

# Verificar estado ANTES
cat("\nDistribución posicion_ocup 7 y 8 ANTES de corrección:\n")
print(dt[posicion_ocup %in% c(7L, 8L), .N, keyby = .(marco, posicion_ocup)])

# Aplicar swap solo en Marco 2005 (años 2019-2020)
n_swap <- dt[marco == "2005" & posicion_ocup %in% c(7L, 8L), .N]
cat(sprintf("\nRegistros a corregir (Marco 2005, códigos 7 u 8): %s\n", format(n_swap, big.mark = ",")))

dt[marco == "2005" & posicion_ocup == 7L, posicion_ocup := 80L]
dt[marco == "2005" & posicion_ocup == 8L, posicion_ocup := 7L]
dt[marco == "2005" & posicion_ocup == 80L, posicion_ocup := 8L]

# Verificar estado DESPUÉS
cat("\nDistribución posicion_ocup 7 y 8 DESPUÉS de corrección:\n")
print(dt[posicion_ocup %in% c(7L, 8L), .N, keyby = .(marco, posicion_ocup)])

# Guardar parquet corregido
cat("\nGuardando parquet corregido...\n")
write_parquet(dt, ruta_parquet)
cat(sprintf("  Guardado: %s\n", ruta_parquet))

cat("\n=== Corrección completada ===\n")
cat("NOTA: Después de correr este script, haz git pull en tu local\n")
cat("      y re-renderiza las páginas del dashboard.\n")
