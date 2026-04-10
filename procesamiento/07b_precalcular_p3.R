#!/usr/bin/env Rscript
# ══════════════════════════════════════════════════════════════
# 07b_precalcular_p3.R
# Pre-calcula datos para Página 3 — Brechas Salariales (descriptivos)
# Fuente: datos/geih_ocupados_2019_2025.parquet
#
# CORRECCIÓN 2026-04-09:
#   - Se cambia ing_mes (cobertura ~57%) por ing_total (cobertura ~98%)
#   - Se genera boxplot en ingreso MENSUAL (no salario/hora)
#   - Se mantiene sal_hora como archivo adicional para referencia
#   - Se excluyen códigos 98/99 de ing_total (no sabe/no responde)
#
# Genera:
#   datos/p3_boxplot_stats.parquet      → cuartiles ponderados por año y sexo (mensual)
#   datos/p3_brecha_educacion.parquet   → brecha salarial por educación (serie)
#   datos/p3_boxplot_hora.parquet       → cuartiles sal/hora (referencia adicional)
#
# Ejecutar desde la raíz del proyecto:
#   Rscript procesamiento/07b_precalcular_p3.R
#   O en RStudio: source("procesamiento/07b_precalcular_p3.R")
# ══════════════════════════════════════════════════════════════

library(arrow)
library(dplyr)
library(tidyr)
library(Hmisc)

cat("\n══════════════════════════════════════════════════════════\n")
cat("  EQUILAB — Pre-cálculo Página 3 (Descriptivos salariales)\n")
cat("  VERSIÓN CORREGIDA: usa ing_total (INGLABO)\n")
cat("══════════════════════════════════════════════════════════\n\n")

# ════════════════════════════════════════════════════════════════════════════
# 1. CARGAR Y FILTRAR DATOS
# ════════════════════════════════════════════════════════════════════════════

cat("[1/5] Cargando parquet de ocupados...\n")
df_raw <- read_parquet("datos/geih_ocupados_2019_2025.parquet")
cat("      ", format(nrow(df_raw), big.mark = "."), "filas cargadas\n")

cat("[2/5] Filtrando y preparando variables...\n")
df <- df_raw |>
  filter(
    condicion_activ == 1,
    edad >= 15, edad <= 65,
    sexo %in% c(1, 2),
    !is.na(ing_total),
    ing_total > 0,
    !ing_total %in% c(98, 99),
    !is.na(fex)
  ) |>
  mutate(
    sexo_label = if_else(sexo == 1, "Hombres", "Mujeres"),
    # Usamos niv_edu_orig en vez de niv_edu_armon porque la armonización
    # colapsa todos los niveles superiores (6-11) en un solo código 6.
    # Marco 2005: P6210 → 6 = Superior (sin desagregar)
    # Marco 2018: P3042 → 6=Técnica, 7=Tecnológica, 8=Universitaria,
    #                      9=Especialización, 10=Maestría, 11=Doctorado
    niv_edu_label = case_when(
      niv_edu_orig %in% c(1, 2)    ~ "Sin educación / Preescolar",
      niv_edu_orig == 3             ~ "Primaria",
      niv_edu_orig == 4             ~ "Secundaria",
      niv_edu_orig == 5             ~ "Media",
      niv_edu_orig %in% c(6, 7)    ~ "Técnico / Tecnológico",
      niv_edu_orig == 8             ~ "Universitario",
      niv_edu_orig %in% c(9,10,11) ~ "Posgrado",
      # Marco 2005: nivel 6 = "Superior" genérico
      # Se agrupa con Universitario como mejor aproximación
      # (en Marco 2005, niv_edu_orig ya es 6 para todo superior)
      TRUE                          ~ NA_character_
    )
  ) |>
  filter(!is.na(niv_edu_label))

cat("      ", format(nrow(df), big.mark = "."), "filas tras filtro\n")
cat("      (usando ing_total — INGLABO — cobertura ~98%)\n")

# ════════════════════════════════════════════════════════════════════════════
# 2. BOXPLOT STATS — cuartiles ponderados MENSUALES por año × sexo
# ════════════════════════════════════════════════════════════════════════════

cat("[3/5] Calculando estadísticos de boxplot (ingreso mensual)...\n")

boxplot_stats <- df |>
  group_by(anio, sexo_label) |>
  summarise(
    n_obs  = n(),
    q1     = as.numeric(wtd.quantile(ing_total, weights = fex, probs = 0.25, na.rm = TRUE)),
    median = as.numeric(wtd.quantile(ing_total, weights = fex, probs = 0.50, na.rm = TRUE)),
    q3     = as.numeric(wtd.quantile(ing_total, weights = fex, probs = 0.75, na.rm = TRUE)),
    lower  = as.numeric(wtd.quantile(ing_total, weights = fex, probs = 0.05, na.rm = TRUE)),
    upper  = as.numeric(wtd.quantile(ing_total, weights = fex, probs = 0.95, na.rm = TRUE)),
    mean   = as.numeric(weighted.mean(ing_total, w = fex, na.rm = TRUE)),
    .groups = "drop"
  )

cat("      ", nrow(boxplot_stats), "filas (", length(unique(boxplot_stats$anio)), "años × 2 sexos)\n")

# Verificación de cordura
cat("\n      Verificación rápida (medianas 2025):\n")
for (sx in c("Hombres", "Mujeres")) {
  med <- boxplot_stats |> filter(anio == 2025, sexo_label == sx) |> pull(median)
  n   <- boxplot_stats |> filter(anio == 2025, sexo_label == sx) |> pull(n_obs)
  cat("        ", sx, ": Mediana = $", format(round(med), big.mark = "."),
      " | N = ", format(n, big.mark = "."), "\n")
}

# ════════════════════════════════════════════════════════════════════════════
# 3. BRECHA POR EDUCACIÓN — serie temporal (ingreso mensual)
# ════════════════════════════════════════════════════════════════════════════

cat("[4/5] Calculando brecha salarial por nivel educativo...\n")

mediana_edu <- df |>
  filter(!niv_edu_label %in% c("Sin educación / Preescolar")) |>
  group_by(anio, niv_edu_label, sexo_label) |>
  summarise(
    mediana_ing = as.numeric(wtd.quantile(ing_total, weights = fex, probs = 0.5, na.rm = TRUE)),
    n_obs = n(),
    .groups = "drop"
  )

brecha_edu <- mediana_edu |>
  pivot_wider(
    names_from  = sexo_label,
    values_from = c(mediana_ing, n_obs)
  ) |>
  filter(
    !is.na(mediana_ing_Hombres),
    !is.na(mediana_ing_Mujeres),
    n_obs_Hombres >= 50,
    n_obs_Mujeres >= 50
  ) |>
  mutate(
    brecha_pct = round((mediana_ing_Hombres - mediana_ing_Mujeres) /
                         mediana_ing_Hombres * 100, 2),
    mediana_h  = round(mediana_ing_Hombres),
    mediana_m  = round(mediana_ing_Mujeres)
  ) |>
  select(anio, niv_edu_label, mediana_h, mediana_m, brecha_pct,
         n_hombres = n_obs_Hombres, n_mujeres = n_obs_Mujeres)

n_iguales <- sum(brecha_edu$brecha_pct == 0, na.rm = TRUE)
if (n_iguales > 0) {
  cat("      ⚠ ", n_iguales, "combinaciones con brecha = 0% (medianas iguales)\n")
}
cat("      ", nrow(brecha_edu), "filas\n")

# ════════════════════════════════════════════════════════════════════════════
# 4. BOXPLOT SAL/HORA (archivo adicional de referencia)
# ════════════════════════════════════════════════════════════════════════════

cat("[5/5] Calculando estadísticos de salario/hora (referencia)...\n")

df_hora <- df |>
  filter(!is.na(horas_sem), horas_sem > 0) |>
  mutate(sal_hora = ing_total / (horas_sem * 4.33))

boxplot_hora <- df_hora |>
  group_by(anio, sexo_label) |>
  summarise(
    n_obs  = n(),
    q1     = as.numeric(wtd.quantile(sal_hora, weights = fex, probs = 0.25, na.rm = TRUE)),
    median = as.numeric(wtd.quantile(sal_hora, weights = fex, probs = 0.50, na.rm = TRUE)),
    q3     = as.numeric(wtd.quantile(sal_hora, weights = fex, probs = 0.75, na.rm = TRUE)),
    lower  = as.numeric(wtd.quantile(sal_hora, weights = fex, probs = 0.05, na.rm = TRUE)),
    upper  = as.numeric(wtd.quantile(sal_hora, weights = fex, probs = 0.95, na.rm = TRUE)),
    .groups = "drop"
  )

# ════════════════════════════════════════════════════════════════════════════
# 5. GUARDAR
# ════════════════════════════════════════════════════════════════════════════

write_parquet(boxplot_stats, "datos/p3_boxplot_stats.parquet")
write_parquet(brecha_edu,    "datos/p3_brecha_educacion.parquet")
write_parquet(boxplot_hora,  "datos/p3_boxplot_hora.parquet")

cat("\n✓ Archivos generados:\n")
cat("  datos/p3_boxplot_stats.parquet      (", nrow(boxplot_stats), "filas) ← INGRESO MENSUAL\n")
cat("  datos/p3_brecha_educacion.parquet   (", nrow(brecha_edu), "filas) ← INGRESO MENSUAL\n")
cat("  datos/p3_boxplot_hora.parquet       (", nrow(boxplot_hora), "filas) ← SAL/HORA referencia\n")
cat("\n══════════════════════════════════════════════════════════\n")
cat("  Pre-cálculo P3 completado (CORREGIDO con ing_total)\n")
cat("══════════════════════════════════════════════════════════\n")
