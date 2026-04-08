#!/usr/bin/env Rscript
# ══════════════════════════════════════════════════════════════
# 07b_precalcular_p3.R
# Pre-calcula datos para Página 3 — Brechas Salariales (descriptivos)
# Fuente: datos/geih_ocupados_2019_2025.parquet
#
# Genera:
#   datos/p3_boxplot_stats.parquet      → cuartiles ponderados por año y sexo
#   datos/p3_brecha_educacion.parquet   → brecha salarial por educación (serie)
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
cat("══════════════════════════════════════════════════════════\n\n")

# ════════════════════════════════════════════════════════════════════════════
# 1. CARGAR Y FILTRAR DATOS
# ════════════════════════════════════════════════════════════════════════════

cat("[1/4] Cargando parquet de ocupados...\n")
df_raw <- read_parquet("datos/geih_ocupados_2019_2025.parquet")
cat("      ", format(nrow(df_raw), big.mark = "."), "filas cargadas\n")

cat("[2/4] Filtrando y preparando variables...\n")
df <- df_raw |>
  filter(
    condicion_activ == 1,
    edad >= 15, edad <= 65,
    sexo %in% c(1, 2),
    !is.na(ing_mes), ing_mes > 0,
    !is.na(horas_sem), horas_sem > 0,
    !is.na(fex)
  ) |>
  mutate(
    sexo_label = if_else(sexo == 1, "Hombres", "Mujeres"),
    sal_hora   = ing_mes / (horas_sem * 4.33),
    niv_edu_label = case_when(
      niv_edu_armon %in% c(1, 2) ~ "Sin educación / Preescolar",
      niv_edu_armon == 3         ~ "Primaria",
      niv_edu_armon == 4         ~ "Secundaria",
      niv_edu_armon == 5         ~ "Media",
      niv_edu_armon == 6         ~ "Técnico / Tecnológico",
      niv_edu_armon == 7         ~ "Universitario",
      niv_edu_armon == 8         ~ "Posgrado",
      TRUE                       ~ NA_character_
    )
  ) |>
  filter(!is.na(niv_edu_label), sal_hora > 0)

cat("      ", format(nrow(df), big.mark = "."), "filas tras filtro\n")

# ════════════════════════════════════════════════════════════════════════════
# 2. BOXPLOT STATS — cuartiles ponderados por año × sexo
# ════════════════════════════════════════════════════════════════════════════

cat("[3/4] Calculando estadísticos de boxplot...\n")

# Trimamos al P1–P99 del salario/hora por año para evitar outliers extremos
boxplot_stats <- df |>
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

cat("      ", nrow(boxplot_stats), "filas (", length(unique(boxplot_stats$anio)), "años × 2 sexos)\n")

# ════════════════════════════════════════════════════════════════════════════
# 3. BRECHA POR EDUCACIÓN — serie temporal
# ════════════════════════════════════════════════════════════════════════════

cat("[4/4] Calculando brecha salarial por nivel educativo...\n")

# Nota: las medianas generales colapsan al salario mínimo en 2022-2025.
# Dentro de cada nivel educativo la dispersión es mayor y las medianas
# difieren más entre sexos, especialmente en educación superior.

mediana_edu <- df |>
  filter(!niv_edu_label %in% c("Sin educación / Preescolar")) |>
  group_by(anio, niv_edu_label, sexo_label) |>
  summarise(
    mediana_sal_hora = as.numeric(wtd.quantile(sal_hora, weights = fex, probs = 0.5, na.rm = TRUE)),
    n_obs = n(),
    .groups = "drop"
  )

brecha_edu <- mediana_edu |>
  pivot_wider(
    names_from  = sexo_label,
    values_from = c(mediana_sal_hora, n_obs)
  ) |>
  filter(
    !is.na(mediana_sal_hora_Hombres),
    !is.na(mediana_sal_hora_Mujeres),
    n_obs_Hombres >= 50,
    n_obs_Mujeres >= 50
  ) |>
  mutate(
    brecha_pct = round((mediana_sal_hora_Hombres - mediana_sal_hora_Mujeres) /
                         mediana_sal_hora_Hombres * 100, 2),
    mediana_h  = round(mediana_sal_hora_Hombres),
    mediana_m  = round(mediana_sal_hora_Mujeres)
  ) |>
  select(anio, niv_edu_label, mediana_h, mediana_m, brecha_pct,
         n_hombres = n_obs_Hombres, n_mujeres = n_obs_Mujeres)

# Advertencia sobre medianas colapsadas
n_iguales <- sum(brecha_edu$brecha_pct == 0, na.rm = TRUE)
if (n_iguales > 0) {
  cat("      ⚠ ", n_iguales, "combinaciones con brecha = 0% (medianas iguales)\n")
}

cat("      ", nrow(brecha_edu), "filas\n")

# ════════════════════════════════════════════════════════════════════════════
# 4. GUARDAR
# ════════════════════════════════════════════════════════════════════════════

write_parquet(boxplot_stats, "datos/p3_boxplot_stats.parquet")
write_parquet(brecha_edu,    "datos/p3_brecha_educacion.parquet")

cat("\n✓ Archivos generados:\n")
cat("  datos/p3_boxplot_stats.parquet    (", nrow(boxplot_stats), "filas)\n")
cat("  datos/p3_brecha_educacion.parquet (", nrow(brecha_edu), "filas)\n")
cat("\n══════════════════════════════════════════════════════════\n")
cat("  Pre-cálculo P3 completado\n")
cat("══════════════════════════════════════════════════════════\n")
