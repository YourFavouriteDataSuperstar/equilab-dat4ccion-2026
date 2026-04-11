#!/usr/bin/env Rscript
# ============================================================================
# PRE-CALCULO — Pagina 1: Descomposicion BOY de participacion laboral
#
# Genera parquets listos para el dashboard:
#   datos/p1_boy_resultados.parquet     -> BOY por anio/zona/etnia
#   datos/p1_boy_yun.parquet            -> BOY contribucion por variable (ultimo anio)
#
# Ejecutar desde la raiz del proyecto:
#   Rscript procesamiento/07_precalcular_p1_boy.R
# ============================================================================

library(arrow)
library(dplyr)
library(tidyr)

cat("\n══════════════════════════════════════════════════════════\n")
cat("  EQUILAB — Pre-cálculo Página 1: BOY (participación)\n")
cat("══════════════════════════════════════════════════════════\n\n")

# ════════════════════════════════════════════════════════════════════════════
# DATOS PET
# ════════════════════════════════════════════════════════════════════════════

cat("► Cargando PET...\n")

pet_raw <- read_parquet("datos/geih_pet_2019_2025.parquet")

pet <- pet_raw |>
  filter(
    !is.na(sexo), sexo %in% c(1, 2),
    !is.na(edad), edad >= 15, edad <= 65,
    !is.na(esc_anios),
    !is.na(condicion_activ),
    !is.na(fex)
  ) |>
  mutate(
    sexo_bin   = ifelse(sexo == 2, 1, 0),
    participa  = ifelse(condicion_activ %in% c(1, 2, 3), 1, 0),
    exp_pot    = pmax(edad - esc_anios - 6, 0),
    exp_pot2   = exp_pot^2,
    zona_label = ifelse(clase_zona == 1, "Urbano", "Rural"),
    etnia_label = case_when(
      etnia_bin == 0 ~ "No étnico",
      etnia_bin == 1 ~ "Étnico",
      TRUE ~ NA_character_
    ),
    edad_grupo = cut(edad, breaks = c(14, 25, 35, 45, 55, 65),
                     labels = c("15-25", "26-35", "36-45", "46-55", "56-65"))
  )

anio_reciente <- max(pet$anio)
cat("  N PET:", nrow(pet), "\n")

# ════════════════════════════════════════════════════════════════════════════
# MODELO BOY (participacion laboral)
# ════════════════════════════════════════════════════════════════════════════

cat("━━━ Descomposición BOY ━━━\n")

# Funcion BOY generica
run_boy <- function(df, label = "General", formula_boy = NULL) {
  if (is.null(formula_boy)) {
    formula_boy <- participa ~ esc_anios + exp_pot + exp_pot2 +
      factor(zona_label) + factor(edad_grupo)
  }
  n_h <- sum(df$sexo_bin == 0)
  n_m <- sum(df$sexo_bin == 1)
  if (n_h < 100 | n_m < 100) {
    return(tibble(grupo = label,
      componente = c("Brecha", "Dotaciones", "Coeficientes", "Residuo", "Tasa H", "Tasa M"),
      valor = rep(NA_real_, 6), n_hombres = n_h, n_mujeres = n_m))
  }
  if (nrow(df) > 40000) {
    set.seed(2026)
    df <- df |> slice_sample(n = 40000, weight_by = fex)
  }
  tasa_h <- mean(df$participa[df$sexo_bin == 0])
  tasa_m <- mean(df$participa[df$sexo_bin == 1])
  brecha <- tasa_h - tasa_m

  datos_h <- df |> filter(sexo_bin == 0)
  datos_m <- df |> filter(sexo_bin == 1)
  logit_h <- tryCatch(glm(formula_boy, data = datos_h, family = binomial("logit")), error = function(e) NULL)
  logit_m <- tryCatch(glm(formula_boy, data = datos_m, family = binomial("logit")), error = function(e) NULL)
  if (is.null(logit_h) || is.null(logit_m)) {
    return(tibble(grupo = label,
      componente = c("Brecha", "Dotaciones", "Coeficientes", "Residuo", "Tasa H", "Tasa M"),
      valor = rep(NA_real_, 6), n_hombres = n_h, n_mujeres = n_m))
  }

  coef_h <- coef(logit_h); coef_m <- coef(logit_m)
  X_m <- model.matrix(formula_boy, data = datos_m)
  X_h <- model.matrix(formula_boy, data = datos_h)
  cols_c  <- intersect(colnames(X_m), names(coef_h))
  cols_cm <- intersect(colnames(X_m), names(coef_m))

  tasa_pred_h <- mean(plogis(X_h[, cols_c, drop = FALSE] %*% coef_h[cols_c]))
  tasa_cf     <- mean(plogis(X_m[, cols_c, drop = FALSE] %*% coef_h[cols_c]))
  tasa_pred_m <- mean(plogis(X_m[, cols_cm, drop = FALSE] %*% coef_m[cols_cm]))

  comp_coef <- tasa_pred_h - tasa_cf
  comp_dot  <- tasa_cf - tasa_pred_m
  residuo   <- brecha - comp_coef - comp_dot

  tibble(grupo = label,
    componente = c("Brecha", "Dotaciones", "Coeficientes", "Residuo", "Tasa H", "Tasa M"),
    valor = c(brecha, comp_dot, comp_coef, residuo, tasa_h, tasa_m),
    n_hombres = n_h, n_mujeres = n_m)
}

f_boy_completa <- participa ~ esc_anios + exp_pot + exp_pot2 +
  factor(zona_label) + factor(edad_grupo)
f_boy_sin_zona <- participa ~ esc_anios + exp_pot + exp_pot2 + factor(edad_grupo)

# Por anio
cat("  Por año...\n")
boy_por_anio <- pet |>
  group_by(anio) |> group_split() |>
  lapply(function(df) {
    cat("    ", unique(df$anio), "\n")
    run_boy(df, label = as.character(unique(df$anio)), formula_boy = f_boy_completa) |>
      mutate(anio = unique(df$anio), corte = "Nacional")
  }) |> bind_rows()

# Por zona
cat("  Por zona...\n")
boy_por_zona <- pet |>
  filter(!is.na(zona_label)) |>
  group_by(anio, zona_label) |> group_split() |>
  lapply(function(df) {
    run_boy(df, label = unique(df$zona_label), formula_boy = f_boy_sin_zona) |>
      mutate(anio = unique(df$anio), corte = "Zona")
  }) |> bind_rows()

# Por etnia
cat("  Por etnia...\n")
boy_por_etnia <- pet |>
  filter(!is.na(etnia_label)) |>
  group_by(anio, etnia_label) |> group_split() |>
  lapply(function(df) {
    run_boy(df, label = unique(df$etnia_label), formula_boy = f_boy_completa) |>
      mutate(anio = unique(df$anio), corte = "Etnia")
  }) |> bind_rows()

boy_all <- bind_rows(boy_por_anio, boy_por_zona, boy_por_etnia)
write_parquet(boy_all, "datos/p1_boy_resultados.parquet")
cat("  ✓ datos/p1_boy_resultados.parquet\n")

# Contribucion por variable (Yun) — ultimo anio
cat("  Pesos de Yun...\n")
boy_yun <- tryCatch({
  set.seed(2026)
  pet_rec <- pet |> filter(anio == anio_reciente)
  d_boy <- if (nrow(pet_rec) > 40000) pet_rec |> slice_sample(n = 40000, weight_by = fex) else pet_rec

  datos_h <- d_boy |> filter(sexo_bin == 0)
  datos_m <- d_boy |> filter(sexo_bin == 1)
  logit_h <- glm(f_boy_completa, data = datos_h, family = binomial("logit"))
  logit_m <- glm(f_boy_completa, data = datos_m, family = binomial("logit"))

  coef_h <- coef(logit_h); coef_m <- coef(logit_m)
  X_h <- model.matrix(f_boy_completa, data = datos_h)
  X_m <- model.matrix(f_boy_completa, data = datos_m)
  cols_comun   <- intersect(colnames(X_m), names(coef_h))
  cols_comun_m <- intersect(colnames(X_m), names(coef_m))

  mean_x_h <- colMeans(X_h[, cols_comun, drop = FALSE])
  mean_x_m <- colMeans(X_m[, cols_comun, drop = FALSE])
  contrib_dot <- coef_h[cols_comun] * (mean_x_h - mean_x_m)

  diff_beta <- coef_h[cols_comun] - coef_m[cols_comun_m[cols_comun_m %in% cols_comun]]
  contrib_coef <- mean_x_m[names(diff_beta)] * diff_beta

  tibble(
    variable = names(contrib_dot),
    dotacion = as.numeric(contrib_dot),
    coeficiente = as.numeric(contrib_coef[names(contrib_dot)]),
    anio = anio_reciente
  ) |> filter(variable != "(Intercept)")
}, error = function(e) {
  cat("  ⚠ Error en Yun:", conditionMessage(e), "\n")
  tibble(variable = character(), dotacion = numeric(),
         coeficiente = numeric(), anio = integer())
})
write_parquet(boy_yun, "datos/p1_boy_yun.parquet")
cat("  ✓ datos/p1_boy_yun.parquet\n")

cat("\n══════════════════════════════════════════════════════════\n")
cat("  ✓ Pre-cálculo BOY completado. 2 parquets generados.\n")
cat("══════════════════════════════════════════════════════════\n")
