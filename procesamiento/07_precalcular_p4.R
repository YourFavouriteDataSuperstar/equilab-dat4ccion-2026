#!/usr/bin/env Rscript
# ============================================================================
# PRE-CÁLCULO — Página 4: Descomposición de brechas de género
#
# Genera parquets listos para el dashboard:
#   datos/p4_ob_resultados.parquet      → O-B por año/zona/etnia
#   datos/p4_ob_detalle_vars.parquet    → O-B detalle por variable (último año)
#   datos/p4_nopo_resultados.parquet    → Ñopo por año
#   datos/p4_boy_resultados.parquet     → BOY por año/zona/etnia
#   datos/p4_boy_yun.parquet            → BOY contribución por variable (último año)
#
# Ejecutar desde la raíz del proyecto:
#   Rscript procesamiento/07_precalcular_p4.R
# ============================================================================

library(arrow)
library(dplyr)
library(tidyr)
library(oaxaca)

cat("\n══════════════════════════════════════════════════════════\n")
cat("  EQUILAB — Pre-cálculo Página 4 (3 modelos)\n")
cat("══════════════════════════════════════════════════════════\n\n")

# ════════════════════════════════════════════════════════════════════════════
# DATOS COMUNES
# ════════════════════════════════════════════════════════════════════════════

cat("► Cargando datos de ocupados...\n")
dat_raw <- read_parquet("datos/geih_ocupados_2019_2025.parquet")

dat <- dat_raw |>
  filter(
    !is.na(ing_mes), ing_mes > 0,
    !is.na(horas_sem), horas_sem > 0,
    !is.na(esc_anios),
    !is.na(sexo), sexo %in% c(1, 2),
    !is.na(edad), edad >= 18, edad <= 65,
    !is.na(fex)
  ) |>
  mutate(
    sal_hora     = ing_mes / (horas_sem * 4.33),
    log_sal      = log(sal_hora),
    sexo_bin     = ifelse(sexo == 2, 1, 0),
    exp_pot      = pmax(edad - esc_anios - 6, 0),
    exp_pot2     = exp_pot^2,
    zona_label   = ifelse(clase_zona == 1, "Urbano", "Rural"),
    etnia_label  = case_when(
      etnia_bin == 0 ~ "No étnico",
      etnia_bin == 1 ~ "Étnico",
      TRUE ~ NA_character_
    ),
    pos_ocup_cat = case_when(
      posicion_ocup %in% c(1, 2) ~ "Asalariado",
      posicion_ocup %in% c(3, 7) ~ "Independiente",
      posicion_ocup == 8 ~ "Jornalero",
      TRUE ~ "Otro"
    ),
    rama1d = substr(rama2d_orig, 1, 1),
    esc_grupo = cut(esc_anios, breaks = c(-Inf, 5, 11, 16, Inf),
                    labels = c("Primaria", "Secundaria", "Superior", "Posgrado")),
    edad_grupo_nopo = cut(edad, breaks = c(17, 25, 35, 45, 55, 65),
                          labels = c("18-25", "26-35", "36-45", "46-55", "56-65"))
  ) |>
  group_by(anio) |>
  filter(
    sal_hora > quantile(sal_hora, 0.01, na.rm = TRUE),
    sal_hora < quantile(sal_hora, 0.99, na.rm = TRUE)
  ) |>
  ungroup()

anio_reciente <- max(dat$anio)
cat("  N ocupados:", nrow(dat), "| Años:", min(dat$anio), "-", max(dat$anio), "\n\n")


# ════════════════════════════════════════════════════════════════════════════
# MODELO 1: OAXACA-BLINDER
# ════════════════════════════════════════════════════════════════════════════

cat("━━━ MODELO 1: Oaxaca-Blinder ━━━\n")

f_nacional <- log_sal ~ esc_anios + exp_pot + exp_pot2 +
  factor(pos_ocup_cat) + factor(rama1d) + factor(clase_zona) | sexo_bin
f_sin_zona <- log_sal ~ esc_anios + exp_pot + exp_pot2 +
  factor(pos_ocup_cat) + factor(rama1d) | sexo_bin
f_simple <- log_sal ~ esc_anios + exp_pot + exp_pot2 +
  factor(pos_ocup_cat) | sexo_bin

run_ob <- function(df, label = "General", formula = f_nacional) {
  n_h <- sum(df$sexo_bin == 0)
  n_m <- sum(df$sexo_bin == 1)
  if (n_h < 50 | n_m < 50) {
    return(tibble(grupo = label,
      componente = c("Brecha total", "Explicado", "No explicado"),
      valor = rep(NA_real_, 3), se = rep(NA_real_, 3),
      n_hombres = n_h, n_mujeres = n_m))
  }
  if (nrow(df) > 20000) {
    set.seed(2026)
    df <- df |> slice_sample(n = 20000, weight_by = fex)
  }
  ob <- tryCatch(oaxaca(formula, data = df, R = 30), error = function(e) NULL)
  if (is.null(ob)) {
    return(tibble(grupo = label,
      componente = c("Brecha total", "Explicado", "No explicado"),
      valor = rep(NA_real_, 3), se = rep(NA_real_, 3),
      n_hombres = n_h, n_mujeres = n_m))
  }
  tw_all <- ob$twofold$overall
  tw_row <- tw_all[tw_all[, "group.weight"] == 0, , drop = FALSE]
  tibble(grupo = label,
    componente = c("Brecha total", "Explicado", "No explicado"),
    valor = c(ob$y$y.diff, tw_row[1, "coef(explained)"], tw_row[1, "coef(unexplained)"]),
    se = c(NA_real_, tw_row[1, "se(explained)"], tw_row[1, "se(unexplained)"]),
    n_hombres = n_h, n_mujeres = n_m)
}

# Por año
cat("  Por año...\n")
ob_por_anio <- dat |>
  group_by(anio) |> group_split() |>
  lapply(function(df) {
    cat("    ", unique(df$anio), "\n")
    run_ob(df, label = as.character(unique(df$anio))) |>
      mutate(anio = unique(df$anio), corte = "Nacional")
  }) |> bind_rows()

# Por zona
cat("  Por zona...\n")
ob_por_zona <- dat |>
  filter(!is.na(clase_zona)) |>
  group_by(anio, zona_label) |> group_split() |>
  lapply(function(df) {
    run_ob(df, label = unique(df$zona_label), formula = f_sin_zona) |>
      mutate(anio = unique(df$anio), corte = "Zona")
  }) |> bind_rows()

# Por etnia
cat("  Por etnia...\n")
ob_por_etnia <- dat |>
  filter(!is.na(etnia_label)) |>
  group_by(anio, etnia_label) |> group_split() |>
  lapply(function(df) {
    run_ob(df, label = unique(df$etnia_label), formula = f_simple) |>
      mutate(anio = unique(df$anio), corte = "Etnia")
  }) |> bind_rows()

ob_all <- bind_rows(ob_por_anio, ob_por_zona, ob_por_etnia)
write_parquet(ob_all, "datos/p4_ob_resultados.parquet")
cat("  ✓ datos/p4_ob_resultados.parquet\n")

# Detalle por variable (último año)
cat("  Detalle por variable...\n")
ob_detalle <- tryCatch({
  set.seed(2026)
  dat_det <- dat |> filter(anio == anio_reciente)
  if (nrow(dat_det) > 20000) dat_det <- dat_det |> slice_sample(n = 20000, weight_by = fex)
  ob_tmp <- oaxaca(f_nacional, data = dat_det, R = 30)
  vars_tw <- ob_tmp$twofold$variables[[1]]
  tibble(
    variable     = rownames(vars_tw),
    explicado    = vars_tw[, "coef(explained)"],
    no_explicado = vars_tw[, "coef(unexplained)"],
    anio         = anio_reciente
  ) |> filter(variable != "(Intercept)")
}, error = function(e) {
  cat("  ⚠ Error en detalle:", conditionMessage(e), "\n")
  tibble(variable = character(), explicado = numeric(),
         no_explicado = numeric(), anio = integer())
})
write_parquet(ob_detalle, "datos/p4_ob_detalle_vars.parquet")
cat("  ✓ datos/p4_ob_detalle_vars.parquet\n\n")


# ════════════════════════════════════════════════════════════════════════════
# MODELO 2: ÑOPO MATCHING
# ════════════════════════════════════════════════════════════════════════════

cat("━━━ MODELO 2: Ñopo Matching ━━━\n")

run_nopo <- function(df, label = "General") {
  n_h <- sum(df$sexo_bin == 0)
  n_m <- sum(df$sexo_bin == 1)
  if (n_h < 50 | n_m < 50) {
    return(tibble(grupo = label,
      componente = c("Brecha total", "D_0", "D_X", "D_M", "D_F"),
      valor = rep(NA_real_, 5), n_hombres = n_h, n_mujeres = n_m,
      n_celdas_comun = NA_integer_, n_celdas_total = NA_integer_,
      pct_matching = NA_real_))
  }

  d_n <- df |>
    filter(complete.cases(pick(all_of(c("esc_grupo", "edad_grupo_nopo", "pos_ocup_cat", "zona_label"))))) |>
    mutate(celda = paste(esc_grupo, edad_grupo_nopo, pos_ocup_cat, zona_label, sep = "|"))

  hombres <- d_n |> filter(sexo_bin == 0)
  mujeres <- d_n |> filter(sexo_bin == 1)
  celdas_h     <- unique(hombres$celda)
  celdas_m     <- unique(mujeres$celda)
  celdas_comun <- intersect(celdas_h, celdas_m)
  n_celdas_total <- length(union(celdas_h, celdas_m))

  h_comun <- hombres |> filter(celda %in% celdas_comun)
  m_comun <- mujeres |> filter(celda %in% celdas_comun)
  h_fuera <- hombres |> filter(!celda %in% celdas_comun)
  m_fuera <- mujeres |> filter(!celda %in% celdas_comun)

  y_h <- mean(hombres$log_sal); y_m <- mean(mujeres$log_sal)
  D_total <- y_h - y_m

  D_M <- if (nrow(h_fuera) > 0) {
    (nrow(h_fuera) / nrow(hombres)) * (mean(h_fuera$log_sal) - mean(h_comun$log_sal))
  } else 0

  D_F <- if (nrow(m_fuera) > 0) {
    (nrow(m_fuera) / nrow(mujeres)) * (mean(m_comun$log_sal) - mean(m_fuera$log_sal))
  } else 0

  resumen_celdas <- d_n |>
    filter(celda %in% celdas_comun) |>
    group_by(celda, sexo_bin) |>
    summarise(media_log_sal = mean(log_sal), n = n(), .groups = "drop") |>
    pivot_wider(names_from = sexo_bin, values_from = c(media_log_sal, n), names_sep = "_") |>
    mutate(brecha_celda = media_log_sal_0 - media_log_sal_1,
           peso_m = n_1 / sum(n_1, na.rm = TRUE))

  D_0 <- sum(resumen_celdas$brecha_celda * resumen_celdas$peso_m, na.rm = TRUE)
  D_X <- D_total - D_M - D_F - D_0
  pct_match <- (nrow(h_comun) + nrow(m_comun)) / (nrow(hombres) + nrow(mujeres)) * 100

  tibble(grupo = label,
    componente = c("Brecha total", "D_0", "D_X", "D_M", "D_F"),
    valor = c(D_total, D_0, D_X, D_M, D_F),
    n_hombres = n_h, n_mujeres = n_m,
    n_celdas_comun = length(celdas_comun),
    n_celdas_total = n_celdas_total,
    pct_matching = pct_match)
}

nopo_por_anio <- dat |>
  group_by(anio) |> group_split() |>
  lapply(function(df) {
    cat("  ", unique(df$anio), "\n")
    set.seed(2026)
    d_s <- if (nrow(df) > 30000) df |> slice_sample(n = 30000, weight_by = fex) else df
    run_nopo(d_s, label = as.character(unique(df$anio))) |>
      mutate(anio = unique(df$anio))
  }) |> bind_rows()

write_parquet(nopo_por_anio, "datos/p4_nopo_resultados.parquet")
cat("  ✓ datos/p4_nopo_resultados.parquet\n\n")


# ════════════════════════════════════════════════════════════════════════════
# MODELO 3: BOY (participación laboral)
# ════════════════════════════════════════════════════════════════════════════

cat("━━━ MODELO 3: BOY (participación) ━━━\n")
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

cat("  N PET:", nrow(pet), "\n")

# Función BOY genérica
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

# Por año
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
write_parquet(boy_all, "datos/p4_boy_resultados.parquet")
cat("  ✓ datos/p4_boy_resultados.parquet\n")

# Contribución por variable (Yun) — último año
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
write_parquet(boy_yun, "datos/p4_boy_yun.parquet")
cat("  ✓ datos/p4_boy_yun.parquet\n")

cat("\n══════════════════════════════════════════════════════════\n")
cat("  ✓ Pre-cálculo completado. 5 parquets generados.\n")
cat("══════════════════════════════════════════════════════════\n")
