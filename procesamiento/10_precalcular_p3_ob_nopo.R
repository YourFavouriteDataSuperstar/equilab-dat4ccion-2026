#!/usr/bin/env Rscript
# ============================================================================
# PRE-CALCULO — Pagina 3: Oaxaca-Blinder y Nopo Matching
#
# Genera parquets listos para el dashboard:
#   datos/p3_ob_resultados.parquet      -> O-B por anio/zona/etnia
#   datos/p3_ob_detalle_vars.parquet    -> O-B detalle por variable (ultimo anio)
#   datos/p3_nopo_resultados.parquet    -> Nopo por anio
#
# Ejecutar desde la raiz del proyecto:
#   Rscript procesamiento/10_precalcular_p3_ob_nopo.R
# ============================================================================

library(arrow)
library(dplyr)
library(tidyr)
library(oaxaca)

cat("\n══════════════════════════════════════════════════════════\n")
cat("  EQUILAB — Pre-cálculo Página 3: O-B + Ñopo\n")
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

# Por anio
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
write_parquet(ob_all, "datos/p3_ob_resultados.parquet")
cat("  ✓ datos/p3_ob_resultados.parquet\n")

# Detalle por variable (ultimo anio)
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
write_parquet(ob_detalle, "datos/p3_ob_detalle_vars.parquet")
cat("  ✓ datos/p3_ob_detalle_vars.parquet\n\n")


# ════════════════════════════════════════════════════════════════════════════
# MODELO 2: NOPO MATCHING
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

write_parquet(nopo_por_anio, "datos/p3_nopo_resultados.parquet")
cat("  ✓ datos/p3_nopo_resultados.parquet\n\n")

cat("\n══════════════════════════════════════════════════════════\n")
cat("  ✓ Pre-cálculo O-B + Ñopo completado. 3 parquets generados.\n")
cat("══════════════════════════════════════════════════════════\n")
