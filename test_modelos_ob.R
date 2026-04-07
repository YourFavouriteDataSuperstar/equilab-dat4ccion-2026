#!/usr/bin/env Rscript
# ============================================================================
# TEST: Tres modelos de descomposición de brechas de género
#   1. Oaxaca-Blinder clásico (twofold) → salarios
#   2. Ñopo matching (no paramétrico) → salarios
#   3. BOY (Blinder-Oaxaca-Yun) → participación laboral (PET)
#
# Ejecutar desde la raíz del proyecto:
#   Rscript test_modelos_ob.R
# ============================================================================

library(arrow)
library(dplyr)
library(tidyr)
library(oaxaca)

cat("\n══════════════════════════════════════════════════════════\n")
cat("  EQUILAB — Test de modelos de descomposición\n")
cat("══════════════════════════════════════════════════════════\n\n")

# ── 1. Carga y preparación — OCUPADOS (para O-B y Ñopo) ────────────────────
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
    sal_hora  = ing_mes / (horas_sem * 4.33),
    log_sal   = log(sal_hora),
    sexo_bin  = ifelse(sexo == 2, 1, 0),   # 1=Mujer, 0=Hombre
    exp_pot   = pmax(edad - esc_anios - 6, 0),
    exp_pot2  = exp_pot^2,
    zona_label = ifelse(clase_zona == 1, "Urbano", "Rural"),
    pos_ocup_cat = case_when(
      posicion_ocup %in% c(1, 2) ~ "Asalariado",
      posicion_ocup %in% c(3, 7) ~ "Independiente",
      posicion_ocup == 8 ~ "Jornalero",
      TRUE ~ "Otro"
    ),
    rama1d = substr(rama2d_orig, 1, 1),
    # Para Ñopo: discretizar variables continuas
    esc_grupo = cut(esc_anios, breaks = c(-Inf, 5, 11, 16, Inf),
                    labels = c("Primaria", "Secundaria", "Superior", "Posgrado")),
    edad_grupo = cut(edad, breaks = c(17, 25, 35, 45, 55, 65),
                     labels = c("18-25", "26-35", "36-45", "46-55", "56-65"))
  ) |>
  group_by(anio) |>
  filter(
    sal_hora > quantile(sal_hora, 0.01, na.rm = TRUE),
    sal_hora < quantile(sal_hora, 0.99, na.rm = TRUE)
  ) |>
  ungroup()

# Año más reciente para test
anio_test <- max(dat$anio)
d <- dat |> filter(anio == anio_test)

cat("Año de prueba:", anio_test, "\n")
cat("N ocupados:", nrow(d), "| Hombres:", sum(d$sexo_bin == 0),
    "| Mujeres:", sum(d$sexo_bin == 1), "\n")
cat("Salario hora promedio H:", round(mean(d$sal_hora[d$sexo_bin == 0]), 0),
    "| M:", round(mean(d$sal_hora[d$sexo_bin == 1]), 0), "\n")
cat("Log-salario promedio H:", round(mean(d$log_sal[d$sexo_bin == 0]), 4),
    "| M:", round(mean(d$log_sal[d$sexo_bin == 1]), 4), "\n\n")


# ════════════════════════════════════════════════════════════════════════════
# MODELO 1: OAXACA-BLINDER CLÁSICO (twofold)
# ════════════════════════════════════════════════════════════════════════════
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat("  MODELO 1: Oaxaca-Blinder Clásico (twofold)\n")
cat("  Variable dep: log(salario/hora) | Método: paramétrico\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

set.seed(2026)
d_ob <- if (nrow(d) > 20000) d |> slice_sample(n = 20000, weight_by = fex) else d

ob <- oaxaca(
  log_sal ~ esc_anios + exp_pot + exp_pot2 +
    factor(pos_ocup_cat) + factor(rama1d) +
    factor(clase_zona) | sexo_bin,
  data = d_ob,
  R = 30
)

# Extraer resultados (group.weight = 0 → coef. hombres como referencia)
tw <- ob$twofold$overall
tw_row <- tw[tw[, "group.weight"] == 0, , drop = FALSE]

brecha_ob    <- ob$y$y.diff
explicado_ob <- tw_row[1, "coef(explained)"]
no_expl_ob   <- tw_row[1, "coef(unexplained)"]

cat("  Brecha total (log H-M):      ", round(brecha_ob, 4), "\n")
cat("  Brecha total (%):            ", round((exp(abs(brecha_ob)) - 1) * 100, 1), "%\n")
cat("  Comp. explicado:             ", round(explicado_ob, 4),
    " (", round(explicado_ob * 100, 1), " log-pts×100)\n")
cat("  Comp. no explicado:          ", round(no_expl_ob, 4),
    " (", round(no_expl_ob * 100, 1), " log-pts×100)\n")
cat("  Verificación (sum = brecha):  ",
    round((explicado_ob + no_expl_ob), 4), " vs ", round(brecha_ob, 4), "\n\n")

# Detalle por variable
vars_tw <- ob$twofold$variables[[1]]
cat("  Detalle por variable (explicado | no explicado):\n")
for (v in rownames(vars_tw)) {
  if (v == "(Intercept)") next
  cat("    ", sprintf("%-30s %+.4f | %+.4f", v,
              vars_tw[v, "coef(explained)"],
              vars_tw[v, "coef(unexplained)"]), "\n")
}


# ════════════════════════════════════════════════════════════════════════════
# MODELO 2: ÑOPO MATCHING (no paramétrico)
# ════════════════════════════════════════════════════════════════════════════
cat("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat("  MODELO 2: Ñopo Matching (descomposición no paramétrica)\n")
cat("  Variable dep: log(salario/hora) | Método: matching exacto\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

# Ñopo (2008): Descompone la brecha en 4 componentes:
#   D = D_X + D_M + D_F + D_0
#   D_X  = diferencias en distribución de características (soporte común)
#   D_M  = ventaja salarial de hombres fuera del soporte común
#   D_F  = desventaja salarial de mujeres fuera del soporte común
#   D_0  = brecha inexplicada dentro del soporte común (≈ discriminación)

set.seed(2026)
d_nopo <- if (nrow(d) > 30000) d |> slice_sample(n = 30000, weight_by = fex) else d

# Variables de matching (discretas)
match_vars <- c("esc_grupo", "edad_grupo", "pos_ocup_cat", "zona_label")

d_nopo <- d_nopo |>
  filter(complete.cases(pick(all_of(match_vars)))) |>
  mutate(celda = paste(esc_grupo, edad_grupo, pos_ocup_cat, zona_label, sep = "|"))

hombres <- d_nopo |> filter(sexo_bin == 0)
mujeres <- d_nopo |> filter(sexo_bin == 1)

celdas_h <- unique(hombres$celda)
celdas_m <- unique(mujeres$celda)
celdas_comun <- intersect(celdas_h, celdas_m)
celdas_solo_h <- setdiff(celdas_h, celdas_m)
celdas_solo_m <- setdiff(celdas_m, celdas_h)

cat("  Celdas de matching:\n")
cat("    Total hombres:", length(celdas_h), "| Total mujeres:", length(celdas_m), "\n")
cat("    Soporte común:", length(celdas_comun),
    "| Solo H:", length(celdas_solo_h),
    "| Solo M:", length(celdas_solo_m), "\n\n")

h_comun <- hombres |> filter(celda %in% celdas_comun)
m_comun <- mujeres |> filter(celda %in% celdas_comun)
h_fuera <- hombres |> filter(!celda %in% celdas_comun)
m_fuera <- mujeres |> filter(!celda %in% celdas_comun)

cat("  N en soporte común: H=", nrow(h_comun), " M=", nrow(m_comun), "\n")
cat("  N fuera de soporte: H=", nrow(h_fuera), " M=", nrow(m_fuera), "\n\n")

# Brecha total
y_h <- mean(hombres$log_sal)
y_m <- mean(mujeres$log_sal)
D_total <- y_h - y_m

# D_M: ventaja de hombres fuera del soporte
if (nrow(h_fuera) > 0) {
  D_M <- (nrow(h_fuera) / nrow(hombres)) *
    (mean(h_fuera$log_sal) - mean(h_comun$log_sal))
} else {
  D_M <- 0
}

# D_F: desventaja de mujeres fuera del soporte
if (nrow(m_fuera) > 0) {
  D_F <- (nrow(m_fuera) / nrow(mujeres)) *
    (mean(m_comun$log_sal) - mean(m_fuera$log_sal))
} else {
  D_F <- 0
}

# D_0: brecha dentro del soporte común (matching por celda)
resumen_celdas <- d_nopo |>
  filter(celda %in% celdas_comun) |>
  group_by(celda, sexo_bin) |>
  summarise(media_log_sal = mean(log_sal), n = n(), .groups = "drop") |>
  pivot_wider(names_from = sexo_bin, values_from = c(media_log_sal, n),
              names_sep = "_")

resumen_celdas <- resumen_celdas |>
  mutate(
    brecha_celda = media_log_sal_0 - media_log_sal_1,
    peso_m = n_1 / sum(n_1, na.rm = TRUE)
  )

D_0 <- sum(resumen_celdas$brecha_celda * resumen_celdas$peso_m, na.rm = TRUE)

# D_X: composición (residuo)
D_X <- D_total - D_M - D_F - D_0

cat("  ── Resultados Ñopo ──\n")
cat("  Brecha total (log H-M):        ", round(D_total, 4),
    " (", round(D_total * 100, 1), " log-pts×100)\n")
cat("  D_0  (brecha soporte común):   ", round(D_0, 4),
    " (", round(D_0 * 100, 1), " lp) ← ≈ discriminación\n")
cat("  D_X  (composición/dotaciones): ", round(D_X, 4),
    " (", round(D_X * 100, 1), " lp) ← diferencias en características\n")
cat("  D_M  (ventaja H fuera soporte):", round(D_M, 4),
    " (", round(D_M * 100, 1), " lp)\n")
cat("  D_F  (desventaja M fuera sop.):", round(D_F, 4),
    " (", round(D_F * 100, 1), " lp)\n")
cat("  Verificación (sum = brecha):    ",
    round(D_X + D_M + D_F + D_0, 4), " vs ", round(D_total, 4), "\n")


# ════════════════════════════════════════════════════════════════════════════
# MODELO 3: BOY — Participación laboral (PET completa)
# ════════════════════════════════════════════════════════════════════════════
cat("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat("  MODELO 3: BOY (Blinder-Oaxaca-Yun) — Participación laboral\n")
cat("  Variable dep: participa (0/1) | Datos: PET completa\n")
cat("  condicion_activ: 1,2,3 = PEA (participa); 4,5,6 = no participa\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

# Cargar PET
pet_raw <- read_parquet("datos/geih_pet_2019_2025.parquet")

pet <- pet_raw |>
  filter(
    anio == anio_test,
    !is.na(sexo), sexo %in% c(1, 2),
    !is.na(edad), edad >= 15, edad <= 65,
    !is.na(esc_anios),
    !is.na(condicion_activ),
    !is.na(fex)
  ) |>
  mutate(
    sexo_bin   = ifelse(sexo == 2, 1, 0),   # 1=Mujer
    participa  = ifelse(condicion_activ %in% c(1, 2, 3), 1, 0),  # PEA
    exp_pot    = pmax(edad - esc_anios - 6, 0),
    exp_pot2   = exp_pot^2,
    zona_label = ifelse(clase_zona == 1, "Urbano", "Rural"),
    edad_grupo = cut(edad, breaks = c(14, 25, 35, 45, 55, 65),
                     labels = c("15-25", "26-35", "36-45", "46-55", "56-65"))
  )

cat("  N PET:", nrow(pet), "| Hombres:", sum(pet$sexo_bin == 0),
    "| Mujeres:", sum(pet$sexo_bin == 1), "\n")

# Tasas de participación por sexo
tasa_h <- mean(pet$participa[pet$sexo_bin == 0])
tasa_m <- mean(pet$participa[pet$sexo_bin == 1])
brecha_part <- tasa_h - tasa_m  # H - M (hombres participan más)

cat("  Tasa participación hombres:", round(tasa_h * 100, 1), "%\n")
cat("  Tasa participación mujeres:", round(tasa_m * 100, 1), "%\n")
cat("  Brecha (H - M):            ", round(brecha_part * 100, 1), " pp\n\n")

# Distribución de condicion_activ por sexo
cat("  Distribución condicion_activ (%):\n")
for (ca in 1:6) {
  n_h <- sum(pet$condicion_activ == ca & pet$sexo_bin == 0)
  n_m <- sum(pet$condicion_activ == ca & pet$sexo_bin == 1)
  pct_h <- round(n_h / sum(pet$sexo_bin == 0) * 100, 1)
  pct_m <- round(n_m / sum(pet$sexo_bin == 1) * 100, 1)
  cat(sprintf("    Código %d: H=%5.1f%%  M=%5.1f%%\n", ca, pct_h, pct_m))
}

# Muestrear para velocidad
set.seed(2026)
d_boy <- if (nrow(pet) > 40000) pet |> slice_sample(n = 40000, weight_by = fex) else pet

# Modelo logit por grupo
datos_h <- d_boy |> filter(sexo_bin == 0)
datos_m <- d_boy |> filter(sexo_bin == 1)

formula_boy <- participa ~ esc_anios + exp_pot + exp_pot2 +
  factor(zona_label) + factor(edad_grupo)

logit_h <- glm(formula_boy, data = datos_h, family = binomial(link = "logit"))
logit_m <- glm(formula_boy, data = datos_m, family = binomial(link = "logit"))

cat("\n  Coeficientes logit (Hombres vs Mujeres):\n")
coef_h <- coef(logit_h)
coef_m <- coef(logit_m)
all_names <- union(names(coef_h), names(coef_m))
for (nm in all_names) {
  ch <- if (nm %in% names(coef_h)) sprintf("%+.4f", coef_h[nm]) else "     NA"
  cm <- if (nm %in% names(coef_m)) sprintf("%+.4f", coef_m[nm]) else "     NA"
  cat(sprintf("    %-35s H: %s  M: %s\n", nm, ch, cm))
}

# ── Descomposición BOY/Fairlie ──────────────────────────────────────────────
# Contrafactual: P(participa=1 | X_mujer, beta_hombre) vs observado
X_m <- model.matrix(formula_boy, data = datos_m)
X_h <- model.matrix(formula_boy, data = datos_h)

# Columnas comunes (por si algún nivel de factor está ausente en un grupo)
cols_comun <- intersect(colnames(X_m), names(coef_h))
cols_comun_m <- intersect(colnames(X_m), names(coef_m))

# Predicciones
pred_m_con_beta_h <- plogis(X_m[, cols_comun, drop = FALSE] %*% coef_h[cols_comun])
pred_m_con_beta_m <- plogis(X_m[, cols_comun_m, drop = FALSE] %*% coef_m[cols_comun_m])
pred_h_con_beta_h <- plogis(X_h[, cols_comun, drop = FALSE] %*% coef_h[cols_comun])

# Tasas predichas
tasa_pred_h <- mean(pred_h_con_beta_h)  # H con sus propios coef
tasa_cf     <- mean(pred_m_con_beta_h)  # M con coef de H (contrafactual)
tasa_pred_m <- mean(pred_m_con_beta_m)  # M con sus propios coef

# Descomposición en 3 partes
# Brecha total ≈ (tasa_pred_h - tasa_cf) + (tasa_cf - tasa_pred_m) + residuo
comp_coef <- tasa_pred_h - tasa_cf      # diferencia por coeficientes (no explicado)
comp_dot  <- tasa_cf - tasa_pred_m      # diferencia por dotaciones (explicado)
residuo   <- brecha_part - comp_coef - comp_dot

cat("\n  ── Descomposición BOY (participación laboral) ──\n")
cat("  Brecha observada (H-M):       ", round(brecha_part * 100, 2), " pp\n")
cat("  Tasa pred. H (beta_H, X_H):   ", round(tasa_pred_h * 100, 2), "%\n")
cat("  Contrafactual (beta_H, X_M):  ", round(tasa_cf * 100, 2), "%\n")
cat("  Tasa pred. M (beta_M, X_M):   ", round(tasa_pred_m * 100, 2), "%\n\n")
cat("  Comp. dotaciones (explicado):  ", round(comp_dot * 100, 2), " pp\n")
cat("    → diferencia porque M tienen distintas X (edu, edad, zona)\n")
cat("  Comp. coeficientes (no expl.): ", round(comp_coef * 100, 2), " pp\n")
cat("    → diferencia porque las mismas X producen efecto distinto en M\n")
cat("  Residuo (aprox. predicción):   ", round(residuo * 100, 2), " pp\n")
cat("  Verificación: ", round((comp_dot + comp_coef + residuo) * 100, 2),
    " pp vs brecha ", round(brecha_part * 100, 2), " pp\n")

# ── Contribución por variable (Yun, 2004) ──────────────────────────────────
# Pesos de Yun para descomposición detallada
cat("\n  ── Contribución por variable (pesos de Yun) ──\n")

# Media de X por grupo
mean_x_h <- colMeans(X_h[, cols_comun, drop = FALSE])
mean_x_m <- colMeans(X_m[, cols_comun, drop = FALSE])

# Contribución de cada variable al componente de dotaciones
# W_dot_k = beta_h_k * (mean_X_h_k - mean_X_m_k) / sum(...)
diff_x <- mean_x_h - mean_x_m
contrib_dot <- coef_h[cols_comun] * diff_x

# Contribución al componente de coeficientes
# W_coef_k = mean_X_m_k * (beta_h_k - beta_m_k)
diff_beta <- coef_h[cols_comun] - coef_m[cols_comun_m[cols_comun_m %in% cols_comun]]
contrib_coef <- mean_x_m[names(diff_beta)] * diff_beta

cat(sprintf("    %-35s %10s  %10s\n", "Variable", "Dotación", "Coeficiente"))
cat("    ", strrep("─", 58), "\n")
for (nm in names(contrib_dot)) {
  if (nm == "(Intercept)") next
  cd <- if (!is.na(contrib_dot[nm])) sprintf("%+.4f", contrib_dot[nm]) else "      NA"
  cc <- if (nm %in% names(contrib_coef) && !is.na(contrib_coef[nm]))
    sprintf("%+.4f", contrib_coef[nm]) else "      NA"
  cat(sprintf("    %-35s %10s  %10s\n", nm, cd, cc))
}


# ════════════════════════════════════════════════════════════════════════════
# RESUMEN COMPARATIVO
# ════════════════════════════════════════════════════════════════════════════
cat("\n\n══════════════════════════════════════════════════════════\n")
cat("  RESUMEN COMPARATIVO — Año", anio_test, "\n")
cat("══════════════════════════════════════════════════════════\n\n")

cat(sprintf("  %-25s %-18s %-18s %-18s\n",
            "", "O-B Clásico", "Ñopo Matching", "BOY (particip.)"))
cat("  ", strrep("─", 75), "\n")
cat(sprintf("  %-25s %-18s %-18s %-18s\n",
            "Variable dep.", "log(sal/hora)", "log(sal/hora)", "participa (0/1)"))
cat(sprintf("  %-25s %-18s %-18s %-18s\n",
            "Datos", "Ocupados", "Ocupados", "PET completa"))
cat(sprintf("  %-25s %-18s %-18s %-18s\n",
            "Método", "Paramétrico OLS", "Matching exacto", "Logit no-lineal"))
cat(sprintf("  %-25s %-18s %-18s %-18s\n",
            "Brecha total",
            paste0(round(brecha_ob * 100, 1), " lp"),
            paste0(round(D_total * 100, 1), " lp"),
            paste0(round(brecha_part * 100, 1), " pp")))
cat(sprintf("  %-25s %-18s %-18s %-18s\n",
            "Explicado (dotaciones)",
            paste0(round(explicado_ob * 100, 1), " lp"),
            paste0(round(D_X * 100, 1), " lp"),
            paste0(round(comp_dot * 100, 1), " pp")))
cat(sprintf("  %-25s %-18s %-18s %-18s\n",
            "No explicado",
            paste0(round(no_expl_ob * 100, 1), " lp"),
            paste0(round(D_0 * 100, 1), " lp"),
            paste0(round(comp_coef * 100, 1), " pp")))
cat("\n  lp = log-puntos ×100 | pp = puntos porcentuales\n")
cat("  Ñopo adicional → D_M + D_F (fuera soporte):",
    round((D_M + D_F) * 100, 1), "lp\n")

cat("\n══════════════════════════════════════════════════════════\n")
cat("  INTERPRETACIÓN NARRATIVA\n")
cat("══════════════════════════════════════════════════════════\n\n")
cat("  O-B y Ñopo analizan la MISMA brecha (salarios) con métodos distintos:\n")
cat("    O-B asume forma funcional lineal → puede sobreestimar explicado.\n")
cat("    Ñopo no asume forma funcional → valida soporte común.\n")
cat("    Si D_0 (Ñopo) ≈ no explicado (O-B), los resultados son robustos.\n\n")
cat("  BOY analiza una brecha DISTINTA (participación laboral):\n")
cat("    Captura la barrera de ENTRADA al mercado (selección).\n")
cat("    O-B y Ñopo solo ven a quienes ya están empleados.\n")
cat("    Juntos cuentan la historia completa: acceso → salarios.\n")
cat("══════════════════════════════════════════════════════════\n")
