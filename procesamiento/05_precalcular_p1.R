# ══════════════════════════════════════════════════════════════
# 05_precalcular_p1.R
# Pre-calcula TODAS las tasas con CV para la Página 1 (Acceso)
# Resultado: datos/tasas_p1.parquet (~630 filas, <1 MB)
#
# Ejecutar UNA VEZ o cada vez que cambien los datos fuente.
# Desde RStudio:
#   source("procesamiento/05_precalcular_p1.R")
# ══════════════════════════════════════════════════════════════

cat("── Inicio del pre-cálculo para Página 1 ──\n")
cat("   Esto puede tardar 5–10 minutos...\n\n")

library(arrow)
library(dplyr)
library(Hmisc)
library(survey)

# ── Constantes ───────────────────────────────────────────────
edu_grupo <- function(niv) {
  dplyr::case_when(
    niv %in% c(1, 2)           ~ "Sin educación formal",
    niv %in% c(3, 4, 5, 6, 7) ~ "Básica y media",
    niv %in% c(8, 9)           ~ "Técnica / Tecnológica",
    niv %in% c(10, 11)         ~ "Universitaria y más",
    TRUE                       ~ NA_character_
  )
}

semaforo_cv <- function(cv) {
  dplyr::case_when(
    cv <= 7  ~ "Preciso",
    cv <= 14 ~ "Aceptable",
    cv <= 20 ~ "Regular",
    TRUE     ~ "No confiable"
  )
}

# ── Función de cálculo survey ────────────────────────────────
run_survey <- function(data) {
  data |>
    group_modify(~ {
      tryCatch({
        if (nrow(.x) < 100) stop("muestra insuficiente")
        dptos_ok <- .x |> count(dpto_cod) |> filter(n >= 2) |> pull(dpto_cod)
        dat <- filter(.x, dpto_cod %in% dptos_ok)
        if (nrow(dat) < 100) stop("muestra insuficiente")

        dis     <- svydesign(ids = ~1, strata = ~dpto_cod, weights = ~fex, data = dat)
        tgp_s   <- svymean(~es_pea,     dis, na.rm = TRUE)
        to_s    <- svymean(~es_ocupado, dis, na.rm = TRUE)
        dis_pea <- svydesign(ids = ~1, strata = ~dpto_cod, weights = ~fex,
                              data = filter(dat, es_pea))
        td_s    <- svymean(~es_desocup, dis_pea, na.rm = TRUE)

        tibble(
          TGP    = round(100 * coef(tgp_s)["es_peaTRUE"],    1),
          cv_TGP = round(100 * SE(tgp_s)["es_peaTRUE"]    / coef(tgp_s)["es_peaTRUE"],    1),
          TO     = round(100 * coef(to_s)["es_ocupadoTRUE"], 1),
          cv_TO  = round(100 * SE(to_s)["es_ocupadoTRUE"]  / coef(to_s)["es_ocupadoTRUE"], 1),
          TD     = round(100 * coef(td_s)["es_desocupTRUE"], 1),
          cv_TD  = round(100 * SE(td_s)["es_desocupTRUE"]  / coef(td_s)["es_desocupTRUE"], 1),
          n_obs  = nrow(.x)
        )
      }, error = function(e) {
        tibble(TGP = NA_real_, cv_TGP = NA_real_,
               TO  = NA_real_, cv_TO  = NA_real_,
               TD  = NA_real_, cv_TD  = NA_real_,
               n_obs = nrow(.x))
      })
    }) |>
    ungroup()
}

# ── 1. Cargar datos ──────────────────────────────────────────
cat("[1/5] Cargando parquet PET...\n")
df_pet <- read_parquet("datos/geih_pet_2019_2025.parquet")
cat("       ", format(nrow(df_pet), big.mark = "."), "filas cargadas\n")

# ── 2. Filtro base + etiquetas ───────────────────────────────
cat("[2/5] Filtrando y creando etiquetas...\n")
df_base <- df_pet |>
  filter(sexo %in% c(1, 2), edad >= 15, edad <= 99) |>
  mutate(
    es_pea        = condicion_activ %in% c(1, 2, 3),
    es_ocupado    = condicion_activ %in% c(1, 2),
    es_desocup    = condicion_activ == 3,
    sexo_label    = if_else(sexo == 1, "Hombres", "Mujeres"),
    etnia_label   = if_else(etnia_bin == 1, "Grupo étnico", "Sin grupo étnico"),
    zona_label    = if_else(clase_zona == 1, "Cabecera", "Resto rural"),
    niv_edu_label = edu_grupo(niv_edu_armon)
  ) |>
  filter(!is.na(etnia_label), !is.na(zona_label), !is.na(niv_edu_label))

cat("       ", format(nrow(df_base), big.mark = "."), "filas tras filtro\n")

# ── 3. Pre-calcular TODAS las combinaciones ──────────────────
cat("[3/5] Calculando tasas con CV (8 niveles de agregación)...\n")
cat("       Esto es lo que tarda — paciencia...\n")

combos <- list(
  list(e = FALSE, z = FALSE, d = FALSE, label = "Específico completo"),
  list(e = TRUE,  z = FALSE, d = FALSE, label = "Todos etnia"),
  list(e = FALSE, z = TRUE,  d = FALSE, label = "Todos zona"),
  list(e = FALSE, z = FALSE, d = TRUE,  label = "Todos educación"),
  list(e = TRUE,  z = TRUE,  d = FALSE, label = "Todos etnia + zona"),
  list(e = TRUE,  z = FALSE, d = TRUE,  label = "Todos etnia + edu"),
  list(e = FALSE, z = TRUE,  d = TRUE,  label = "Todos zona + edu"),
  list(e = TRUE,  z = TRUE,  d = TRUE,  label = "Todos (nacional)")
)

tasas_completo <- do.call(bind_rows, lapply(seq_along(combos), function(i) {
  cc <- combos[[i]]
  cat("       [", i, "/8] ", cc$label, "...\n")

  d <- df_base
  if (cc$e) d <- d |> mutate(etnia_label   = "Todos")
  if (cc$z) d <- d |> mutate(zona_label    = "Todos")
  if (cc$d) d <- d |> mutate(niv_edu_label = "Todos")

  d |>
    group_by(anio, sexo_label, etnia_label, zona_label, niv_edu_label) |>
    run_survey()
})) |>
  filter(!is.na(TGP)) |>
  mutate(
    cal_TGP = semaforo_cv(cv_TGP),
    cal_TO  = semaforo_cv(cv_TO),
    cal_TD  = semaforo_cv(cv_TD),
    se_tgp  = (cv_TGP * TGP) / 100,
    li      = round(TGP - 1.96 * se_tgp, 1),
    ls      = round(TGP + 1.96 * se_tgp, 1)
  ) |>
  select(-se_tgp)

cat("       ", nrow(tasas_completo), "filas calculadas\n")

# ── 4. Tabla departamental 2025 ──────────────────────────────
cat("[4/5] Calculando tabla departamental (2025)...\n")

tabla_dpto <- df_base |>
  filter(anio == 2025) |>
  group_by(dpto_cod, sexo_label) |>
  group_modify(~ {
    tryCatch({
      if (nrow(.x) < 50) stop("small")
      dis     <- svydesign(ids = ~1, weights = ~fex, data = .x)
      tgp_s   <- svymean(~es_pea,     dis, na.rm = TRUE)
      to_s    <- svymean(~es_ocupado, dis, na.rm = TRUE)
      dis_pea <- svydesign(ids = ~1, weights = ~fex, data = filter(.x, es_pea))
      td_s    <- svymean(~es_desocup, dis_pea, na.rm = TRUE)
      tibble(
        TGP    = round(100 * coef(tgp_s)["es_peaTRUE"],   1),
        cv_TGP = round(100 * SE(tgp_s)["es_peaTRUE"]    / coef(tgp_s)["es_peaTRUE"],   1),
        TO     = round(100 * coef(to_s)["es_ocupadoTRUE"], 1),
        cv_TO  = round(100 * SE(to_s)["es_ocupadoTRUE"]  / coef(to_s)["es_ocupadoTRUE"], 1),
        TD     = round(100 * coef(td_s)["es_desocupTRUE"], 1),
        cv_TD  = round(100 * SE(td_s)["es_desocupTRUE"]  / coef(td_s)["es_desocupTRUE"], 1)
      )
    }, error = function(e)
      tibble(TGP=NA_real_,cv_TGP=NA_real_,TO=NA_real_,cv_TO=NA_real_,TD=NA_real_,cv_TD=NA_real_)
    )
  }) |>
  ungroup() |>
  mutate(
    cal_TGP = semaforo_cv(cv_TGP),
    cal_TO  = semaforo_cv(cv_TO),
    cal_TD  = semaforo_cv(cv_TD)
  )

cat("       ", nrow(tabla_dpto), "filas departamentales\n")

# ── 5. Tabla brecha de género por año ────────────────────────
cat("[5/5] Calculando tabla de brechas...\n")

tasas_global <- df_base |>
  group_by(anio, sexo_label) |>
  run_survey()

cat("       Listo\n\n")

# ── Guardar resultados ───────────────────────────────────────
write_parquet(tasas_completo, "datos/tasas_p1_completo.parquet")
write_parquet(tabla_dpto,     "datos/tasas_p1_departamental.parquet")
write_parquet(tasas_global,   "datos/tasas_p1_global.parquet")

cat("══ RESULTADOS GUARDADOS ══\n")
cat("  datos/tasas_p1_completo.parquet      →", nrow(tasas_completo), "filas\n")
cat("  datos/tasas_p1_departamental.parquet  →", nrow(tabla_dpto), "filas\n")
cat("  datos/tasas_p1_global.parquet         →", nrow(tasas_global), "filas\n")
cat("\nAhora puedes hacer Render de p1_acceso.qmd (tarda ~15 seg)\n")
