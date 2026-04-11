# ============================================================
# 11_precalcular_p4.R
# Pre-calcula indicadores laborales para mujeres cis vs trans
# Usa survey::svydesign para CV y estimaciones ponderadas
# Periodo: 2022-2025 (marco muestral 2018 — modulo de genero)
# ============================================================

library(arrow)
library(dplyr)
library(tidyr)
library(survey)
library(Hmisc)

cat("=== Iniciando precalculo p4: diversidad de genero ===\n")

# ── Carga de datos ─────────────────────────────────────────────

gen <- read_parquet("datos/geih_genero_2022_2025.parquet")
ocu <- read_parquet("datos/geih_ocupados_2019_2025.parquet") |>
  filter(anio >= 2022)
pet <- read_parquet("datos/geih_pet_2019_2025.parquet") |>
  filter(anio >= 2022)

cat("Genero:", nrow(gen), "filas\n")
cat("Ocupados 2022+:", nrow(ocu), "filas\n")
cat("PET 2022+:", nrow(pet), "filas\n")

# ── Llaves de join ─────────────────────────────────────────────

join_keys <- c("DIRECTORIO", "SECUENCIA_P", "ORDEN", "HOGAR", "mes", "anio")

# ── Construir categorias ──────────────────────────────────────

etiquetar_identidad <- function(df) {
  df |>
    filter(!is.na(identidad_genero), !is.na(sexo_nacer)) |>
    mutate(
      # Categorias detalladas
      identidad_label = case_when(
        identidad_genero == 1 & sexo_nacer == 1 ~ "Hombre cis",
        identidad_genero == 1 & sexo_nacer == 2 ~ "Hombre trans",
        identidad_genero == 2 & sexo_nacer == 2 ~ "Mujer cis",
        identidad_genero == 2 & sexo_nacer == 1 ~ "Mujer trans",
        identidad_genero == 3              ~ "No binario",
        identidad_genero %in% c(4, 5)      ~ "Otra identidad",
        TRUE ~ NA_character_
      ),
      # Eje principal: tipo de mujer (solo identidad_genero == 2)
      tipo_mujer = case_when(
        identidad_genero == 2 & sexo_nacer == 2 ~ "Mujer cis",
        identidad_genero == 2 & sexo_nacer == 1 ~ "Mujer trans",
        TRUE ~ NA_character_
      ),
      # Flag: es mujer (cis o trans)
      es_mujer = identidad_genero == 2
    )
}

# ── Funcion semaforo CV ────────────────────────────────────────

semaforo_cv <- function(cv) {
  case_when(
    cv <= 7  ~ "Preciso",
    cv <= 14 ~ "Aceptable",
    cv <= 20 ~ "Regular",
    TRUE     ~ "No confiable"
  )
}

# ============================================================
# BLOQUE 1: TASAS DE ACCESO (join con PET)
# ============================================================

cat("\n--- Bloque 1: Tasas de acceso ---\n")

pet_gen <- pet |>
  filter(edad >= 15, edad <= 99) |>
  inner_join(
    gen |> select(all_of(join_keys), sexo_nacer, identidad_genero),
    by = join_keys
  ) |>
  etiquetar_identidad() |>
  mutate(
    es_pea = condicion_activ %in% c(1, 2, 3),
    es_ocupado = condicion_activ %in% c(1, 2),
    es_desocupado = condicion_activ == 3
  )

cat("PET con genero:", nrow(pet_gen), "filas\n")

# Funcion para calcular tasas con CV
calc_tasas <- function(dat, grupo_var) {
  if (nrow(dat) < 30) return(NULL)

  resultados <- dat |>
    group_by(across(all_of(grupo_var))) |>
    group_modify(~ {
      d <- .x
      if (nrow(d) < 30) return(tibble())

      dis <- tryCatch(
        svydesign(ids = ~1, weights = ~fex, data = d),
        error = function(e) NULL
      )
      if (is.null(dis)) return(tibble())

      # TGP = PEA / PET
      tgp_s <- tryCatch(svymean(~es_pea, dis), error = function(e) NULL)
      # TO = Ocupados / PET
      to_s <- tryCatch(svymean(~es_ocupado, dis), error = function(e) NULL)

      if (is.null(tgp_s) || is.null(to_s)) return(tibble())

      tgp_est <- coef(tgp_s)["es_peaTRUE"]
      tgp_se  <- SE(tgp_s)["es_peaTRUE"]
      to_est  <- coef(to_s)["es_ocupadoTRUE"]
      to_se   <- SE(to_s)["es_ocupadoTRUE"]

      # TD = Desocupados / PEA (solo entre PEA)
      d_pea <- d |> filter(es_pea)
      td_est <- NA_real_; td_se <- NA_real_; td_cv <- NA_real_
      if (nrow(d_pea) >= 30) {
        dis_pea <- tryCatch(svydesign(ids = ~1, weights = ~fex, data = d_pea),
                            error = function(e) NULL)
        if (!is.null(dis_pea)) {
          td_s <- tryCatch(svymean(~es_desocupado, dis_pea), error = function(e) NULL)
          if (!is.null(td_s)) {
            td_est <- coef(td_s)["es_desocupadoTRUE"]
            td_se  <- SE(td_s)["es_desocupadoTRUE"]
            td_cv  <- ifelse(td_est > 0, round(100 * td_se / td_est, 1), NA_real_)
          }
        }
      }

      tibble(
        TGP = round(tgp_est * 100, 1),
        cv_TGP = round(100 * tgp_se / tgp_est, 1),
        TO = round(to_est * 100, 1),
        cv_TO = round(100 * to_se / to_est, 1),
        TD = round(td_est * 100, 1),
        cv_TD = td_cv,
        n_obs = nrow(d)
      )
    }) |>
    ungroup() |>
    mutate(
      cal_TGP = semaforo_cv(cv_TGP),
      cal_TO  = semaforo_cv(cv_TO),
      cal_TD  = semaforo_cv(cv_TD)
    )

  resultados
}

# Agregado 2022-2025 por identidad_label
tasas_identidad <- calc_tasas(pet_gen, "identidad_label") |>
  mutate(anio = "2022-2025", .before = 1)

# Agregado 2022-2025 por tipo_mujer (solo mujeres)
tasas_tipo_mujer <- pet_gen |>
  filter(!is.na(tipo_mujer)) |>
  calc_tasas("tipo_mujer") |>
  mutate(anio = "2022-2025", .before = 1)

# Por anio x identidad_label
tasas_identidad_anio <- pet_gen |>
  calc_tasas(c("anio", "identidad_label")) |>
  mutate(anio = as.character(anio))

# Por anio x tipo_mujer
tasas_tipo_mujer_anio <- pet_gen |>
  filter(!is.na(tipo_mujer)) |>
  calc_tasas(c("anio", "tipo_mujer")) |>
  mutate(anio = as.character(anio))

# Combinar
tasas_acceso <- bind_rows(
  tasas_identidad |> rename(grupo = identidad_label) |> mutate(tipo = "identidad"),
  tasas_tipo_mujer |> rename(grupo = tipo_mujer) |> mutate(tipo = "tipo_mujer"),
  tasas_identidad_anio |> rename(grupo = identidad_label) |> mutate(tipo = "identidad"),
  tasas_tipo_mujer_anio |> rename(grupo = tipo_mujer) |> mutate(tipo = "tipo_mujer")
)

write_parquet(tasas_acceso, "datos/p4d_tasas_acceso.parquet")
cat("Guardado: p4d_tasas_acceso.parquet (", nrow(tasas_acceso), "filas)\n")

# ============================================================
# BLOQUE 2: CALIDAD DEL EMPLEO (join con ocupados)
# ============================================================

cat("\n--- Bloque 2: Calidad del empleo ---\n")

ocu_gen <- ocu |>
  filter(condicion_activ == 1, edad >= 15, edad <= 99) |>
  inner_join(
    gen |> select(all_of(join_keys), sexo_nacer, identidad_genero),
    by = join_keys
  ) |>
  etiquetar_identidad() |>
  mutate(
    es_informal = posicion_ocup %in% c(4, 6, 7, 8)
  )

cat("Ocupados con genero:", nrow(ocu_gen), "filas\n")

# Funcion para indicadores de calidad
calc_calidad <- function(dat, grupo_var) {
  dat |>
    group_by(across(all_of(grupo_var))) |>
    group_modify(~ {
      d <- .x
      if (nrow(d) < 30) return(tibble())

      dis <- tryCatch(
        svydesign(ids = ~1, weights = ~fex, data = d),
        error = function(e) NULL
      )
      if (is.null(dis)) return(tibble())

      # Informalidad
      inf_s <- tryCatch(svymean(~es_informal, dis), error = function(e) NULL)
      inf_est <- NA_real_; inf_cv <- NA_real_
      if (!is.null(inf_s)) {
        inf_est <- coef(inf_s)["es_informalTRUE"]
        inf_se  <- SE(inf_s)["es_informalTRUE"]
        inf_cv  <- ifelse(inf_est > 0, round(100 * inf_se / inf_est, 1), NA_real_)
      }

      # Horas semanales (media ponderada)
      horas_s <- tryCatch(svymean(~horas_sem, dis, na.rm = TRUE), error = function(e) NULL)
      horas_est <- NA_real_; horas_cv <- NA_real_
      if (!is.null(horas_s)) {
        horas_est <- coef(horas_s)
        horas_se  <- SE(horas_s)
        horas_cv  <- ifelse(horas_est > 0, round(100 * horas_se / horas_est, 1), NA_real_)
      }

      # Mediana de horas ponderada
      med_horas <- tryCatch(
        wtd.quantile(d$horas_sem, weights = d$fex, probs = 0.5, na.rm = TRUE),
        error = function(e) NA_real_
      )

      tibble(
        tasa_informalidad = round(inf_est * 100, 1),
        cv_informalidad = inf_cv,
        cal_informalidad = semaforo_cv(inf_cv),
        media_horas = round(horas_est, 1),
        cv_horas = horas_cv,
        cal_horas = semaforo_cv(horas_cv),
        mediana_horas = round(as.numeric(med_horas), 1),
        n_obs = nrow(d)
      )
    }) |>
    ungroup()
}

# Agregado por identidad_label
calidad_identidad <- calc_calidad(ocu_gen, "identidad_label") |>
  mutate(anio = "2022-2025", .before = 1)

# Agregado por tipo_mujer
calidad_tipo_mujer <- ocu_gen |>
  filter(!is.na(tipo_mujer)) |>
  calc_calidad("tipo_mujer") |>
  mutate(anio = "2022-2025", .before = 1)

# Por anio x identidad_label
calidad_identidad_anio <- ocu_gen |>
  calc_calidad(c("anio", "identidad_label")) |>
  mutate(anio = as.character(anio))

# Combinar
calidad <- bind_rows(
  calidad_identidad |> rename(grupo = identidad_label) |> mutate(tipo = "identidad"),
  calidad_tipo_mujer |> rename(grupo = tipo_mujer) |> mutate(tipo = "tipo_mujer"),
  calidad_identidad_anio |> rename(grupo = identidad_label) |> mutate(tipo = "identidad")
)

write_parquet(calidad, "datos/p4d_calidad.parquet")
cat("Guardado: p4d_calidad.parquet (", nrow(calidad), "filas)\n")

# Distribucion por posicion ocupacional
posicion_dist <- ocu_gen |>
  filter(!is.na(tipo_mujer), !is.na(posicion_ocup)) |>
  mutate(
    posicion_label = case_when(
      posicion_ocup == 1 ~ "Empleado particular",
      posicion_ocup == 2 ~ "Empleado gobierno",
      posicion_ocup == 3 ~ "Empleado domestico",
      posicion_ocup == 4 ~ "Cuenta propia",
      posicion_ocup == 5 ~ "Patron o empleador",
      posicion_ocup == 6 ~ "Trab. familiar sin pago",
      posicion_ocup %in% c(7, 8) ~ "Jornalero / Sin remun.",
      posicion_ocup == 9 ~ "Otro",
      TRUE ~ "Desconocido"
    )
  ) |>
  group_by(tipo_mujer, posicion_label) |>
  summarise(pob = sum(fex), n_obs = n(), .groups = "drop") |>
  group_by(tipo_mujer) |>
  mutate(pct = round(pob / sum(pob) * 100, 1)) |>
  ungroup()

write_parquet(posicion_dist, "datos/p4d_posicion_ocupacional.parquet")
cat("Guardado: p4d_posicion_ocupacional.parquet (", nrow(posicion_dist), "filas)\n")

# ============================================================
# BLOQUE 3: SALARIOS (join con ocupados, filtro ingresos)
# ============================================================

cat("\n--- Bloque 3: Salarios ---\n")

sal_gen <- ocu_gen |>
  filter(
    !is.na(ing_mes), ing_mes > 0,
    !is.na(horas_sem), horas_sem > 0
  ) |>
  mutate(sal_hora = ing_mes / (horas_sem * 4.33))

cat("Ocupados con ingreso y genero:", nrow(sal_gen), "filas\n")

calc_salarios <- function(dat, grupo_var) {
  dat |>
    group_by(across(all_of(grupo_var))) |>
    group_modify(~ {
      d <- .x
      if (nrow(d) < 30) return(tibble())

      # Medianas ponderadas
      med_mes <- tryCatch(
        as.numeric(wtd.quantile(d$ing_mes, weights = d$fex, probs = 0.5, na.rm = TRUE)),
        error = function(e) NA_real_
      )
      med_hora <- tryCatch(
        as.numeric(wtd.quantile(d$sal_hora, weights = d$fex, probs = 0.5, na.rm = TRUE)),
        error = function(e) NA_real_
      )

      # Media ponderada con CV (survey)
      dis <- tryCatch(svydesign(ids = ~1, weights = ~fex, data = d), error = function(e) NULL)
      media_mes <- NA_real_; cv_ing <- NA_real_
      if (!is.null(dis)) {
        ing_s <- tryCatch(svymean(~ing_mes, dis, na.rm = TRUE), error = function(e) NULL)
        if (!is.null(ing_s)) {
          media_mes <- round(as.numeric(coef(ing_s)[1]), 0)
          cv_ing <- round(100 * as.numeric(SE(ing_s)[1]) / as.numeric(coef(ing_s)[1]), 1)
        }
      }

      tibble(
        mediana_mensual = round(med_mes, 0),
        mediana_hora = round(med_hora, 0),
        media_mensual = media_mes,
        cv_ingreso = cv_ing,
        cal_ingreso = semaforo_cv(cv_ing),
        n_obs = nrow(d)
      )
    }) |>
    ungroup()
}

# Agregado por identidad_label
salarios_identidad <- calc_salarios(sal_gen, "identidad_label") |>
  mutate(anio = "2022-2025", .before = 1)

# Agregado por tipo_mujer
salarios_tipo_mujer <- sal_gen |>
  filter(!is.na(tipo_mujer)) |>
  calc_salarios("tipo_mujer") |>
  mutate(anio = "2022-2025", .before = 1)

# Combinar
salarios <- bind_rows(
  salarios_identidad |> rename(grupo = identidad_label) |> mutate(tipo = "identidad"),
  salarios_tipo_mujer |> rename(grupo = tipo_mujer) |> mutate(tipo = "tipo_mujer")
)

write_parquet(salarios, "datos/p4d_salarios.parquet")
cat("Guardado: p4d_salarios.parquet (", nrow(salarios), "filas)\n")

# ============================================================
# BLOQUE 4: PANORAMA RESUMEN
# ============================================================

cat("\n--- Bloque 4: Panorama resumen ---\n")

# Conteo de poblacion por identidad
panorama_pob <- gen |>
  filter(!is.na(identidad_genero), !is.na(sexo_nacer)) |>
  etiquetar_identidad() |>
  group_by(identidad_label) |>
  summarise(
    n_obs = n(),
    poblacion = sum(fex),
    edad_mediana = median(edad),
    .groups = "drop"
  ) |>
  mutate(pct_pob = round(poblacion / sum(poblacion) * 100, 2))

write_parquet(panorama_pob, "datos/p4d_panorama_poblacion.parquet")
cat("Guardado: p4d_panorama_poblacion.parquet (", nrow(panorama_pob), "filas)\n")

# Orientacion sexual por tipo_mujer
orient_mujer <- gen |>
  filter(!is.na(identidad_genero), !is.na(sexo_nacer)) |>
  etiquetar_identidad() |>
  filter(!is.na(tipo_mujer), !is.na(orientacion_sex)) |>
  mutate(
    orientacion_label = case_when(
      orientacion_sex == 1 ~ "Hombres",
      orientacion_sex == 2 ~ "Mujeres",
      orientacion_sex == 3 ~ "Ambos sexos",
      orientacion_sex == 4 ~ "Otro",
      TRUE ~ NA_character_
    )
  ) |>
  group_by(tipo_mujer, orientacion_label) |>
  summarise(n_obs = n(), poblacion = sum(fex), .groups = "drop") |>
  group_by(tipo_mujer) |>
  mutate(pct = round(poblacion / sum(poblacion) * 100, 1)) |>
  ungroup()

write_parquet(orient_mujer, "datos/p4d_orientacion_mujer.parquet")
cat("Guardado: p4d_orientacion_mujer.parquet (", nrow(orient_mujer), "filas)\n")

cat("\n=== Precalculo p4 completado ===\n")
