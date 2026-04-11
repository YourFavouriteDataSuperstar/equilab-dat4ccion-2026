# ══════════════════════════════════════════════════════════════
# 08_precalcular_p2.R
# Pre-calcula datos para Página 2 — Calidad del Empleo
# Fuente: datos/geih_ocupados_2019_2025.parquet
#
# Ejecutar UNA VEZ o cuando cambien los datos fuente.
# Desde RStudio:  source("procesamiento/08_precalcular_p2.R")
# ══════════════════════════════════════════════════════════════

cat("── Inicio pre-cálculo Página 2 (Calidad del Empleo) ──\n")
cat("   Esto puede tardar 5–10 minutos...\n\n")

library(arrow)
library(dplyr)
library(tidyr)

# ── Tablas de referencia ──────────────────────────────────────
posicion_labels <- c(
  "1" = "Empleado particular",
  "2" = "Empleado gobierno",
  "3" = "Empleo doméstico",
  "4" = "Cuenta propia",
  "5" = "Empleador",
  "6" = "Familiar sin remuneración",
  "7" = "Sin remuneración (otras emp.)",
  "8" = "Jornalero / peón",
  "9" = "Otro"
)

# CORRECCIÓN 2026-04-09: niv_edu_armon solo tiene códigos 1–6 y 9.
# La armonización colapsa todos los niveles superiores (6–11 de niv_edu_orig)
# en un solo código 6. Mapeo anterior metía código 6 en "Básica y media".
edu_grupo <- function(niv) {
  dplyr::case_when(
    niv %in% c(1, 2)   ~ "Sin educación formal",
    niv %in% c(3, 4, 5) ~ "Básica y media",
    niv == 6             ~ "Superior",
    TRUE                 ~ NA_character_
  )
}

seccion_ciiu <- function(cod) {
  cod <- as.integer(cod)
  dplyr::case_when(
    cod %in% 1:3   ~ "Agricultura y pesca",
    cod %in% 5:9   ~ "Minas y canteras",
    cod %in% 10:33 ~ "Industria manufacturera",
    cod == 35       ~ "Electricidad y gas",
    cod %in% 36:39 ~ "Agua y saneamiento",
    cod %in% 41:43 ~ "Construcción",
    cod %in% 45:47 ~ "Comercio",
    cod %in% 49:53 ~ "Transporte y almacenamiento",
    cod %in% 55:56 ~ "Alojamiento y comidas",
    cod %in% 58:63 ~ "Información y comunicaciones",
    cod %in% 64:66 ~ "Financiero y seguros",
    cod == 68       ~ "Actividades inmobiliarias",
    cod %in% 69:75 ~ "Profesionales y científicas",
    cod %in% 77:82 ~ "Servicios administrativos",
    cod == 84       ~ "Administración pública",
    cod == 85       ~ "Educación",
    cod %in% 86:88 ~ "Salud y asistencia social",
    cod %in% 90:93 ~ "Arte y entretenimiento",
    cod %in% 94:96 ~ "Otros servicios",
    cod == 97       ~ "Hogares como empleadores",
    TRUE            ~ NA_character_
  )
}

# ── 1. Cargar datos ───────────────────────────────────────────
cat("[1/6] Cargando parquet de ocupados...\n")
df_ocup <- read_parquet("datos/geih_ocupados_2019_2025.parquet")
cat("      ", format(nrow(df_ocup), big.mark = "."), "filas cargadas\n")

# ── 2. Filtro base + etiquetas ────────────────────────────────
cat("[2/6] Filtrando y etiquetando...\n")
df_base <- df_ocup |>
  filter(condicion_activ == 1, edad >= 15, edad <= 99, sexo %in% c(1, 2)) |>
  mutate(
    sexo_label     = if_else(sexo == 1, "Hombres", "Mujeres"),
    posicion_label = posicion_labels[as.character(posicion_ocup)],
    etnia_label    = if_else(etnia_bin == 1, "Grupo étnico", "Sin grupo étnico"),
    zona_label     = if_else(clase_zona == 1, "Cabecera", "Resto rural"),
    edu_label      = edu_grupo(niv_edu_armon)
  ) |>
  filter(!is.na(etnia_label), !is.na(zona_label),
         !is.na(edu_label), !is.na(posicion_label))

anio_ref <- max(df_base$anio)
cat("      ", format(nrow(df_base), big.mark = "."), "filas | año ref:", anio_ref, "\n")

# ── 3. Heatmap histórico (todos los subgrupos para filtros) ───
cat("[3/6] Heatmap histórico — todos los subgrupos...\n")

# Calcular para las 8 combinaciones para que responda a filtros
posicion_hist_list <- vector("list", 8)
combos_heat <- list(
  list(e = TRUE,  z = TRUE,  d = TRUE),
  list(e = FALSE, z = TRUE,  d = TRUE),
  list(e = TRUE,  z = FALSE, d = TRUE),
  list(e = TRUE,  z = TRUE,  d = FALSE),
  list(e = FALSE, z = FALSE, d = TRUE),
  list(e = FALSE, z = TRUE,  d = FALSE),
  list(e = TRUE,  z = FALSE, d = FALSE),
  list(e = FALSE, z = FALSE, d = FALSE)
)

for (i in seq_along(combos_heat)) {
  cc <- combos_heat[[i]]
  d <- df_base
  if (cc$e) d <- d |> mutate(etnia_label = "Todos")
  if (cc$z) d <- d |> mutate(zona_label  = "Todos")
  if (cc$d) d <- d |> mutate(edu_label   = "Todos")

  posicion_hist_list[[i]] <- d |>
    group_by(anio, posicion_label, sexo_label, etnia_label, zona_label, edu_label) |>
    summarise(n_pond = sum(fex, na.rm = TRUE), .groups = "drop") |>
    group_by(anio, posicion_label, etnia_label, zona_label, edu_label) |>
    mutate(
      pct    = round(n_pond / sum(n_pond) * 100, 1),
      n_mill = round(sum(n_pond) / 1e6, 2)
    ) |>
    ungroup()
}

posicion_hist_nac <- bind_rows(posicion_hist_list) |>
  mutate(
    etnia_f = if_else(etnia_label == "Todos", "Ninguno", etnia_label),
    zona_f  = if_else(zona_label  == "Todos", "Ninguno", zona_label),
    edu_f   = if_else(edu_label   == "Todos", "Ninguno", edu_label)
  )

cat("      ", nrow(posicion_hist_nac), "filas\n")

# ── 4. Treemap sectorial (estático, año ref) ──────────────────
cat("[4/6] Segregación sectorial — treemap nacional...\n")
seg_base <- df_base |>
  filter(anio == anio_ref, !is.na(rama2d_orig)) |>
  mutate(seccion = seccion_ciiu(rama2d_orig)) |>
  filter(!is.na(seccion)) |>
  group_by(seccion, sexo_label) |>
  summarise(pob = sum(fex, na.rm = TRUE), .groups = "drop") |>
  group_by(seccion) |>
  mutate(total = sum(pob)) |>
  ungroup()

# Índice de Duncan
duncan_val <- seg_base |>
  group_by(sexo_label) |>
  mutate(share = pob / sum(pob)) |>
  ungroup() |>
  select(seccion, sexo_label, share) |>
  pivot_wider(names_from = sexo_label, values_from = share, values_fill = 0) |>
  summarise(d = round(0.5 * sum(abs(Mujeres - Hombres)) * 100, 1)) |>
  pull(d)

segregacion_nac <- seg_base |>
  filter(sexo_label == "Mujeres") |>
  mutate(
    pct_m      = round(pob / total * 100, 1),
    total_mill = round(total / 1e6, 1),
    n_mill     = round(pob / 1e6, 1),
    duncan     = duncan_val,
    parents    = ""
  ) |>
  arrange(pct_m)

cat("      ", nrow(segregacion_nac), "sectores | Duncan:", duncan_val, "%\n")

# ── 5. Datos filtrables (8 niveles de agregación) ─────────────
cat("[5/6] Calculando desgloses para filtros (8 combinaciones)...\n")

# Todas las combinaciones de etnia × zona × edu
# Permite mezclar filtros: etnia="Grupo étnico" + zona="Cabecera" funciona
combos <- list(
  list(e = TRUE,  z = TRUE,  d = TRUE,  tag = "Ninguno (nacional)"),
  list(e = FALSE, z = TRUE,  d = TRUE,  tag = "Por etnia"),
  list(e = TRUE,  z = FALSE, d = TRUE,  tag = "Por zona"),
  list(e = TRUE,  z = TRUE,  d = FALSE, tag = "Por educación"),
  list(e = FALSE, z = FALSE, d = TRUE,  tag = "Etnia + zona"),
  list(e = FALSE, z = TRUE,  d = FALSE, tag = "Etnia + edu"),
  list(e = TRUE,  z = FALSE, d = FALSE, tag = "Zona + edu"),
  list(e = FALSE, z = FALSE, d = FALSE, tag = "Etnia + zona + edu")
)

pos_list  <- vector("list", 4)
hor_list  <- vector("list", 4)
inf_list  <- vector("list", 4)

for (i in seq_along(combos)) {
  cc <- combos[[i]]
  cat("      [", i, "/4]", cc$tag, "...\n")

  d <- df_base
  if (cc$e) d <- d |> mutate(etnia_label = "Todos")
  if (cc$z) d <- d |> mutate(zona_label  = "Todos")
  if (cc$d) d <- d |> mutate(edu_label   = "Todos")

  # Posición ocupacional — año ref
  pos_list[[i]] <- d |>
    filter(anio == anio_ref) |>
    group_by(posicion_label, sexo_label, etnia_label, zona_label, edu_label) |>
    summarise(n_pond = sum(fex, na.rm = TRUE), .groups = "drop") |>
    group_by(posicion_label, etnia_label, zona_label, edu_label) |>
    mutate(
      pct    = round(n_pond / sum(n_pond) * 100, 1),
      n_mill = round(sum(n_pond) / 1e6, 2)
    ) |>
    ungroup()

  # Horas trabajadas — año ref
  hor_list[[i]] <- d |>
    filter(anio == anio_ref, !is.na(horas_sem), horas_sem > 0) |>
    group_by(posicion_label, sexo_label, etnia_label, zona_label, edu_label) |>
    summarise(
      horas_prom = round(weighted.mean(horas_sem, w = fex, na.rm = TRUE), 1),
      n_mill     = round(sum(fex, na.rm = TRUE) / 1e6, 2),
      .groups    = "drop"
    )

  # Informalidad — todos los años
  inf_list[[i]] <- d |>
    mutate(informal = posicion_ocup %in% c(4, 6, 7, 8)) |>
    group_by(anio, sexo_label, etnia_label, zona_label, edu_label) |>
    summarise(
      pct_inf = round(sum(fex[informal], na.rm = TRUE) /
                        sum(fex, na.rm = TRUE) * 100, 1),
      n_mill  = round(sum(fex[informal], na.rm = TRUE) / 1e6, 2),
      .groups = "drop"
    )
}

# Unir y renombrar "Todos" → "Nacional" en las columnas de filtro
rename_f <- function(df) {
  df |> mutate(
    etnia_f = if_else(etnia_label == "Todos", "Ninguno", etnia_label),
    zona_f  = if_else(zona_label  == "Todos", "Ninguno", zona_label),
    edu_f   = if_else(edu_label   == "Todos", "Ninguno", edu_label)
  )
}

posicion_fil     <- bind_rows(pos_list)  |> rename_f()
horas_fil        <- bind_rows(hor_list)  |> rename_f()
informalidad_fil <- bind_rows(inf_list)  |> rename_f()

cat("      Posición filtrable: ", nrow(posicion_fil), "filas\n")
cat("      Horas filtrable:    ", nrow(horas_fil), "filas\n")
cat("      Informalidad fil.:  ", nrow(informalidad_fil), "filas\n")

# ── Value boxes (nivel nacional, año ref) ─────────────────────
pos_vb <- posicion_fil |>
  filter(etnia_f == "Ninguno", zona_f == "Ninguno", edu_f == "Ninguno")

pct_domestico_m  <- pos_vb |>
  filter(posicion_label == "Empleo doméstico",
         sexo_label == "Mujeres") |>
  pull(pct)
pct_domestico_m  <- if (length(pct_domestico_m)  == 0) NA_real_ else pct_domestico_m[1]

pct_cuentapropia_m <- pos_vb |>
  filter(posicion_label == "Cuenta propia",
         sexo_label == "Mujeres") |>
  pull(pct)
pct_cuentapropia_m <- if (length(pct_cuentapropia_m) == 0) NA_real_ else pct_cuentapropia_m[1]

pct_sinremun_m <- pos_vb |>
  filter(posicion_label == "Familiar sin remuneración",
         sexo_label == "Mujeres") |>
  pull(pct)
pct_sinremun_m <- if (length(pct_sinremun_m) == 0) NA_real_ else pct_sinremun_m[1]

hor_vb <- horas_fil |>
  filter(etnia_f == "Ninguno", zona_f == "Ninguno", edu_f == "Ninguno")

horas_m <- hor_vb |>
  filter(sexo_label == "Mujeres") |>
  summarise(h = round(weighted.mean(horas_prom, w = n_mill, na.rm = TRUE), 1)) |>
  pull(h)

horas_h <- hor_vb |>
  filter(sexo_label == "Hombres") |>
  summarise(h = round(weighted.mean(horas_prom, w = n_mill, na.rm = TRUE), 1)) |>
  pull(h)

inf_vb <- informalidad_fil |>
  filter(anio == anio_ref,
         etnia_f == "Ninguno", zona_f == "Ninguno", edu_f == "Ninguno")

pct_inf_m <- inf_vb |> filter(sexo_label == "Mujeres") |> pull(pct_inf)
pct_inf_h <- inf_vb |> filter(sexo_label == "Hombres") |> pull(pct_inf)
pct_inf_m <- if (length(pct_inf_m) == 0) NA_real_ else pct_inf_m[1]
pct_inf_h <- if (length(pct_inf_h) == 0) NA_real_ else pct_inf_h[1]

vboxes <- tibble(
  pct_domestico_m    = pct_domestico_m,
  pct_cuentapropia_m = pct_cuentapropia_m,
  pct_sinremun_m     = pct_sinremun_m,
  horas_m            = horas_m,
  horas_h            = horas_h,
  brecha_horas       = round(horas_h - horas_m, 1),
  pct_inf_m          = pct_inf_m,
  pct_inf_h          = pct_inf_h,
  duncan             = duncan_val,
  anio_ref           = anio_ref
)

# ── 6. Guardar todos los parquets ────────────────────────────
cat("[6/6] Guardando parquets...\n")

write_parquet(posicion_hist_nac, "datos/calidad_p2_posicion_hist.parquet")
write_parquet(segregacion_nac,   "datos/calidad_p2_segregacion.parquet")
write_parquet(posicion_fil,      "datos/calidad_p2_posicion_fil.parquet")
write_parquet(horas_fil,         "datos/calidad_p2_horas_fil.parquet")
write_parquet(informalidad_fil,  "datos/calidad_p2_informalidad_fil.parquet")
write_parquet(vboxes,            "datos/calidad_p2_vboxes.parquet")

cat("\n══ RESULTADOS GUARDADOS ══\n")
cat("  calidad_p2_posicion_hist.parquet  →", nrow(posicion_hist_nac), "filas\n")
cat("  calidad_p2_segregacion.parquet    →", nrow(segregacion_nac),   "filas\n")
cat("  calidad_p2_posicion_fil.parquet   →", nrow(posicion_fil),      "filas\n")
cat("  calidad_p2_horas_fil.parquet      →", nrow(horas_fil),         "filas\n")
cat("  calidad_p2_informalidad_fil.parquet →", nrow(informalidad_fil), "filas\n")
cat("  calidad_p2_vboxes.parquet         → 1 fila\n")
cat("\nValue boxes calculados:\n")
cat("  % Mujeres doméstico:", pct_domestico_m, "%\n")
cat("  % Mujeres cuenta propia:", pct_cuentapropia_m, "%\n")
cat("  % Mujeres sin remuneración familiar:", pct_sinremun_m, "%\n")
cat("  Horas prom. Mujeres:", horas_m, "h | Hombres:", horas_h, "h\n")
cat("  % Informal Mujeres:", pct_inf_m, "% | Hombres:", pct_inf_h, "%\n")
cat("  Índice de Duncan:", duncan_val, "%\n")
cat("\nAhora puedes hacer Render de p2_calidad.qmd (~15 seg)\n")
