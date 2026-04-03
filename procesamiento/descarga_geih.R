# =============================================================================
# DAT4CCIÓN - Descarga automatizada GEIH (2019-2025)
# Portal: microdatos.dane.gov.co
# Nota: el reCAPTCHA del portal es solo client-side; las URLs de descarga
#       responden 200 OK directamente sin token.
# =============================================================================

library(httr)
library(rvest)
library(stringr)

# --- Configuración -----------------------------------------------------------

DEST_BASE <- "C:/Users/Alejandra Otero/OneDrive/Documentos/DANE"

# Catálogo de IDs por año (GEIH nacional anual)
catalogo_geih <- c(
  "2019" = 599,
  "2020" = 780,
  "2021" = 701,
  "2022" = 771,
  "2023" = 782,
  "2024" = 819,
  "2025" = 853
)

# Años a descargar (ajustar si se quiere un subconjunto)
anios_descargar <- names(catalogo_geih)  # todos: 2019-2025

# --- Funciones ---------------------------------------------------------------

#' Extrae las URLs de descarga de una página get-microdata del catálogo DANE
extraer_urls_descarga <- function(catalog_id) {
  url_pagina <- paste0(
    "https://microdatos.dane.gov.co/index.php/catalog/",
    catalog_id, "/get-microdata"
  )

  cat("  Leyendo catálogo", catalog_id, "...\n")
  resp <- tryCatch(
    GET(url_pagina, timeout(30),
        add_headers("User-Agent" = "Mozilla/5.0 (compatible; R/httr)")),
    error = function(e) NULL
  )

  if (is.null(resp) || status_code(resp) != 200) {
    warning("No se pudo acceder a catalog/", catalog_id)
    return(NULL)
  }

  html <- content(resp, "text", encoding = "UTF-8")

  # Extraer: mostrarModal('NombreArchivo.zip', 'URL')
  patron <- "mostrarModal\\('([^']+\\.zip)'\\s*,\\s*'([^']+)'\\)"
  matches <- str_match_all(html, patron)[[1]]

  if (nrow(matches) == 0) {
    warning("Sin archivos encontrados en catalog/", catalog_id)
    return(NULL)
  }

  # Deduplicar por URL (el portal a veces duplica botones)
  df <- unique(data.frame(
    archivo = trimws(matches[, 2]),
    url     = trimws(matches[, 3]),
    stringsAsFactors = FALSE
  ))

  cat("  Encontrados", nrow(df), "archivos\n")
  df
}

#' Descarga un ZIP en la ruta destino, con barra de progreso
descargar_zip <- function(url, ruta_destino) {
  if (file.exists(ruta_destino)) {
    cat("    [ya existe, saltando]", basename(ruta_destino), "\n")
    return(invisible(TRUE))
  }

  dir.create(dirname(ruta_destino), recursive = TRUE, showWarnings = FALSE)

  cat("    Descargando:", basename(ruta_destino), "... ")
  inicio <- proc.time()

  resp <- tryCatch(
    GET(
      url,
      timeout(300),
      add_headers("User-Agent" = "Mozilla/5.0 (compatible; R/httr)",
                  "Referer"    = sub("/download/.*", "/get-microdata", url)),
      write_disk(ruta_destino, overwrite = TRUE),
      progress()
    ),
    error = function(e) {
      cat("ERROR:", conditionMessage(e), "\n")
      if (file.exists(ruta_destino)) file.remove(ruta_destino)
      return(NULL)
    }
  )

  if (is.null(resp)) return(invisible(FALSE))

  if (status_code(resp) != 200) {
    cat("HTTP", status_code(resp), "- fallo\n")
    if (file.exists(ruta_destino)) file.remove(ruta_destino)
    return(invisible(FALSE))
  }

  elapsed <- (proc.time() - inicio)[["elapsed"]]
  size_mb <- round(file.size(ruta_destino) / 1024^2, 1)
  cat(sprintf("OK [%.1f MB en %.0fs]\n", size_mb, elapsed))
  invisible(TRUE)
}

# --- Ejecución principal -----------------------------------------------------

cat("\n========================================================\n")
cat(" DAT4CCIÓN - Descarga GEIH", paste(anios_descargar, collapse = ", "), "\n")
cat(" Destino:", DEST_BASE, "\n")
cat("========================================================\n\n")

log_resultados <- list()

for (anio in anios_descargar) {
  catalog_id <- catalogo_geih[anio]
  cat("► Año", anio, "(catalog ID:", catalog_id, ")\n")

  archivos <- extraer_urls_descarga(catalog_id)

  if (is.null(archivos)) {
    log_resultados[[anio]] <- "ERROR al obtener lista de archivos"
    next
  }

  carpeta_anio <- file.path(DEST_BASE, anio)
  ok <- 0; fail <- 0

  for (i in seq_len(nrow(archivos))) {
    nombre <- archivos$archivo[i]
    url    <- archivos$url[i]
    ruta   <- file.path(carpeta_anio, nombre)

    exito <- descargar_zip(url, ruta)
    if (isTRUE(exito)) ok <- ok + 1 else fail <- fail + 1
  }

  log_resultados[[anio]] <- sprintf("%d OK, %d fallos", ok, fail)
  cat("  Resumen año", anio, ":", log_resultados[[anio]], "\n\n")
}

# --- Resumen final -----------------------------------------------------------

cat("========================================================\n")
cat(" RESUMEN FINAL\n")
cat("========================================================\n")
for (anio in names(log_resultados)) {
  cat(sprintf("  %s: %s\n", anio, log_resultados[[anio]]))
}
cat("\nArchivos guardados en:", DEST_BASE, "\n")
