# Radar de Brechas Laborales de Genero — Colombia 2019-2025

**EQUILAB - DAT4CCION 2026 - ONU Mujeres / CEPAL / CEEG**

[![Dashboard](https://img.shields.io/badge/Dashboard-Ver%20en%20Quarto%20Pub-blue)](PENDIENTE)
[![Datos](https://img.shields.io/badge/Datos-Zenodo%20DOI-green)](PENDIENTE)

---

## Que es este proyecto?

El **Radar de Brechas Laborales de Genero** caracteriza y descompone las brechas de genero en el mercado laboral colombiano entre 2019 y 2025, usando microdatos de la Gran Encuesta Integrada de Hogares (GEIH) del DANE.

El tablero es un sitio web interactivo con 6 paginas construido en Quarto, con una portada narrativa tipo scrollytelling (closeread) y 4 momentos de analisis:

| Pagina | Contenido |
|--------|-----------|
| **Inicio** | Portada scrollytelling — contexto, metodologia y navegacion |
| **P1 — Acceso laboral** | TGP, TO, TD por sexo, etnia, zona y departamento (2019-2025) |
| **P2 — Calidad del empleo** | Informalidad, posicion ocupacional, horas y segregacion |
| **P3 — Brechas salariales** | Distribuciones salariales, brechas por educacion, boxplots comparativos |
| **P4 — Mujeres diversas** | Oaxaca-Blinder, Nopo, Blinder-Oaxaca-Yun + analisis de diversidades de genero |
| **Metodologia** | Guia metodologica, fuentes y limitaciones |

El diseno metodologico es modular y replicable en cualquier pais de America Latina con encuestas de hogares similares (ENIGH, CASEN, ECH, etc.).

---

## Estructura del repositorio

```
equilab-dat4ccion-2026/
├── _quarto.yml              # Configuracion del proyecto Quarto (type: website)
├── index.qmd                # Portada scrollytelling (closeread)
├── p1_acceso.qmd            # Pagina 1: Acceso al mercado laboral
├── p2_calidad.qmd           # Pagina 2: Calidad del empleo
├── p3_salariales.qmd        # Pagina 3: Brechas salariales
├── p4_mujeres_diversas.qmd  # Pagina 4: Mujeres diversas + descomposiciones
├── metodologia.qmd          # Guia metodologica
│
├── styles/
│   └── equilab.css           # Estilos personalizados del tablero
│
├── analisis/                 # Borradores de analisis del equipo (desarrollo)
│   ├── p1_accesoeind.qmd
│   ├── P2_ calidad borrador.qmd
│   └── p3_salarialesnuevo.qmd
│
├── procesamiento/            # Pipeline de datos — reproducibilidad completa
│   ├── README.md             # Instrucciones para reproducir desde cero
│   ├── 01_descarga_geih.py   # Descarga microdatos DANE (2019-2025)
│   ├── 02_extraer_geih.py    # Extrae CSVs de los ZIPs
│   ├── 03_crear_parquet_ocupados.py  # Armoniza panel de ocupados
│   ├── 04_crear_parquet_pet.py       # Armoniza panel PET
│   ├── 05_crear_parquet_genero.py    # Extrae identidad de genero
│   ├── 05_precalcular_p1.R   # Precalcula tasas para Pagina 1
│   ├── 06_precalcular_p2.R   # Precalcula indicadores Pagina 2
│   ├── 07_precalcular_p4.R   # Precalcula descomposiciones Pagina 4
│   ├── 07b_precalcular_p3.R  # Precalcula brechas Pagina 3
│   └── 08_precalcular_p4_diversidad.R  # Precalcula analisis diversidades
│
├── datos/
│   ├── README.md             # Instrucciones para obtener los datos
│   ├── geih_ocupados_2019_2025.parquet   # Panel de ocupados (base)
│   ├── geih_pet_2019_2025.parquet        # Panel PET (base)
│   ├── geih_genero_2022_2025.parquet     # Variables de genero diverso
│   ├── tasas_p1_*.parquet                # Precalculados Pagina 1
│   ├── calidad_p2_*.parquet              # Precalculados Pagina 2
│   ├── p3_*.parquet                      # Precalculados Pagina 3
│   ├── p4_ob_*.parquet                   # Precalculados Pagina 4 (O-B)
│   ├── p4_nopo_*.parquet                 # Precalculados Pagina 4 (Nopo)
│   ├── p4_boy_*.parquet                  # Precalculados Pagina 4 (B-O-Yun)
│   └── p4d_*.parquet                     # Precalculados diversidades
│
├── docs/
│   ├── guia_metodologica_dashboard.qmd   # Marco metodologico del equipo
│   └── reporte_problemas_datos.txt       # Documentacion de problemas encontrados
│
├── _site/                    # Salida HTML renderizada (no se commitea)
└── tablero_radar.zip         # ZIP para compartir el tablero sin servidor
```

---

## Como ver el tablero (sin R ni Quarto)

Si solo quieres **navegar el tablero** sin instalar nada:

1. Descarga `tablero_radar.zip` del repositorio
2. Descomprime la carpeta
3. Abre `index.html` en tu navegador

---

## Como reproducir el dashboard

### Paso 1 — Clonar el repositorio

```bash
git clone https://github.com/equilab-ean/equilab-dat4ccion-2026.git
cd equilab-dat4ccion-2026
```

### Paso 2 — Obtener los datos

Los parquets procesados estan en Zenodo (DOI: **pendiente de publicacion**).

Descarga los archivos base y colocalos en `datos/`:

| Archivo | Tamano | Descripcion |
|---------|--------|-------------|
| `geih_ocupados_2019_2025.parquet` | 48 MB | Panel de ocupados 2019-2025 |
| `geih_pet_2019_2025.parquet` | 46 MB | Panel PET completo 2019-2025 |
| `geih_genero_2022_2025.parquet` | ~15 MB | Variables identidad de genero 2022-2025 |

Los parquets precalculados (`tasas_p1_*`, `calidad_p2_*`, `p3_*`, `p4_*`, `p4d_*`) se generan con los scripts de `procesamiento/` (pasos 05-08).

Si prefieres reproducir desde los microdatos originales del DANE (~50 GB, varias horas):

```bash
python procesamiento/01_descarga_geih.py
# Ver procesamiento/README.md para el pipeline completo
```

### Paso 3 — Instalar dependencias en R

```r
install.packages(c(
  "arrow",       # leer parquets
  "dplyr",       # manipulacion de datos
  "tidyr",       # reshape de datos
  "ggplot2",     # visualizaciones base
  "plotly",      # graficas interactivas
  "gt",          # tablas formateadas
  "scales",      # formato de ejes y etiquetas
  "survey",      # estimaciones con diseno muestral complejo
  "oaxaca",      # descomposicion Oaxaca-Blinder
  "quarto"       # renderizar el sitio
))
```

Tambien necesitas la extension Quarto [closeread](https://closeread.netlify.app/) para la portada scrollytelling:

```bash
quarto add qmd-lab/closeread
```

### Paso 4 — Precalcular datos

Los `.qmd` del tablero leen parquets precalculados (no los microdatos directamente). Si no los descargaste de Zenodo, ejecutalos en orden:

```r
source("procesamiento/05_precalcular_p1.R")
source("procesamiento/06_precalcular_p2.R")
source("procesamiento/07b_precalcular_p3.R")
source("procesamiento/07_precalcular_p4.R")
source("procesamiento/08_precalcular_p4_diversidad.R")
```

### Paso 5 — Renderizar el sitio

```bash
quarto render
```

El sitio se genera en `_site/`. Abre `_site/index.html` en tu navegador.

Para publicar en Quarto Pub:

```bash
quarto publish quarto-pub
```

---

## Fuente de datos

**Gran Encuesta Integrada de Hogares (GEIH)** — DANE Colombia, 2019-2025.

- Portal de microdatos: [microdatos.dane.gov.co](https://microdatos.dane.gov.co)
- Los datos son de acceso publico y gratuito.
- Los microdatos crudos no estan en este repositorio por su tamano (~50 GB). Los parquets procesados y armonizados estan disponibles en Zenodo.

**Nota metodologica importante:** La GEIH cambio de marco muestral en 2021 (Censo 2005 -> Censo 2018). Los totales de poblacion no son comparables entre 2019-2020 y 2021-2025, pero las tasas y brechas relativas si lo son. Ver `docs/guia_metodologica_dashboard.qmd` para detalles.

---

## Equipo — EQUILAB

Universidad EAN - ODEM / Ean Inspira - DAT4CCION 2026

| Integrante | Rol en el proyecto |
|------------|--------------------|
| Alejandra Otero | Lider tecnica, arquitectura, integracion, analisis Oaxaca-Blinder |
| Jeidy Alzate | Analisis y desarrollo |
| Sofia Lamprea | Analisis y desarrollo |
| Juliana Pemberthy | Analisis y desarrollo |

---

## Licencia

[MIT License](LICENSE) — codigo libre para reutilizar y adaptar con atribucion.

Los microdatos del DANE son de dominio publico bajo la politica de datos abiertos del gobierno colombiano.

---

*Desarrollado para DAT4CCION 2026 — Dataton Regional para la Igualdad de Genero. ONU Mujeres / CEPAL / CEEG.*
