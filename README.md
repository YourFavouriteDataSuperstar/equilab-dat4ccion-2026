# Radar de Brechas Laborales de Género — Colombia 2019–2025

**EQUILAB · DAT4CCIÓN 2026 · ONU Mujeres / CEPAL / CEEG**

[![Dashboard](https://img.shields.io/badge/Dashboard-Ver%20en%20Quarto%20Pub-blue)](PENDIENTE)
[![Datos](https://img.shields.io/badge/Datos-Zenodo%20DOI-green)](PENDIENTE)

---

## ¿Qué es este proyecto?

El **Radar de Brechas Laborales de Género** caracteriza y descompone las brechas de género en el mercado laboral colombiano entre 2019 y 2025, usando microdatos de la Gran Encuesta Integrada de Hogares (GEIH) del DANE.

El análisis combina:
- **Dashboard descriptivo interactivo** (3 páginas): acceso al mercado laboral, calidad del empleo y brechas salariales, con lentes de interseccionalidad por etnia, zona y nivel educativo.
- **Descomposición Oaxaca-Blinder**: separación del componente explicado e inexplicado de la brecha salarial entre 2019–2025.

El diseño metodológico es modular y replicable en cualquier país de América Latina con encuestas de hogares similares (ENIGH, CASEN, ECH, etc.).

---

## Estructura del repositorio

```
equilab-dat4ccion-2026/
├── dashboard.qmd            # Entregable final — dashboard HTML integrado
├── _quarto.yml              # Configuración del proyecto Quarto
│
├── analisis/                # Módulos de análisis por página (desarrollo del equipo)
│   ├── p1_acceso.qmd        # Página 1: TGP, TO, TD por sexo y año
│   ├── p2_calidad.qmd       # Página 2: Calidad del empleo y segregación
│   └── p3_salariales.qmd   # Página 3: Brechas salariales
│
├── procesamiento/           # Pipeline de datos — reproducibilidad completa
│   ├── README.md            # Instrucciones para reproducir desde cero
│   ├── descarga_geih.py     # Descarga microdatos DANE (2019–2025)
│   └── descarga_geih.R      # Alternativa en R
│
├── datos/
│   └── README.md            # Instrucciones para obtener los parquets (Zenodo)
│
└── docs/
    └── guia_metodologica_dashboard.qmd  # Marco metodológico del equipo
```

---

## Cómo reproducir el dashboard

### Paso 1 — Clonar el repositorio

```bash
git clone https://github.com/equilab-ean/equilab-dat4ccion-2026.git
cd equilab-dat4ccion-2026
```

### Paso 2 — Obtener los datos

Los parquets procesados están en Zenodo (DOI: **pendiente de publicación**).

Descarga los dos archivos y colócalos en `datos/`:

```
datos/geih_ocupados_2019_2025.parquet   # 48 MB — Páginas 2, 3 y O-B
datos/geih_pet_2019_2025.parquet        # 46 MB — Página 1 (TGP/TO/TD)
```

Si prefieres reproducir desde los microdatos originales del DANE (requiere ~50 GB de espacio y varias horas):

```bash
python procesamiento/descarga_geih.py   # Descarga microdatos por año
# Luego correr los scripts de armonización (ver procesamiento/README.md)
```

### Paso 3 — Instalar dependencias en R

```r
install.packages(c(
  "arrow",      # leer parquets
  "dplyr",      # manipulación de datos
  "ggplot2",    # visualizaciones base
  "plotly",     # gráficas interactivas
  "crosstalk",  # filtros compartidos entre gráficas
  "Hmisc",      # medianas ponderadas
  "survey",     # estimaciones con diseño muestral complejo
  "quarto"      # renderizar el dashboard
))
```

### Paso 4 — Renderizar el dashboard

```bash
quarto render dashboard.qmd
```

O publicar en Quarto Pub:

```bash
quarto publish quarto-pub dashboard.qmd
```

---

## Fuente de datos

**Gran Encuesta Integrada de Hogares (GEIH)** — DANE Colombia, 2019–2025.

- Portal de microdatos: [microdatos.dane.gov.co](https://microdatos.dane.gov.co)
- Los datos son de acceso público y gratuito.
- Los microdatos crudos no están en este repositorio por su tamaño (~50 GB). Los parquets procesados y armonizados están disponibles en Zenodo.

**Nota metodológica importante:** La GEIH cambió de marco muestral en 2021 (Censo 2005 → Censo 2018). Los totales de población no son comparables entre 2019–2020 y 2021–2025, pero las tasas y brechas relativas sí lo son. Ver `docs/guia_metodologica_dashboard.qmd` para detalles.

---

## Equipo — EQUILAB

Universidad EAN · ODEM / Ean Inspira · DAT4CCIÓN 2026

| Integrante | Rol en el proyecto |
|------------|--------------------|
| Alejandra Otero | Arquitectura, análisis Oaxaca-Blinder, integración |
| [Persona B] | Pipeline de datos, Página 3 (brechas salariales) |
| [Persona C] | Visualizaciones, Página 2 (calidad del empleo) |
| [Persona D] | Dashboard Quarto, narrativa, Página 1 (acceso laboral) |

---

## Licencia

[MIT License](LICENSE) — código libre para reutilizar y adaptar con atribución.

Los microdatos del DANE son de dominio público bajo la política de datos abiertos del gobierno colombiano.

---

*Desarrollado para DAT4CCIÓN 2026 — Datatón Regional para la Igualdad de Género. ONU Mujeres / CEPAL / CEEG.*
