# Plan de Migración: Dashboard → Quarto Website con Closeread

**Fecha:** 6 de abril de 2026
**Decisión tomada por:** Alejandra Otero
**Estado:** Aprobado, pendiente de ejecución paso a paso

---

## ¿Qué vamos a hacer?

Migrar el proyecto de un `type: default` (un solo dashboard.qmd) a un `type: website` que combina **dos tipos de páginas**:

- **Páginas narrativas** con scrollytelling (formato `closeread-html`) → para la portada y la ficha metodológica
- **Páginas de dashboard** interactivo (formato `dashboard`) → para las 3 secciones de datos con filtros

El resultado es un sitio HTML estático (sin servidor) que se puede publicar en GitHub Pages o Quarto Pub.

---

## ¿Por qué no Shiny?

Shiny requiere un servidor R corriendo en tiempo real. Nuestro entregable para DAT4CCIÓN debe ser un HTML estático público y funcional. Con Quarto Website + crosstalk + plotly logramos interactividad (filtros, tooltips, sliders) sin infraestructura de servidor.

---

## Estructura objetivo

```
equilab-dat4ccion-2026/
├── _quarto.yml              ← CAMBIAR a type: website + navbar + render list
├── _extensions/closeread/   ← YA INSTALADA (quarto add qmd-lab/closeread)
│
├── index.qmd                ← CREAR — Portada (format: closeread-html)
├── p1_acceso.qmd            ← CREAR — Dashboard Acceso Laboral (format: dashboard)
├── p2_calidad.qmd           ← CREAR — Dashboard Calidad del Empleo (format: dashboard)
├── p3_salariales.qmd        ← CREAR — Dashboard Brechas Salariales (format: dashboard)
├── metodologia.qmd          ← CREAR — Ficha Metodológica (format: closeread-html)
│
├── analisis/                ← SIN TOCAR — código fuente original de cada persona
├── datos/                   ← SIN TOCAR — Parquets de la GEIH
├── docs/                    ← SIN TOCAR — guía metodológica original
├── procesamiento/           ← SIN TOCAR — scripts de descarga
├── dashboard.qmd            ← CONSERVAR como respaldo hasta que todo funcione
└── GUIA_EQUIPO.md           ← ACTUALIZAR al final con nueva estructura
```

---

## Cómo funciona cada tipo de página

### Páginas closeread (index.qmd, metodologia.qmd)

Closeread divide la pantalla en dos columnas:
- **Izquierda (scroll):** texto narrativo que el usuario lee al bajar
- **Derecha (sticky):** contenido fijo (imagen, gráfico, card) que cambia conforme avanza el texto

Sintaxis básica:

```markdown
:::{.cr-section}

:::{focus-on="elemento1"}
Primer párrafo de contexto. El usuario lee esto mientras
el contenido sticky se mantiene a la derecha.
:::

:::{focus-on="elemento2"}
Segundo párrafo. Al llegar aquí, el sticky cambia.
:::

:::{#cr-elemento1 .sticky}
<!-- Contenido que se queda pegado: imagen, gráfico, card -->
![Imagen de ejemplo](ruta/imagen.png)
:::

:::{#cr-elemento2 .sticky}
<!-- Segundo contenido sticky -->
```{r}
# Puede ser un gráfico plotly
ggplotly(mi_grafico)
```
:::

:::
```

### Páginas dashboard (p1, p2, p3)

Usan el formato Quarto Dashboard con filtros crosstalk del lado del cliente.

Estructura:

```markdown
---
format:
  dashboard:
    orientation: rows
    scrolling: true
    theme: cosmo
---

# Título de Pestaña

## Row {height="15%"}

### {.valuebox}
<!-- Indicador clave -->

## Row {height="50%"}

### {width="60%"}
<!-- Gráfico con filtro crosstalk -->

### {width="40%"}
<!-- Otro gráfico -->
```

Los filtros interactivos se hacen con crosstalk:

```r
library(crosstalk)
sd <- SharedData$new(mis_datos_agregados)
filter_select("id_filtro", "Seleccione año:", sd, ~anio)
ggplotly(ggplot(sd, aes(...)))  # reacciona al filtro automáticamente
```

**Importante:** crosstalk filtra filas en el navegador, no recalcula. Los datos deben llegar pre-agregados por todas las combinaciones de filtros que queramos ofrecer.

---

## _quarto.yml objetivo

```yaml
project:
  type: website
  output-dir: _site
  render:
    - index.qmd
    - p1_acceso.qmd
    - p2_calidad.qmd
    - p3_salariales.qmd
    - metodologia.qmd

website:
  title: "Radar de Brechas Laborales de Género"
  navbar:
    background: "#1a1a2e"
    foreground: "#ffffff"
    left:
      - text: "Inicio"
        href: index.qmd
      - text: "Acceso Laboral"
        href: p1_acceso.qmd
      - text: "Calidad del Empleo"
        href: p2_calidad.qmd
      - text: "Brechas Salariales"
        href: p3_salariales.qmd
      - text: "Metodología"
        href: metodologia.qmd
    right:
      - icon: github
        href: https://github.com/equilab-ean/equilab-dat4ccion-2026

  page-footer:
    center: |
      Fuente: DANE, GEIH 2019–2025. Cálculos: EQUILAB — DAT4CCIÓN 2026.
      Estimaciones ponderadas con factor de expansión (fex).
    background: "#1a1a2e"

execute:
  echo: false
  warning: false
  message: false
  cache: false

lang: es
```

La clave es `render:` — le dice a Quarto que SOLO renderice esos 5 archivos e ignore `analisis/`, `docs/`, etc.

---

## Pasos de ejecución (en orden)

### Paso 1: Crear index.qmd (portada closeread)
- Empezar con un ejemplo mínimo de closeread (sin CSS complejo)
- Presentación del proyecto + cards de navegación a las 3 páginas
- Probar con `quarto render index.qmd` antes de seguir

### Paso 2: Crear p1_acceso.qmd (dashboard)
- Tomar el código de `analisis/p1_accesoeind.qmd`
- Cambiar formato a `dashboard` con layout de rows
- Agregar value boxes y filtros crosstalk
- Ajustar rutas de datos a `datos/` (sin `../`)
- Probar con `quarto render p1_acceso.qmd`

### Paso 3: Crear p2_calidad.qmd (dashboard)
- Tomar código de `analisis/p2_calidad.qmd`
- Mismo patrón que p1: formato dashboard + crosstalk
- Probar individualmente

### Paso 4: Crear p3_salariales.qmd (dashboard)
- Tomar código de `analisis/p3_salarialesnuevo.qmd`
- Mantener el slider de plotly nativo para años (ya funciona bien)
- Agregar value boxes
- Probar individualmente

### Paso 5: Cambiar _quarto.yml
- Solo cuando las 4 páginas anteriores compilen sin errores
- Cambiar `type: default` → `type: website`
- Agregar navbar y render list
- Probar con `quarto preview`

### Paso 6: Crear metodologia.qmd (closeread)
- Ficha metodológica narrativa con la info de `docs/guia_metodologica_dashboard.qmd`
- Documentar fuentes, filtros, decisiones éticas, limitaciones

### Paso 7: Optimizar rendimiento
- Pre-calcular datos agregados en un script R separado
- Guardar como archivos .rds pequeños
- Las páginas dashboard leen los .rds en vez de los Parquets pesados
- Esto reduce el tiempo de render de minutos a segundos

---

## Convenciones que se mantienen

- Colores: `COLOR_HOMBRES = "#2166AC"`, `COLOR_MUJERES = "#D6604D"`
- Filtros base: `condicion_activ == 1`, edad 15–99, sexo 1 o 2
- Factor de expansión `fex` en todas las estimaciones
- Línea de cambio de marco muestral en 2021
- Caption: "Fuente: DANE, GEIH 2019–2025. Cálculos: EQUILAB — DAT4CCIÓN 2026."

---

## Requisitos

- **Quarto** >= 1.4 (para soporte de dashboard)
- **Extensión closeread** ya instalada (`_extensions/qmd-lab/closeread/`)
- **Paquetes R:** arrow, dplyr, tidyr, ggplot2, plotly, Hmisc, survey, gt, crosstalk, htmltools, scales

---

## Para publicar (cuando esté listo)

```bash
quarto publish gh-pages      # GitHub Pages
quarto publish quarto-pub    # Quarto Pub
```

O copiar `_site/` a cualquier hosting estático.
