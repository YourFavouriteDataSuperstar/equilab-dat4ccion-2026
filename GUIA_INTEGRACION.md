# Guía de integración — EQUILAB DAT4CCIÓN 2026

Cómo pegar tu análisis en la plantilla del sitio web.

---

## Arquitectura del sitio

```
equilab-dat4ccion-2026/
│
├── _quarto.yml              ← Configuración global: navbar, footer, CSS
├── styles/equilab.css       ← Todos los estilos visuales del sitio
│
├── index.qmd                ← Portada (closeread — narrativa con scroll)
├── p1_acceso.qmd            ← Dashboard: Acceso Laboral
├── p2_calidad.qmd           ← Dashboard: Calidad del Empleo
├── p3_salariales.qmd        ← Dashboard: Brechas Salariales
├── metodologia.qmd          ← Ficha metodológica (closeread)
│
├── analisis/                ← Archivos de trabajo individuales (NO tocar)
├── datos/                   ← Parquets de la GEIH (NO tocar)
└── GUIA_INTEGRACION.md      ← Este archivo
```

Cuando se ejecuta `quarto render` o `quarto preview`, Quarto lee el
`_quarto.yml`, aplica el CSS global y renderiza los 5 archivos `.qmd`
como páginas del sitio. El resultado va a la carpeta `_site/`.

---

## Estructura de cada dashboard (p1, p2, p3)

Cada dashboard tiene exactamente la misma estructura base:

```
YAML header
└── chunk setup          ← librerías + constantes + carga de datos

# Pestaña 1
└── Row (15%)            ← value boxes (indicadores clave)
└── Row (85%)
    ├── Col 60%          ← gráfico principal
    └── Col 40%          ← gráfico de apoyo o tabla

# Pestaña 2
└── (misma estructura)

# Pestaña 3
└── (misma estructura, puede usar Col 100% si solo hay 1 visualización)
```

---

## Cómo integrar tu análisis — paso a paso

### 1. Abre el archivo .qmd correspondiente

- Tu módulo va en `p1_acceso.qmd`, `p2_calidad.qmd` o `p3_salariales.qmd`
- No modificar los otros archivos ni `_quarto.yml`

### 2. Completa el bloque `setup`

Agrega tus librerías debajo del comentario `# ── Librerías` y tu código
de carga de datos debajo de `# ── Carga de datos`. Las constantes de
color y el caption NO se modifican:

```r
# ── Librerías ────────────────────────────────────────────────
library(arrow)
library(dplyr)
library(ggplot2)
library(plotly)

# ── Carga de datos ───────────────────────────────────────────
dat <- read_parquet("datos/geih_ocupados_2019_2025.parquet") |>
  filter(condicion_activ == 1, edad >= 15, edad <= 99, sexo %in% c(1,2))
```

> **Rutas de datos:** usar siempre `datos/` (sin `../`).
> Los dashboards viven en la raíz del proyecto, no en `analisis/`.

### 3. Pon un nombre a cada pestaña

Cambia el texto `Pestaña 1`, `Pestaña 2`, etc. por el nombre real:

```markdown
# Tasa de Ocupación {.tabset}
# Horas Trabajadas {.tabset}
# Informalidad {.tabset}
```

### 4. Actualiza los value boxes

Cada pestaña tiene una fila de value boxes arriba. Cambia el `title`
y el `value` por el indicador real calculado en setup:

```r
#| content: valuebox
#| title: "Tasa de ocupación — Mujeres"
list(
  value = paste0(round(tasa_ocup_mujeres, 1), "%"),
  color = "#D6604D",
  icon  = "person-fill"
)
```

Paleta de íconos disponible: https://icons.getbootstrap.com
Colores fijos del proyecto:
  - Mujeres:   `"#D6604D"`
  - Hombres:   `"#2166AC"`
  - Brecha:    `"#0f3460"`
  - Neutro:    `"#6c757d"`

### 5. Pega tu gráfico en el chunk indicado

Busca el comentario `# ── PEGAR AQUÍ` y reemplázalo con tu código.
El objeto final del chunk debe ser un `plotly` (no un ggplot estático):

```r
# ── PEGAR AQUÍ el gráfico principal de esta pestaña ─────────
p <- ggplot(dat, aes(x = anio, y = tasa, color = sexo_label)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  geom_vline(xintercept = 2021, linetype = "dotted", color = "#adb5bd") +
  scale_color_manual(values = c("Hombres" = COLOR_HOMBRES,
                                "Mujeres" = COLOR_MUJERES)) +
  labs(caption = CAPTION) +
  theme_minimal()

ggplotly(p)
```

Para tablas `gt`, el objeto se renderiza directamente (sin conversión):

```r
mi_tabla |> gt() |> tab_header(...) |> ...
```

### 6. Ajusta el título del panel (opcional)

Cada chunk de visualización tiene `#| title:`. Ese texto aparece como
encabezado del panel en el dashboard:

```r
#| title: "Tasa de ocupación por sexo — Colombia 2019–2025"
```

### 7. Agrega o elimina pestañas según necesites

Para **agregar** una pestaña, copia el bloque completo de una pestaña
existente y pégalo al final, cambiando el nombre del encabezado `#`.

Para **eliminar** una pestaña, borra todo el bloque desde el `#` del
título hasta antes del siguiente `#`.

Para **cambiar el ancho** de las columnas dentro de una fila:

```markdown
### Gráfico principal {width="70%"}
### Gráfico secundario {width="30%"}
```

Los anchos deben sumar 100%.

Para usar **solo una columna** en toda la fila:

```markdown
### Título {width="100%"}
```

---

## Reglas de estilo que siempre aplican

| Regla | Valor |
|-------|-------|
| Color hombres | `#2166AC` |
| Color mujeres | `#D6604D` |
| Caption de fuente | `CAPTION` (ya definida en setup) |
| Línea de quiebre 2021 | `geom_vline(xintercept = 2021, linetype = "dotted", color = "#adb5bd")` |
| Tipo de gráfico final | Siempre `plotly` (interactivo) |
| Rutas de datos | Siempre desde raíz: `datos/` |
| Factor de expansión | Siempre aplicar `fex` |

---

## Cómo previsualizar tu trabajo

**Solo tu página:**
```bash
quarto preview p2_calidad.qmd
```

**Todo el sitio con navegación:**
```bash
quarto preview
```

**Renderizar sin abrir el navegador:**
```bash
quarto render p2_calidad.qmd
```

---

## Problemas frecuentes

**"El gráfico no aparece"**
→ Verificar que el chunk termina con el objeto plotly (no asignado a
  una variable sin llamarla). El último objeto evaluado es lo que
  renderiza Quarto.

**"Error: archivo no encontrado"**
→ Verificar que la ruta al parquet sea `datos/` y no `../datos/`.
  Los dashboards están en la raíz, no en `analisis/`.

**"Los colores no coinciden"**
→ Usar `COLOR_HOMBRES` y `COLOR_MUJERES` definidos en setup, no
  escribir los hex directamente en cada gráfico.

**"La pestaña no aparece en el dashboard"**
→ El título de pestaña debe ser un `#` de nivel 1 seguido de
  `{.tabset}`. Ejemplo: `# Mi Pestaña {.tabset}`

**"El value box muestra '–'"**
→ Reemplazar el `value = "–"` por la variable calculada en setup.
  El chunk de setup debe correr antes que los value boxes.
