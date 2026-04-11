# Guía del equipo — Radar de Brechas Laborales de Género

**EQUILAB · DAT4CCIÓN 2026**

Hola equipo 👋 Esta guía tiene todo lo que necesitan para arrancar. Léanla de arriba a abajo antes de abrir RStudio. No se salten pasos.

------------------------------------------------------------------------

## ¿Qué vamos a construir?

Un dashboard interactivo en tres páginas que muestra las brechas de género en el mercado laboral colombiano entre 2019 y 2025, usando datos oficiales del DANE.

Cada persona construye una página. Al final Alejandra las integra en el producto final.

| Persona | Archivo de trabajo | Tema |
|----|----|----|
| **Alejandra** | `analisis/p1_acceso.qmd` | Acceso al mercado laboral (TGP, TO, TD) |
| **Jeidy** | `analisis/p2_calidad.qmd` | Calidad del empleo y segregación sectorial |
| **Sofía** | `analisis/p3_salariales.qmd` | Brechas salariales |

------------------------------------------------------------------------

## Paso 1 — Configuración inicial (solo se hace una vez)

### 1.1 Clonar el repositorio

Abre RStudio. Ve a **File → New Project → Version Control → Git** y pega esta URL:

```         
https://github.com/YourFavouriteDataSuperstar/equilab-dat4ccion-2026.git
```

Elige una carpeta en tu computador donde guardar el proyecto y haz clic en **Create Project**.

Ahora tienes todos los archivos del equipo en tu computador y RStudio ya sabe que es un proyecto Git.

### 1.2 Instalar los paquetes de R

En la consola de RStudio (panel inferior izquierdo), copia y ejecuta esto:

``` r
install.packages(c(
  "arrow",    # leer los archivos de datos (.parquet)
  "dplyr",    # manipular datos (filtrar, agrupar, calcular)
  "tidyr",    # reorganizar tablas
  "ggplot2",  # hacer gráficas
  "plotly",   # hacer las gráficas interactivas
  "Hmisc",    # calcular medianas ponderadas
  "scales"    # formatear números en las gráficas
))
```

Esto puede tomar unos minutos. Es normal que aparezca mucho texto en rojo — mientras no diga `Error`, está bien.

### 1.3 Obtener los datos

Los archivos de datos son grandes y no están en GitHub. Alejandra te los compartirá. Son dos archivos `.parquet`:

-   `geih_ocupados_2019_2025.parquet` (48 MB)
-   `geih_pet_2019_2025.parquet` (46 MB)

Cuando los tengas, colócalos **exactamente aquí** dentro del proyecto:

```         
equilab-dat4ccion-2026/
└── datos/
    ├── geih_ocupados_2019_2025.parquet   ← aquí
    └── geih_pet_2019_2025.parquet        ← aquí
```

En RStudio puedes arrastrarlos directamente al panel **Files** (abajo a la derecha), dentro de la carpeta `datos`.

------------------------------------------------------------------------

## Paso 2 — Tu archivo de trabajo

Abre tu archivo desde el panel Files de RStudio:

-   Persona D → `analisis/p1_acceso.qmd`
-   Persona C → `analisis/p2_calidad.qmd`
-   Persona B → `analisis/p3_salariales.qmd`

El archivo ya tiene código base listo. Tu trabajo es **entenderlo, ejecutarlo, y mejorarlo**.

### Cómo renderizar (ver el resultado)

Haz clic en el botón **Render** (arriba del archivo, ícono de flecha azul). Esto genera un archivo `.html` que puedes abrir en el navegador y ver cómo se ve tu página.

Si hay un error, aparece en rojo en la consola. Cópialo y mándaselo a Alejandra.

### Cómo ejecutar el código por partes

No tienes que renderizar todo cada vez. Puedes correr cada bloque de código (llamado *chunk*) individualmente haciendo clic en el triángulo verde ▶ en la esquina superior derecha del chunk.

------------------------------------------------------------------------

## Paso 3 — Flujo de trabajo diario

Cada día que trabajen en el proyecto, sigan este orden:

### 3.1 Actualizar antes de empezar (Pull)

En RStudio, panel **Git** (arriba a la derecha), haz clic en **Pull** (flecha azul hacia abajo). Esto descarga los cambios que otros hayan subido.

> Siempre haz Pull antes de empezar a trabajar. Evita conflictos.

### 3.2 Trabaja en tu archivo

Edita tu `.qmd`, ejecuta los chunks, revisa los resultados, mejora las gráficas.

### 3.3 Guardar tus cambios (Commit + Push)

Cuando tengas algo que funcione — aunque sea una sola gráfica — guárdalo en GitHub:

**En el panel Git de RStudio:**

1.  Marca con ✅ los archivos que cambiaste (checkbox en la columna "Staged")
2.  Haz clic en **Commit**
3.  Escribe un mensaje que describa qué hiciste, por ejemplo:
    -   `"agrego gráfica de TGP por sexo"`
    -   `"corrijo filtro de edades"`
    -   `"agrego gráfica de informalidad"`
4.  Haz clic en **Commit** y luego en **Push** (flecha verde hacia arriba)

> No esperes a tener todo perfecto para hacer Push. Es mejor subir avances pequeños con frecuencia.

------------------------------------------------------------------------

## Reglas obligatorias — todos deben seguirlas

Estas reglas garantizan que las tres páginas se vean consistentes cuando Alejandra las integre.

### Colores — no cambiar

``` r
COLOR_HOMBRES <- "#2166AC"   # azul
COLOR_MUJERES <- "#D6604D"   # coral
```

Úsalos siempre con `scale_color_manual()` o `scale_fill_manual()`. Hombres siempre azul, mujeres siempre coral.

### Filtros — siempre aplicar antes de cualquier cálculo

**Si usas el parquet de ocupados (`geih_ocupados`):**

``` r
df_base <- df_ocup |>
  filter(
    condicion_activ == 1,      # solo personas que trabajaron
    edad >= 15, edad <= 99,    # edad válida
    sexo %in% c(1, 2)          # sexo válido
  )
```

**Si vas a analizar ingresos, agrega este filtro adicional:**

``` r
df_ingresos <- df_base |>
  filter(
    !is.na(ing_total),
    !ing_total %in% c(0, 98, 99)   # 98=no sabe, 99=no informa
  )
```

**Si usas el parquet PET (`geih_pet`):**

``` r
df_base <- df_pet |>
  filter(
    sexo %in% c(1, 2),
    edad >= 15, edad <= 99
  )
```

### Factor de expansión — siempre usar `fex`

Cada persona en los datos representa a miles de personas reales. El factor `fex` dice cuántas. Todos los cálculos de totales, porcentajes y medianas deben usar `fex` como peso.

``` r
# Porcentaje ponderado: así sí
group_by(sexo) |> summarise(n = sum(fex))

# Porcentaje sin ponderar: así NO
group_by(sexo) |> count()
```

### Línea de cambio de marco muestral — en todas las series de tiempo

En 2021 el DANE cambió la metodología de la encuesta. Todas las gráficas con el tiempo (2019–2025) deben mostrar esta línea:

``` r
geom_vline(xintercept = 2021, linetype = "dashed", color = "gray50") +
annotate("text", x = 2021.1, y = Inf, vjust = 1.5, hjust = 0,
         label = "Cambio Marco\nMuestral", size = 3, color = "gray40")
```

### Pie de figura — en todas las gráficas

``` r
caption = "Fuente: DANE, GEIH 2019–2025. Cálculos: EQUILAB — DAT4CCIÓN 2026."
```

------------------------------------------------------------------------

## Si algo no funciona

Antes de frustrarse, prueba esto en orden:

1.  **¿El error dice "could not find function"?** → El paquete no está instalado o no lo cargaste con `library()`.
2.  **¿El error dice "object not found"?** → Falta correr un chunk anterior. Corre los chunks de arriba hacia abajo en orden.
3.  **¿El archivo no renderiza?** → Copia el mensaje de error completo (el texto rojo) y mándaselo a Alejandra.
4.  **¿No entiendes qué hace una línea de código?** → Selecciónala y presiona **F1** en RStudio para ver la documentación. O pregúntale a Alejandra.

> **Regla de oro:** si llevas más de 20 minutos atascado en algo, pide ayuda. No pierdas tiempo.

------------------------------------------------------------------------

## Entregable del lunes 6 de abril

Para el lunes cada persona debe tener:

-   [ ] Su archivo `.qmd` renderizando sin errores
-   [ ] Al menos 3 gráficas funcionando con los datos reales
-   [ ] El archivo subido al repositorio (commit + push)

No tiene que estar perfecto — tiene que funcionar.

------------------------------------------------------------------------

## Recursos útiles

-   **Repositorio del proyecto:** [github.com/YourFavouriteDataSuperstar/equilab-dat4ccion-2026](https://github.com/YourFavouriteDataSuperstar/equilab-dat4ccion-2026)
-   **Guía metodológica completa:** `docs/guia_metodologica_dashboard.qmd` (en el repo)
-   **Dudas técnicas:** escríbele directamente a Alejandra

------------------------------------------------------------------------

*EQUILAB · DAT4CCIÓN 2026 · Universidad EAN*
