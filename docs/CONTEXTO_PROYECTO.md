# Contexto del Proyecto — EQUILAB DAT4CCION 2026

**Ultima actualizacion:** 2026-04-08

---

## Descripcion

Sitio web estatico (Quarto Website) que analiza las brechas laborales de genero en Colombia usando microdatos de la GEIH (DANE) 2019-2025. El proyecto se enfoca en las **mujeres** en el mercado laboral colombiano, incluyendo mujeres trans.

**Equipo:** Alejandra Otero (arquitectura, integracion, O-B), Pablo (p1), Jeidy (p2), Sofia (p3)
**Programa:** DAT4CCION 2026 — Universidad EAN

---

## Estructura del sitio

| Pagina | Archivo | Formato | Contenido |
|---|---|---|---|
| Inicio | `index.qmd` | closeread-html | Portada scrollytelling, cards de navegacion |
| Acceso Laboral | `p1_acceso.qmd` | dashboard | TGP, TO, TD por sexo, region, educacion, etnia |
| Calidad del Empleo | `p2_calidad.qmd` | dashboard | Informalidad, posicion ocupacional, horas, segregacion |
| Brechas Salariales | `p3_salariales.qmd` | dashboard | Ingreso, brecha ajustada, Oaxaca-Blinder |
| Mujeres Diversas | `p4_mujeres_diversas.qmd` | closeread-html | Brechas para mujeres cis vs trans (2022-2025) |
| Metodologia | `metodologia.qmd` | closeread-html | Fuentes, filtros, limitaciones |

---

## Paleta de colores vigente

```
--color-fondo:       #1a1a2e   (navy oscuro — navbar, footer)
--color-acento:      #004457   (teal oscuro)
--color-primario:    #0086ab   (teal/cyan — titulos, acentos)
--color-hombres:     #5f7300   (oliva/verde)
--color-mujeres:     #6b2d50   (morado/ciruela)
--color-neutro:      #6c757d   (gris)
--color-blanco:      #f4f4f4   (fondo claro)
--color-texto:       #212529   (texto principal)
```

**Tipografia:** Outfit (titulos), Public Sans (cuerpo)

**Colores de cards (pagina inicio):**
- Acceso: `#dde4bb` (lima)
- Calidad: `#e2c5d6` (rosa)
- Salariales: `#e6d1c6` (tan)
- Diversidades: `#d1e4e9` (cyan)

---

## Convenciones de codigo

- **Factor de expansion:** Siempre usar `fex` en todas las estimaciones
- **Filtros base ocupados:** `condicion_activ == 1, edad >= 15, edad <= 99, sexo %in% c(1, 2)`
- **Filtros base PET:** `sexo %in% c(1, 2), edad >= 15, edad <= 99`
- **Ingresos validos:** `!is.na(ing_total), !ing_total %in% c(0, 98, 99)`
- **Linea de cambio muestral:** Linea punteada vertical en 2021 en todas las series de tiempo
- **Caption estandar:** `"Fuente: DANE, GEIH 2019-2025. Calculos: EQUILAB -- DAT4CCION 2026."`
- **Proxy informalidad:** `posicion_ocup %in% c(4, 6, 7, 8)` (7+8 agrupados por swap entre marcos)

---

## Datos

### Archivos fuente (datos/)
- `geih_ocupados_2019_2025.parquet` — Ocupados 2019-2025 (48 MB)
- `geih_pet_2019_2025.parquet` — PET 15+ anos 2019-2025 (46 MB)
- `geih_genero_2022_2025.parquet` — Variables de genero/orientacion (solo marco 2018, 2022-2025)

### Archivos precalculados (datos/)
- `tasas_p1_*.parquet` — Indicadores de acceso laboral
- `calidad_p2_*.parquet` — Indicadores de calidad del empleo
- `p3_*.parquet` — Estadisticas salariales
- `p4_ob_*.parquet`, `p4_nopo_*.parquet`, `p4_boy_*.parquet` — Modelos de descomposicion salarial
- `p4d_*.parquet` — Indicadores de diversidad de genero

---

## Problemas conocidos

### CRITICO: Swap posicion_ocup codigos 7 y 8
Los codigos 7 (jornalero) y 8 (sin remuneracion) estan intercambiados entre Marco 2005 (2019-2020) y Marco 2018 (2021-2025). **Mitigacion:** se agrupan 7+8 siempre juntos. **Solucion definitiva pendiente:** corregir el swap en el script de construccion del parquet.

### INFORMATIVO: COVID-19 en 2020
Recoleccion exclusivamente telefonica marzo-julio 2020. Menor cobertura (~85%). Mantener datos pero interpretar con cautela.

### INFORMATIVO: Cambio de marco muestral 2021
Tasas y proporciones SI son comparables. Totales absolutos NO son comparables. Todas las graficas usan linea punteada en 2021.

### INFORMATIVO: Diversidad de genero — muestra pequena
Las variables de identidad de genero y orientacion sexual solo existen desde 2022 (marco 2018). La subpoblacion de mujeres trans tiene ~640 observaciones en 4 anos. Se usa CV (coeficiente de variacion) via paquete `survey` para determinar confiabilidad de estimaciones.

---

## Decisiones tecnicas

- **Quarto Website** (no Shiny) — entregable es HTML estatico, sin servidor
- **Closeread** para narrativa scrollytelling (portada, metodologia, p4)
- **Dashboard** con crosstalk para interactividad cliente-side (p1, p2, p3)
- **Datos precalculados** en parquet para rendering rapido (no se computa en tiempo de render)
- **survey::svydesign** para estimaciones con diseno muestral complejo y calculo de CV

---

## Historial de documentacion

Los documentos originales del equipo estan en `docs/historial/`:
- `GUIA_EQUIPO_v1.md` — Guia de onboarding del equipo (abril 2026)
- `GUIA_INTEGRACION_v1.md` — Guia tecnica de integracion de paginas
- `PLAN_MIGRACION_v1.md` — Plan de migracion de dashboard a website
