# Datos — instrucciones para obtener los parquets

Los archivos de datos **no estan en este repositorio** porque pesan ~151 MB combinados y son datos derivados de fuentes publicas del DANE.

---

## Descarga rapida — Zenodo

Todos los parquets (bases + precalculados) estan disponibles con DOI permanente en Zenodo:

**DOI: pendiente de publicacion**

Descarga los 25 archivos y colocalos en esta carpeta (`datos/`). Con ellos puedes renderizar el dashboard directamente con `quarto render` sin ejecutar ningun script de procesamiento.

### Archivos base (3 archivos, ~151 MB)

Paneles armonizados a partir de los microdatos de la GEIH. Necesarios si quieres reproducir los precalculos (scripts 06–11 en `procesamiento/`).

| Archivo | Tamano | Descripcion |
|---------|--------|-------------|
| `geih_ocupados_2019_2025.parquet` | 80 MB | Panel de ocupados 2019–2025. Paginas 2, 3 y 4. |
| `geih_pet_2019_2025.parquet` | 44 MB | Panel PET completo 2019–2025. Pagina 1 (TGP/TO/TD) y 4. |
| `geih_genero_2022_2025.parquet` | 27 MB | Variables de identidad de genero y orientacion sexual (solo marco 2018, 2022–2025). Pagina 4. |

### Archivos precalculados (22 archivos, <1 MB total)

Generados por los scripts R en `procesamiento/` (pasos 06–11). El dashboard lee directamente estos parquets.

| Archivo | Script | Pagina | Descripcion |
|---------|--------|--------|-------------|
| `tasas_p1_completo.parquet` | 06 | P1 | Tasas TGP/TO/TD por sexo, etnia, zona, educacion (2019–2025) |
| `tasas_p1_departamental.parquet` | 06 | P1 | Tasas por departamento (ultimo anio) |
| `tasas_p1_global.parquet` | 06 | P1 | Tasas globales por anio y sexo |
| `p1_boy_resultados.parquet` | 07 | P1 | Descomposicion BOY de participacion laboral |
| `p1_boy_yun.parquet` | 07 | P1 | Contribucion Yun por variable (ultimo anio) |
| `calidad_p2_posicion_hist.parquet` | 08 | P2 | Heatmap posicion ocupacional (historico) |
| `calidad_p2_posicion_fil.parquet` | 08 | P2 | Posicion ocupacional filtrable |
| `calidad_p2_horas_fil.parquet` | 08 | P2 | Horas trabajadas filtrable |
| `calidad_p2_informalidad_fil.parquet` | 08 | P2 | Informalidad filtrable |
| `calidad_p2_segregacion.parquet` | 08 | P2 | Segregacion sectorial (Duncan) |
| `calidad_p2_vboxes.parquet` | 08 | P2 | Value boxes resumen (1 fila) |
| `p3_boxplot_stats.parquet` | 09 | P3 | Cuartiles ingreso mensual por anio y sexo |
| `p3_boxplot_edu.parquet` | 09 | P3 | Cuartiles ingreso mensual por educacion y sexo |
| `p3_brecha_educacion.parquet` | 09 | P3 | Brecha salarial por nivel educativo (serie) |
| `p3_ob_resultados.parquet` | 10 | P3 | Descomposicion Oaxaca-Blinder por anio/zona/etnia |
| `p3_ob_detalle_vars.parquet` | 10 | P3 | Detalle O-B por variable (ultimo anio) |
| `p3_nopo_resultados.parquet` | 10 | P3 | Descomposicion Nopo matching por anio |
| `p4d_tasas_acceso.parquet` | 11 | P4 | Tasas de acceso por identidad de genero |
| `p4d_calidad.parquet` | 11 | P4 | Calidad del empleo por identidad de genero |
| `p4d_salarios.parquet` | 11 | P4 | Medianas salariales por identidad de genero |
| `p4d_panorama_poblacion.parquet` | 11 | P4 | Panorama poblacional por identidad |
| `p4d_posicion_ocupacional.parquet` | 11 | P4 | Posicion ocupacional mujeres cis vs trans |

---

## Fuente original

**Gran Encuesta Integrada de Hogares (GEIH)** — DANE Colombia

- Portal: [microdatos.dane.gov.co](https://microdatos.dane.gov.co)
- Los datos son de acceso publico y gratuito.
- Cobertura: 2019–2025 (12 meses por anio, diseno muestral complejo).

---

## Reproducir los parquets desde cero

Si prefieres generar los parquets desde los microdatos originales, ver `procesamiento/README.md`.

---

## Variables principales

### `geih_ocupados_2019_2025.parquet` (25 columnas)

| Variable | Descripcion |
|----------|-------------|
| `marco` | Marco muestral: "2005" (2019–2020) o "2018" (2021–2025) |
| `anio` | Anio (2019–2025) |
| `mes` | Mes en espanol minuscula |
| `dpto_cod` | Codigo de departamento |
| `clase_zona` | 1=cabecera municipal, 2=resto/rural |
| `fex` | Factor de expansion (ya unificado entre marcos) |
| `sexo` | 1=hombre, 2=mujer |
| `edad` | Edad en anios |
| `niv_edu_orig` | Nivel educativo original (codigos DANE, 1–11) |
| `niv_edu_armon` | Nivel educativo armonizado entre marcos (1–6) |
| `niv_edu_grado` | Grado dentro del nivel educativo |
| `esc_anios` | Anios de escolaridad (variable continua) |
| `ing_mes` | Salario mensual P6500 — cobertura ~57%, usado en modelos O-B/Nopo |
| `ing_total` | Ingreso laboral total INGLABO — cobertura ~98%, usado en descriptivas |
| `horas_sem` | Horas trabajadas la semana anterior |
| `posicion_ocup` | Posicion ocupacional (P6430) |
| `rama2d_orig` | Rama de actividad economica (CIIU 2 digitos) |
| `oficio_orig` | Oficio (codigo original DANE) |
| `condicion_activ` | Condicion de actividad: 1–2=ocupado, 3=desocupado, 4–6=inactivo |
| `oci` | Flag de ocupado segun modulo FT |
| `etnia_bin` | 0=no se autoidentifica con grupo etnico, 1=si |
| `DIRECTORIO` | Llave de vivienda (para joins con otros modulos) |
| `SECUENCIA_P` | Llave de hogar |
| `ORDEN` | Llave de persona dentro del hogar |
| `HOGAR` | Llave de hogar dentro de la vivienda |

### `geih_pet_2019_2025.parquet` (16 columnas)

Contiene toda la PET (>=15 anios): ocupados, desocupados e inactivos. Variables: `marco`, `anio`, `mes`, `dpto_cod`, `clase_zona`, `fex`, `sexo`, `edad`, `etnia_bin`, `niv_edu_armon`, `esc_anios`, `condicion_activ`, `DIRECTORIO`, `SECUENCIA_P`, `ORDEN`, `HOGAR`.

Variable clave: `condicion_activ` — ver guia metodologica para los codigos.

### `geih_genero_2022_2025.parquet` (14 columnas)

Variables de identidad de genero y orientacion sexual, solo disponibles en marco 2018 (2022–2025). Variables: `DIRECTORIO`, `SECUENCIA_P`, `ORDEN`, `HOGAR`, `mes`, `anio`, `dpto_cod`, `clase_zona`, `edad`, `fex`, `sexo_nacer`, `identidad_genero`, `orientacion_sex`, `genero_diverso`.

Se cruza con los parquets de ocupados y PET usando las llaves `DIRECTORIO`, `SECUENCIA_P`, `ORDEN`, `HOGAR`, `mes`, `anio`.

---

## Nota sobre variables de ingreso

- **`ing_total`** (INGLABO): Ingreso laboral total, cobertura ~98%. Se usa para estadisticas descriptivas (medianas, cuartiles) en Pagina 3.
- **`ing_mes`** (P6500): Salario mensual reportado, cobertura ~57%. Se usa para modelos econometricos (Oaxaca-Blinder, Nopo) en Pagina 3 y analisis salariales en Pagina 4.

La diferencia es intencional: los modelos econometricos requieren una variable de salario puro, mientras que las descriptivas usan el ingreso laboral total para mayor cobertura.

---

*Datos procesados por EQUILAB a partir de microdatos publicos del DANE.*
