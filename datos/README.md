# Datos — instrucciones para obtener los parquets

Los archivos de datos **no están en este repositorio** porque tienen un tamaño de ~94 MB combinados y son datos derivados de fuentes públicas del DANE.

---

## Descarga rápida — Zenodo

Los parquets procesados y armonizados están disponibles con DOI permanente en Zenodo:

**DOI: pendiente de publicación**

Descarga los dos archivos y colócalos en esta carpeta (`datos/`):

| Archivo | Tamaño | Descripción |
|---------|--------|-------------|
| `geih_ocupados_2019_2025.parquet` | 48 MB | Panel de ocupados 2019–2025. Páginas 2, 3 y O-B. |
| `geih_pet_2019_2025.parquet` | 46 MB | Panel PET completo 2019–2025. Página 1 (TGP/TO/TD). |

---

## Fuente original

**Gran Encuesta Integrada de Hogares (GEIH)** — DANE Colombia

- Portal: [microdatos.dane.gov.co](https://microdatos.dane.gov.co)
- Los datos son de acceso público y gratuito.
- Cobertura: 2019–2025 (12 meses por año, diseño muestral complejo).

---

## Reproducir los parquets desde cero

Si prefieres generar los parquets desde los microdatos originales, ver `procesamiento/README.md`.

---

## Variables principales

### `geih_ocupados_2019_2025.parquet` (25 columnas)

| Variable | Descripción |
|----------|-------------|
| `anio` | Año (2019–2025) |
| `mes` | Mes en español minúscula |
| `sexo` | 1=hombre, 2=mujer |
| `edad` | Edad en años |
| `fex` | Factor de expansión (ya unificado entre marcos) |
| `ing_total` | Ingreso laboral total INGLABO — usar para brechas |
| `ing_mes` | Salario mensual P6500 — cobertura del 50%, ver advertencias |
| `horas_sem` | Horas trabajadas la semana anterior |
| `posicion_ocup` | Posición ocupacional (P6430) |
| `niv_edu_armon` | Nivel educativo armonizado entre marcos |
| `esc_anios` | Años de escolaridad (variable continua) |
| `rama2d_orig` | Rama de actividad económica (CIIU 2 dígitos) |
| `etnia_bin` | 0=no se autoidentifica con grupo étnico, 1=sí |
| `clase_zona` | 1=cabecera municipal, 2=resto/rural |
| `marco` | Marco muestral: "2005" (2019–2020) o "2018" (2021–2025) |

### `geih_pet_2019_2025.parquet` (16 columnas)

Contiene toda la PET (≥15 años): ocupados, desocupados e inactivos.
Variable clave: `condicion_activ` — ver guía metodológica para los códigos.

---

*Datos procesados por EQUILAB a partir de microdatos públicos del DANE.*
