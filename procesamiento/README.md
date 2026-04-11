# Pipeline de datos — Reproducibilidad completa

Este directorio contiene los scripts para reproducir los parquets que usa el dashboard, partiendo desde cero con los microdatos del DANE.

**Si solo quieres correr el dashboard**, no necesitas nada de esta carpeta. Descarga los parquets desde Zenodo (ver `datos/README.md`) y ponlos en `datos/`.

---

## Visión general

El pipeline tiene dos fases: creación de parquets base (01-05, Python) y pre-cálculo de modelos (06-11, R). Los pasos 01-05 son **opcionales** para quien clone el repositorio: los parquets ya procesados están disponibles en Zenodo.

```
┌─────────────────────────────────────────────────────────────────┐
│  FASE 1: Parquets base (Python)                                 │
│                                                                 │
│  01_descarga_geih.py     Descarga ZIPs del portal DANE          │
│          ↓               datos_crudos/{anio}/*.zip              │
│  02_extraer_geih.py      Extrae CSVs de los ZIPs               │
│          ↓               extraidos/{anio}/*.csv                 │
│  03_crear_parquet_ocupados.py   Armoniza → parquet ocupados     │
│  04_crear_parquet_pet.py        Armoniza → parquet PET          │
│  05_crear_parquet_genero.py     Extrae identidad de género      │
│          ↓                                                      │
│  datos/geih_ocupados_2019_2025.parquet  (Páginas 2, 3 y O-B)   │
│  datos/geih_pet_2019_2025.parquet       (Página 1: TGP/TO/TD)  │
│  datos/geih_genero_2022_2025.parquet    (Análisis género diverso)│
│                                                                 │
│  FASE 2: Pre-cálculo de modelos (R)                             │
│                                                                 │
│  06_precalcular_p1.R            → Indicadores P1                │
│  07_precalcular_p1_boy.R        → BOY participación (P1)        │
│  08_precalcular_p2.R            → Indicadores P2                │
│  09_precalcular_p3.R            → Indicadores P3                │
│  10_precalcular_p3_ob_nopo.R    → O-B + Ñopo (P3)              │
│  11_precalcular_p4.R            → Diversidad de género (P4)     │
└─────────────────────────────────────────────────────────────────┘
```

Las carpetas intermedias (`datos_crudos/`, `extraidos/`, `armonizados/`) están en el `.gitignore` porque son temporales y pesadas (~50 GB en total).

---

## Requisitos

Python 3.8+ con las siguientes dependencias:

```bash
pip install requests beautifulsoup4 pandas pyarrow
```

---

## Paso 1 — Descargar microdatos del DANE

```bash
python 01_descarga_geih.py                        # todos los años (2019–2025)
python 01_descarga_geih.py --anios 2024 2025       # solo algunos años
python 01_descarga_geih.py --destino "./datos_crudos"  # destino por defecto
```

Descarga los ZIPs mensuales desde [microdatos.dane.gov.co](https://microdatos.dane.gov.co). Destino: `datos_crudos/{anio}/`. Requiere ~50 GB de espacio libre.

---

## Paso 2 — Extraer CSVs de los ZIPs

```bash
python 02_extraer_geih.py --todos                  # todos los años
python 02_extraer_geih.py 2024 2025                 # solo algunos años
```

Extrae los módulos CSV de cada ZIP mensual. Maneja automáticamente las diferencias entre marcos muestrales (2005 vs 2018), ZIPs anidados, typos del DANE y el formato semestral de 2021.

Salida: `extraidos/{anio}/Ocupados.csv`, `Caracteristicas_generales.csv`, `Fuerza_de_trabajo.csv`, etc.

---

## Paso 3 — Crear parquet de ocupados

```bash
python 03_crear_parquet_ocupados.py
```

Lee los CSVs extraídos, une los módulos Ocupados + Características generales + Fuerza de trabajo, crea variables armonizadas entre marcos, y genera el parquet panel.

Incluye la corrección del swap de códigos 7↔8 en `posicion_ocup` para los años 2019–2020 (Marco 2005), integrada directamente en el proceso de armonización.

Salida: `datos/geih_ocupados_2019_2025.parquet` (~48 MB, 2.6M filas).

---

## Paso 4 — Crear parquet PET

```bash
python 04_crear_parquet_pet.py
```

Lee Fuerza de trabajo + Características generales, une, armoniza y filtra a PET (≥15 años). Genera el parquet para calcular TGP, TO y TD.

Salida: `datos/geih_pet_2019_2025.parquet` (~46 MB, 4.9M filas).

---

## Paso 5 — Crear parquet de identidad de género

```bash
python 05_crear_parquet_genero.py
```

Extrae las variables de identidad de género (P3039) y orientación sexual (P3038) del módulo Características generales para 2022-2025 (Marco 2018). Estas variables no existen en Marco 2005 (2019-2020), por lo que se guardan en un parquet complementario en vez de integrarse al principal.

Genera una variable derivada `genero_diverso` (0=cisgénero, 1=diverso) y conserva las llaves de cruce para hacer join con los parquets de ocupados y PET.

Salida: `datos/geih_genero_2022_2025.parquet` (~3.3M filas, 2022-2025).

---

## Alternativa rápida — Zenodo

Si no quieres reproducir el pipeline, descarga todos los parquets (bases + precalculados) directamente desde Zenodo:

**DOI: [10.5281/zenodo.19504291](https://doi.org/10.5281/zenodo.19504291)**

Coloca los 25 archivos en `datos/` y listo. Ver `datos/README.md` para el inventario completo.

---

## Notas

- Los microdatos crudos del DANE no se redistribuyen. Los parquets en Zenodo son datos procesados derivados de fuentes públicas.
- Para detalles sobre la armonización de variables, ver `docs/guia_metodologica_dashboard.qmd`.
- La corrección del swap `posicion_ocup` 7↔8 (Problema 1 en `reporte_problemas_datos.txt`) está integrada en el paso 3. No requiere parches adicionales.
