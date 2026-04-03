# Cómo reproducir el pipeline de datos

Este directorio contiene los scripts necesarios para descargar y armonizar los microdatos GEIH del DANE, y reproducir los parquets que usa el dashboard.

---

## Requisitos

- Python 3.8+ con `requests` y `beautifulsoup4`:
  ```bash
  pip install requests beautifulsoup4
  ```
- Espacio en disco: ~50 GB para los microdatos crudos (ZIP + CSV descomprimidos).

---

## Paso 1 — Descargar los microdatos del DANE

```bash
# Descargar todos los años (2019–2025)
python descarga_geih.py

# Solo años específicos
python descarga_geih.py --anios 2023 2024 2025
```

Los archivos se descargan desde el portal público del DANE:
[microdatos.dane.gov.co](https://microdatos.dane.gov.co)

Destino por defecto: `../datos_crudos/{anio}/`

---

## Paso 2 — Armonizar y generar los parquets

> Los scripts de armonización (`armonizar.py` y `armonizar_pet.py`) se agregarán al repo cuando estén listos.

La armonización:
1. Lee los módulos CSV de cada año (Ocupados, Características generales, Fuerza de trabajo)
2. Unifica nombres de variables entre Marco 2005 (2019–2020) y Marco 2018 (2021–2025)
3. Genera dos parquets consolidados

Salida esperada:
```
datos/
├── geih_ocupados_2019_2025.parquet   # 48 MB — Panel de ocupados (Páginas 2, 3 y O-B)
└── geih_pet_2019_2025.parquet        # 46 MB — Panel PET completo (Página 1)
```

---

## Alternativa rápida — Zenodo

Si no quieres reproducir el pipeline completo, descarga los parquets directamente desde Zenodo:

**DOI: pendiente de publicación**

Coloca los archivos en `datos/` y ejecuta directamente `quarto render dashboard.qmd`.

---

## Notas metodológicas

- Los microdatos crudos del DANE no se incluyen en este repositorio (tamaño y política de redistribución).
- Los parquets en Zenodo son datos *procesados y armonizados* derivados de fuentes públicas del DANE.
- Para más detalles sobre la armonización de variables ver `docs/guia_metodologica_dashboard.qmd`.
