"""
extraer_geih.py — Extractor unificado GEIH 2019-2025+ | EQUILAB
================================================================
Un solo script para todos los años. Entiende las diferencias entre
Marco 2005 (2019-2020) y Marco 2018 (2021-2025) sin necesitar scripts
separados ni parches.

Uso:
    python extraer_geih.py 2025                     # un año
    python extraer_geih.py 2024 2025                # varios años
    python extraer_geih.py --todos                  # 2019-2025

Fuente : procesamiento/datos_crudos/YYYY/ZipOriginal.zip
Salida : procesamiento/extraidos/YYYY/Modulo.csv

Diferencias entre marcos que este script resuelve:

  Marco 2005 — 2019
    • Cada mes tiene 3 CSVs por módulo: Cabecera / Resto / Área
    • Los tres se normalizan al mismo nombre y se concatenan
    • Formato ZIP: Mes.csv.zip

  Marco 2005 — 2020
    • Sin prefijos de área. Módulos con typos en el nombre (Descupados, Inativos…)
    • Separador coma (no punto y coma)
    • Formato ZIP: N.Mes.zip  →  N. Mes/CSV/Modulo.CSV interno

  Marco 2018 — 2021
    • 2 ZIPs semestrales (no 12 mensuales)
    • El número de mes se detecta del path interno, no del nombre del ZIP

  Marco 2018 — 2022-2025
    • Un ZIP mensual con CSVs directos O con CSV.zip anidado (se auto-detecta)
    • Nombres de ZIP varían cada año (incluyendo typos de DANE)
    • 2024: módulo "No ocupado" / "No ocupados" → unificado a "No_ocupados"
"""

import sys
import argparse
import zipfile
import csv
import io
import re
import unicodedata
from pathlib import Path

# =============================================================================
# RUTAS
# Script vive en: procesamiento/
# ZIPs están en:  procesamiento/datos_crudos/YYYY/
# Salida va a:    procesamiento/extraidos/YYYY/
# =============================================================================
_SCRIPT_DIR   = Path(__file__).parent.resolve()
DIR_DESCARGA  = _SCRIPT_DIR / "datos_crudos"
DIR_EXTRAIDOS = _SCRIPT_DIR / "extraidos"
SEP_OUT       = ";"

MESES_NOMBRE = {
    1:"enero", 2:"febrero", 3:"marzo",  4:"abril",
    5:"mayo",  6:"junio",   7:"julio",  8:"agosto",
    9:"septiembre", 10:"octubre", 11:"noviembre", 12:"diciembre",
}

# =============================================================================
# CONFIGURACIÓN POR AÑO
# Cada entrada documenta exactamente los nombres de ZIP que DANE publicó
# (typos incluidos) y las particularidades de ese año.
# =============================================================================

# Prefijo de área — Marco 2005 2019
_PREFIJO_AREA = re.compile(
    r"^(Cabecera|Resto|[╡Á]rea|Area)\s*[-–—]\s*",
    re.IGNORECASE,
)

# Keywords para detectar mes del path interno — Marco 2018 2021 semestral
_MES_KW = {
    "enero":1,  "ene":1,  "febrero":2, "feb":2, "marzo":3,  "mar":3,
    "abril":4,  "abr":4,  "mayo":5,              "junio":6,
    "julio":7,  "jul":7,  "agosto":8,  "ago":8,
    "septiembre":9, "sep":9, "octubre":10, "oct":10,
    "noviembre":11, "nov":11, "diciembre":12, "dic":12,
}

YEAR_CONFIG = {
    # ── Marco 2005 ────────────────────────────────────────────────────────────
    2019: {
        "marco": 2005,
        "extractor": "areas",       # Cabecera/Resto/Área × módulo → concatenar
        "zips": [
            ("Enero.csv.zip",1),    ("Febrero.csv.zip",2),  ("Marzo.csv.zip",3),
            ("Abril.csv.zip",4),    ("Mayo.csv.zip",5),     ("Junio.csv.zip",6),
            ("Julio.csv.zip",7),    ("Agosto.csv.zip",8),   ("Septiembre.csv.zip",9),
            ("Octubre.csv.zip",10), ("Noviembre.csv.zip",11),("Diciembre.csv.zip",12),
        ],
        "aliases": {},
    },
    2020: {
        "marco": 2005,
        "extractor": "standard",    # sep=coma, dirs N.Mes/CSV/, typos en módulos
        "zips": [
            ("1.Enero.zip",1),      ("2.Febrero.zip",2),    ("3.Marzo.zip",3),
            ("4.Abril.zip",4),      ("5.Mayo.zip",5),       ("6.Junio.zip",6),
            ("7.Julio.zip",7),      ("8.Agosto.zip",8),     ("9.Septiembre.zip",9),
            ("10.Octubre.zip",10),  ("11.Noviembre.zip",11),("12.Diciembre.zip",12),
        ],
        "aliases": {
            "Descupados":  "Desocupados",
            "Desoucpados": "Desocupados",
            "Inativos":    "Inactivos",
            "Ianctivos":   "Inactivos",
            "Inactivas":   "Inactivos",
            "Fuerzo_de_trabajo": "Fuerza_de_trabajo",
        },
    },
    # ── Marco 2018 ────────────────────────────────────────────────────────────
    2021: {
        "marco": 2018,
        "extractor": "semestral",   # 2 ZIPs con los 12 meses; mes del path interno
        "zips": [                   # lista de strings (no tuplas con mes)
            "GEIH - Marco-2018(I.Semestre).zip",
            "GEIH_Marco_2018(II. semestre).zip",
        ],
        "aliases": {},
    },
    2022: {
        "marco": 2018,
        "extractor": "standard",
        "zips": [
            ("GEIH_Enero_2022_Marco_2018.zip",1),
            ("GEIH_Febrero_2022_Marco_2018.zip",2),
            ("GEIH_Marzo_2022_Marco_2018.zip",3),
            ("GEIH_Abril_2022_Marco_2018_Act.zip",4),   # sufijo _Act
            ("GEIH_Mayo_2022_Marco_2018.zip",5),
            ("GEIH_Junio_2022_Marco_2018.zip",6),
            ("GEIH_Julio_2022_Marco_2018.zip",7),
            ("GEIH_Agosto_2022_Marco_2018.zip",8),
            ("GEIH_Septiembre_Marco_2018.zip",9),        # sin año
            ("GEIH_Octubre_Marco_2018.zip",10),          # sin año
            ("GEIH_Noviembre_2022_Marco_2018.act.zip",11), # extensión .act.zip
            ("GEIH_Diciembre_2022_Marco_2018.zip",12),
        ],
        "aliases": {},
    },
    2023: {
        "marco": 2018,
        "extractor": "standard",
        "zips": [
            ("Enero.zip",1),    ("Febrero.zip",2),   ("Marzo.zip",3),
            ("Abril.zip",4),    ("Mayo.zip",5),       ("Junio.zip",6),
            ("Julio.zip",7),    ("Agosto.zip",8),     ("Septiembre.zip",9),
            ("Octubre.zip",10), ("Noviembre.zip",11), ("Diciembre.zip",12),
        ],
        "aliases": {},
    },
    2024: {
        "marco": 2018,
        "extractor": "standard",
        "zips": [
            ("Ene_2024.zip",1),        ("Febrero_2024.zip",2),
            ("Marzo 2024.zip",3),      ("Abril 2024.zip",4),    # ANIDADOS (CSV.zip interno)
            ("Mayo_2024 1.zip",5),     ("Junio_2024.zip",6),    # espacio antes del 1
            ("Julio_2024.zip",7),      ("Agosto_2024.zip",8),
            ("Septiembre_2024.zip",9), ("Octubre_2024.zip",10),
            ("Noviembre_ 2024.zip",11),                          # espacio extra
            ("Diciembre_2024.zip",12),
        ],
        "aliases": {
            "No_ocupado": "No_ocupados",   # DANE usó singular en Marzo, plural en resto
        },
    },
    2025: {
        "marco": 2018,
        "extractor": "standard",
        "zips": [
            ("Enero 2025.zip",1),      ("Febrero 2025.zip",2),  ("Marzo 2025.zip",3),
            ("Abril 2025.zip",4),      ("Mayo 2025.zip",5),     ("Junio 2025.zip",6),
            ("Julio 2025.zip",7),      ("Agosto 2025.zip",8),   ("Septiembre 2025.zip",9),
            ("Octubre 2025.zip",10),   ("Noviembre 2025.zip",11),("Diciembre 2025.zip",12),
        ],
        "aliases": {},
    },
}

ANOS_DISPONIBLES = sorted(YEAR_CONFIG.keys())

# =============================================================================
# HELPERS COMPARTIDOS
# =============================================================================

def normalizar(nombre_entry, aliases=None, strip_area=False):
    """
    Nombre canónico de módulo a partir del path interno del ZIP:
    - Quita directorio padre
    - Quita prefijo de área si strip_area=True (Marco 2005 2019)
    - Elimina acentos
    - Espacios → guion bajo
    - Aplica alias de typos si se pasan
    """
    name = Path(nombre_entry).name
    if strip_area:
        name = _PREFIJO_AREA.sub("", name)
    stem = Path(name).stem
    if stem.lower().endswith(".csv"):          # doble extensión .CSV.CSV
        stem = Path(stem).stem
    stem = unicodedata.normalize("NFD", stem)
    stem = "".join(c for c in stem if unicodedata.category(c) != "Mn")
    stem = re.sub(r"\s+", "_", stem.strip())
    canon = re.sub(r"_+", "_", stem)
    return aliases.get(canon, canon) if aliases else canon


def detectar_sep(raw_bytes):
    """Detecta separador ; vs , a partir de la primera línea."""
    linea = raw_bytes.decode("latin-1", errors="replace")
    return ";" if linea.count(";") >= linea.count(",") else ","


def csvs_en_zf(zf):
    """Lista de entradas CSV en un ZipFile (excluye directorios)."""
    return [e for e in zf.infolist()
            if not e.filename.endswith("/")
            and Path(e.filename).suffix.lower() == ".csv"]


def encontrar_csv_zip(zf):
    """Devuelve la entrada CSV*.zip dentro de un ZIP externo, o None."""
    for e in zf.infolist():
        n = Path(e.filename).name.lower()
        if n.startswith("csv") and n.endswith(".zip") and not e.filename.endswith("/"):
            return e
    return None


def detectar_mes_de_path(path_str):
    """Extrae número de mes de un path interno del ZIP (para 2021 semestral)."""
    s = unicodedata.normalize("NFD", path_str.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    for kw, num in sorted(_MES_KW.items(), key=lambda x: -len(x[0])):
        if re.search(r"(?:^|[^a-z])" + kw, s):
            return num
    return None


def _leer_csv(zf, entry, aliases, strip_area):
    """
    Lee un CSV de un ZipFile abierto.
    Devuelve (canon, header_list, row_generator).
    """
    canon = normalizar(entry.filename, aliases=aliases, strip_area=strip_area)
    raw   = zf.read(entry.filename)
    primer_linea = raw.split(b"\n")[0] if b"\n" in raw else raw[:500]
    sep   = detectar_sep(primer_linea)
    content = raw.decode("latin-1", errors="replace")
    if content.startswith("\ufeff"):
        content = content[1:]
    reader = csv.reader(io.StringIO(content), delimiter=sep)
    header = next(reader, [])
    if header and header[0].startswith("\ufeff"):
        header[0] = header[0][1:]
    return canon, header, reader


# =============================================================================
# GENERADORES DE MÓDULOS POR TIPO DE EXTRACTOR
# Todos yielden: (canon, header, mes_nombre, row_gen)
# Excepto semestral que añade num_mes para el tracking de cobertura.
# =============================================================================

def _gen_standard(zip_path, num_mes, aliases):
    """
    Marco 2018 mensual (2022-2025) y Marco 2005 2020.
    Maneja CSVs directos en el ZIP O CSV.zip anidado.
    """
    mes_nombre = MESES_NOMBRE[num_mes]
    with zipfile.ZipFile(zip_path) as zf_outer:
        csvs = csvs_en_zf(zf_outer)
        if csvs:
            # Estructura directa
            for entry in csvs:
                canon, header, reader = _leer_csv(zf_outer, entry, aliases, False)
                yield canon, header, mes_nombre, reader
        else:
            # Estructura anidada: CSV.zip dentro del ZIP externo
            csv_entry = encontrar_csv_zip(zf_outer)
            if csv_entry is None:
                print(f"      ⚠️  Sin CSVs ni CSV.zip en {zip_path.name}")
                return
            inner_bytes = zf_outer.read(csv_entry.filename)
            with zipfile.ZipFile(io.BytesIO(inner_bytes)) as zf_inner:
                for entry in csvs_en_zf(zf_inner):
                    canon, header, reader = _leer_csv(zf_inner, entry, aliases, False)
                    yield canon, header, mes_nombre, reader


def _gen_areas(zip_path, num_mes, aliases):
    """
    Marco 2005 2019: cada módulo existe en 3 copias (Cabecera / Resto / Área).
    strip_area=True normaliza los tres al mismo nombre → se concatenan.
    """
    mes_nombre = MESES_NOMBRE[num_mes]
    with zipfile.ZipFile(zip_path) as zf:
        for entry in csvs_en_zf(zf):
            canon, header, reader = _leer_csv(zf, entry, aliases, strip_area=True)
            yield canon, header, mes_nombre, reader


def _gen_semestral(zip_path, aliases):
    """
    Marco 2018 2021: 2 ZIPs con los 12 meses.
    Mes detectado del path interno del directorio.
    Yields (canon, header, mes_nombre, num_mes, row_gen).
    """
    with zipfile.ZipFile(zip_path) as zf:
        for entry in csvs_en_zf(zf):
            parent_path = str(Path(entry.filename).parent)
            num_mes = detectar_mes_de_path(parent_path)
            if num_mes is None:
                print(f"      ⚠️  No se pudo detectar mes en: {entry.filename}")
                continue
            mes_nombre = MESES_NOMBRE[num_mes]
            canon, header, reader = _leer_csv(zf, entry, aliases, False)
            yield canon, header, mes_nombre, num_mes, reader


# =============================================================================
# ALGORITMO DOS-PASOS (COMPARTIDO POR TODOS LOS EXTRACTORES)
# Paso 1: leer solo la primera línea de cada CSV para determinar headers
# Paso 2: streamear datos y escribir a disco
# =============================================================================

def _dos_pasos(modulos_gen_fn, zips_list, dir_out, aliases, es_semestral=False):
    """
    Ejecuta el algoritmo de extracción dos-pasos sobre una lista de ZIPs.

    modulos_gen_fn: función que acepta (zip_path, num_mes, aliases) y yielde módulos.
                   Si es_semestral=True, acepta (zip_path, aliases) y yielde con num_mes.
    zips_list     : lista de (nombre_zip, num_mes) o lista de strings (semestral).
    dir_out       : Path de salida.
    """
    salida_files   = {}   # canon → file handle
    salida_writers = {}   # canon → csv.writer
    salida_ncols   = {}   # canon → int
    headers_canon  = {}   # canon → header list
    meses_por_mod  = {}   # canon → set de num_mes escritos

    # ── PASO 1: detectar headers ──────────────────────────────────────────────
    print("Paso 1: detectando módulos y headers...")

    if es_semestral:
        # Para semestral iteramos cada ZIP de semestre
        for zip_name in zips_list:
            with zipfile.ZipFile(zip_name) as zf:
                for entry in csvs_en_zf(zf):
                    canon = normalizar(entry.filename, aliases=aliases)
                    if canon not in headers_canon:
                        with zf.open(entry.filename) as f:
                            raw_line = f.readline()
                        sep    = detectar_sep(raw_line)
                        header = list(csv.reader([raw_line.decode("latin-1","replace")],
                                                 delimiter=sep))[0]
                        if header and header[0].startswith("\ufeff"):
                            header[0] = header[0][1:]
                        if "MES" not in header:
                            header = header + ["MES"]
                        headers_canon[canon] = header
    else:
        for zip_path, num_mes in zips_list:
            with zipfile.ZipFile(zip_path) as zf_outer:
                csvs = csvs_en_zf(zf_outer)
                entries_to_scan = []
                inner_zf = None
                if csvs:
                    entries_to_scan = csvs
                    active_zf = zf_outer
                else:
                    csv_entry = encontrar_csv_zip(zf_outer)
                    if csv_entry is None:
                        continue
                    inner_bytes = zf_outer.read(csv_entry.filename)
                    inner_zf    = zipfile.ZipFile(io.BytesIO(inner_bytes))
                    entries_to_scan = csvs_en_zf(inner_zf)
                    active_zf = inner_zf

                for entry in entries_to_scan:
                    canon = normalizar(entry.filename, aliases=aliases, strip_area=True)
                    if canon not in headers_canon:
                        with active_zf.open(entry.filename) as f:
                            raw_line = f.readline()
                        sep    = detectar_sep(raw_line)
                        header = list(csv.reader([raw_line.decode("latin-1","replace")],
                                                 delimiter=sep))[0]
                        if header and header[0].startswith("\ufeff"):
                            header[0] = header[0][1:]
                        if "MES" not in header:
                            header = header + ["MES"]
                        headers_canon[canon] = header

                if inner_zf:
                    inner_zf.close()

    print(f"   Módulos detectados: {sorted(headers_canon.keys())}\n")

    # ── Abrir archivos de salida ──────────────────────────────────────────────
    for canon, header in sorted(headers_canon.items()):
        ruta = dir_out / f"{canon}.csv"
        fh   = open(ruta, "w", encoding="latin-1", newline="")
        w    = csv.writer(fh, delimiter=SEP_OUT)
        w.writerow(header)
        salida_files[canon]   = fh
        salida_writers[canon] = w
        salida_ncols[canon]   = len(header)
        meses_por_mod[canon]  = set()

    def _escribir_modulo(canon, header_csv, mes_nombre, row_gen):
        """Escribe todas las filas de un módulo al archivo canónico."""
        if canon not in salida_writers:
            # Módulo nuevo no visto en paso 1 (caso borde)
            ruta = dir_out / f"{canon}.csv"
            hdr  = header_csv + (["MES"] if "MES" not in header_csv else [])
            fh   = open(ruta, "w", encoding="latin-1", newline="")
            w    = csv.writer(fh, delimiter=SEP_OUT)
            w.writerow(hdr)
            salida_files[canon]   = fh
            salida_writers[canon] = w
            salida_ncols[canon]   = len(hdr)
            headers_canon[canon]  = hdr
            meses_por_mod[canon]  = set()

        w             = salida_writers[canon]
        ncols         = salida_ncols[canon]
        can_header    = headers_canon[canon]
        can_pos       = {col: i for i, col in enumerate(can_header)}
        src_to_dst    = {i: can_pos[col]
                         for i, col in enumerate(header_csv)
                         if col in can_pos}
        dst_mes_idx   = can_pos.get("MES")
        n_filas       = 0

        for row in row_gen:
            new_row = [""] * ncols
            for src_i, dst_i in src_to_dst.items():
                if src_i < len(row):
                    new_row[dst_i] = row[src_i]
            if dst_mes_idx is not None:
                new_row[dst_mes_idx] = mes_nombre
            else:
                new_row.append(mes_nombre)
            w.writerow(new_row)
            n_filas += 1

        return n_filas

    # ── PASO 2: streamear datos ───────────────────────────────────────────────
    print("Paso 2: extrayendo y escribiendo datos...")

    if es_semestral:
        for zip_path in zips_list:
            print(f"  {zip_path.name}")
            n_zip = 0
            for canon, header_csv, mes_nombre, num_mes, row_gen in modulos_gen_fn(zip_path, aliases):
                n = _escribir_modulo(canon, header_csv, mes_nombre, row_gen)
                meses_por_mod.setdefault(canon, set()).add(num_mes)
                n_zip += n
                print(f"    [{num_mes:>2}:{mes_nombre:<11}] {canon:<42} {n:,} filas")
            print(f"    → ZIP subtotal: {n_zip:,} filas")
    else:
        for zip_path, num_mes in zips_list:
            mes_nombre = MESES_NOMBRE[num_mes]
            print(f"  [{num_mes:>2}] {zip_path.name:<45} → {mes_nombre}", end="", flush=True)
            n_mes    = 0
            mods_mes = set()
            for canon, header_csv, mes_n, row_gen in modulos_gen_fn(zip_path, num_mes, aliases):
                n = _escribir_modulo(canon, header_csv, mes_n, row_gen)
                meses_por_mod.setdefault(canon, set()).add(num_mes)
                n_mes += n
                mods_mes.add(canon)
            print(f"  ({n_mes:,} filas, {len(mods_mes)} módulos)")

    # ── Cerrar y resumen ──────────────────────────────────────────────────────
    for fh in salida_files.values():
        fh.close()

    print(f"\n{'─'*68}")
    print(f"{'Módulo':<50} {'MB':>5}  {'Filas':>9}  {'Meses'}")
    print(f"{'─'*68}")
    for canon in sorted(salida_files.keys()):
        ruta = dir_out / f"{canon}.csv"
        mb   = ruta.stat().st_size / 1e6
        with open(ruta, "rb") as f:
            n = sum(1 for _ in f) - 1
        meses_ok = sorted(meses_por_mod.get(canon, set()))
        n_meses  = len(meses_ok)
        flag     = "✅" if n_meses == 12 else f"⚠️  {n_meses}/12"
        print(f"  {canon:<48} {mb:>5.1f}  {n:>9,}  {flag}")


# =============================================================================
# FUNCIÓN PRINCIPAL POR AÑO
# =============================================================================

def extraer_anio(anio, dir_descarga=None, dir_extraidos=None):
    """
    Extrae todos los módulos de un año y los escribe en extraidos/YYYY/.
    Usa la configuración de YEAR_CONFIG para manejar las diferencias entre años.
    """
    if anio not in YEAR_CONFIG:
        print(f"❌ Año {anio} no configurado. Años disponibles: {ANOS_DISPONIBLES}")
        print("   Para añadir un año nuevo, agrega su entrada en YEAR_CONFIG.")
        return False

    config       = YEAR_CONFIG[anio]
    marco        = config["marco"]
    extractor    = config["extractor"]
    aliases      = config.get("aliases", {})
    dir_anio     = (dir_descarga or DIR_DESCARGA) / str(anio)
    dir_out      = (dir_extraidos or DIR_EXTRAIDOS) / str(anio)

    print("=" * 68)
    print(f"GEIH {anio}  |  Marco {marco}  |  Extractor: {extractor}")
    print(f"Fuente : {dir_anio}")
    print(f"Salida : {dir_out}")
    print("=" * 68)

    dir_out.mkdir(parents=True, exist_ok=True)

    # ── SEMESTRAL (2021) ──────────────────────────────────────────────────────
    if extractor == "semestral":
        zips_raw = config["zips"]
        disponibles = [dir_anio / z for z in zips_raw if (dir_anio / z).exists()]
        faltantes   = [z for z in zips_raw if not (dir_anio / z).exists()]
        if faltantes:
            print(f"\n⚠️  ZIPs no encontrados: {faltantes}")
        print(f"\nZIPs semestrales a procesar: {len(disponibles)}/{len(zips_raw)}\n")
        if not disponibles:
            print("❌ Sin ZIPs. Verifica que 01_descarga/2021/ existe y tiene los archivos.")
            return False
        _dos_pasos(_gen_semestral, disponibles, dir_out, aliases, es_semestral=True)

    # ── ESTÁNDAR (2019 areas, 2020, 2022-2025) ────────────────────────────────
    else:
        zips_raw    = config["zips"]
        disponibles = [(dir_anio / n, m) for n, m in zips_raw if (dir_anio / n).exists()]
        faltantes   = [n for n, m in zips_raw if not (dir_anio / n).exists()]
        if faltantes:
            print(f"\n⚠️  ZIPs no encontrados ({len(faltantes)}):")
            for n in faltantes:
                print(f"   {n}")
        print(f"\nZIPs a procesar: {len(disponibles)}/{len(zips_raw)}\n")
        if not disponibles:
            print(f"❌ Sin ZIPs. Verifica que 01_descarga/{anio}/ existe y tiene los archivos.")
            return False

        gen_fn = _gen_areas if extractor == "areas" else _gen_standard
        _dos_pasos(gen_fn, disponibles, dir_out, aliases, es_semestral=False)

    print(f"\n✅ Extracción {anio} completada → {dir_out}\n")
    return True


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Extractor GEIH — EQUILAB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Ejemplos:
  python extraer_geih.py 2025
  python extraer_geih.py 2024 2025
  python extraer_geih.py --todos

Años configurados: {ANOS_DISPONIBLES}

Para añadir un año nuevo (ej. 2026):
  1. Agrega una entrada en YEAR_CONFIG al inicio del script
  2. Usa extractor="standard" si es Marco 2018 mensual (caso normal desde 2022)
  3. Copia exactamente los nombres de ZIP desde la carpeta 01_descarga/2026/
""",
    )
    parser.add_argument(
        "anos",
        nargs="*",
        type=int,
        help="Año(s) a extraer (ej: 2024 2025)",
    )
    parser.add_argument(
        "--todos",
        action="store_true",
        help=f"Extrae todos los años configurados ({ANOS_DISPONIBLES[0]}–{ANOS_DISPONIBLES[-1]})",
    )
    args = parser.parse_args()

    if args.todos:
        anos = ANOS_DISPONIBLES
    elif args.anos:
        anos = sorted(set(args.anos))
    else:
        parser.print_help()
        sys.exit(0)

    print(f"\nPipeline GEIH | EQUILAB — años a procesar: {anos}\n")
    print(f"Fuente ZIPs : {DIR_DESCARGA}")
    print(f"Salida CSVs : {DIR_EXTRAIDOS}\n")

    resultados = {}
    for anio in anos:
        ok = extraer_anio(anio)
        resultados[anio] = "✅" if ok else "❌"

    if len(anos) > 1:
        print("\n" + "=" * 40)
        print("RESUMEN FINAL")
        print("=" * 40)
        for anio, estado in resultados.items():
            print(f"  {anio}: {estado}")


if __name__ == "__main__":
    main()
