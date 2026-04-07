# =============================================================================
# 05_crear_parquet_genero.py — GEIH 2022-2025 | DAT4CCIÓN 2026 — EQUILAB
#
# Qué hace:
#   1. Lee Características_generales para años 2022-2025 (Marco 2018)
#   2. Extrae P3271 (sexo al nacer), P3039 (identidad de género),
#      P3038 (orientación sexual) y llaves de cruce
#   3. Armoniza y guarda parquet complementario en datos/
#
# Para qué sirve:
#   Análisis exploratorio de brechas laborales para personas de género
#   diverso (trans, no binarias). La variable P3039 solo existe en
#   Marco 2018 (2022+), por lo que NO se integra al parquet principal
#   de ocupados/PET que cubre 2019-2025.
#
#   Este parquet se cruza con los parquets principales usando las llaves:
#   DIRECTORIO + SECUENCIA_P + ORDEN + HOGAR + mes + anio
#
# Variables resultado:
#   sexo_nacer      : 1=Hombre, 2=Mujer (P3271)
#   identidad_genero: 1=Hombre, 2=Mujer, 3=Hombre trans, 4=Mujer trans,
#                     5=Otro (P3039 — Diccionario DANE)
#   orientacion_sex : 1=Heterosexual, 2=Gay/Lesbiana, 3=Bisexual,
#                     4=No sabe/No responde (P3038)
#   genero_diverso  : 1 si identidad de género es trans u otro (P3039 ∈ {3,4,5}),
#                     0 si cisgénero (P3039 ∈ {1,2} y concuerda con sexo al nacer),
#                     NA sin dato en P3039
#
# Salida:
#   datos/geih_genero_2022_2025.parquet
# =============================================================================

import os
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
DIR_BASE   = os.path.dirname(os.path.abspath(__file__))
DIR_INPUT  = os.path.join(DIR_BASE, "extraidos")
DIR_OUTPUT = os.path.join(DIR_BASE, "..", "datos")
ANOS       = list(range(2022, 2026))  # P3039 solo existe desde 2022
LLAVES     = ["DIRECTORIO", "SECUENCIA_P", "ORDEN", "HOGAR", "MES"]
SEP        = ";"
ENC        = "latin-1"

os.makedirs(DIR_OUTPUT, exist_ok=True)

# Columnas que necesitamos de Características generales (Marco 2018)
COLS_CG = set(LLAVES + [
    "P3271",    # Sexo al nacer
    "P3039",    # Identidad de género
    "P3038",    # Orientación sexual
    "FEX_C18",  # Factor de expansión
    "DPTO",     # Departamento
    "CLASE",    # Zona (urbano/rural)
    "P6040",    # Edad
])


# ---------------------------------------------------------------------------
# HELPERS (mismos que en 03/04, copiados para independencia del script)
# ---------------------------------------------------------------------------

def leer_csv(ruta, cols_pedir=None):
    """Lee CSV con sep=; encoding=latin-1, leyendo solo las columnas necesarias."""
    if not os.path.exists(ruta):
        return None
    with open(ruta, encoding=ENC) as fh:
        raw_header = fh.readline().lstrip('\ufeff').rstrip('\n')
    header = [c.strip() for c in raw_header.split(SEP)]

    if cols_pedir:
        usecols = [c for c in header if c in cols_pedir]
    else:
        usecols = None

    df = pd.read_csv(ruta, sep=SEP, encoding=ENC, dtype=str,
                     low_memory=False, usecols=usecols)
    df.columns = [c.lstrip('\ufeff').strip() for c in df.columns]
    return df


def encontrar_modulo(dir_anio, patron):
    """Devuelve la primera ruta cuyo nombre EMPIEZA con el patrón (case-insensitive)."""
    pat = re.compile(r'^' + re.escape(patron), re.IGNORECASE)
    for f in sorted(os.listdir(dir_anio)):
        if pat.match(f) and f.endswith(".csv"):
            return os.path.join(dir_anio, f)
    return None


def parse_int_series(s):
    """Vectorizado: string→Int64 nullable."""
    return pd.to_numeric(
        s.astype(str).str.strip().str.replace(",", ".", regex=False),
        errors="coerce"
    ).astype("Int64")


# ---------------------------------------------------------------------------
# PROCESAMIENTO POR AÑO
# ---------------------------------------------------------------------------

def procesar_anio(anio):
    print(f"\n{'='*60}")
    print(f"  Año {anio}")
    print(f"{'='*60}")

    dir_anio = os.path.join(DIR_INPUT, str(anio))
    if not os.path.isdir(dir_anio):
        print(f"  [FATAL] Directorio {dir_anio} no existe — saltando")
        return None

    # Buscar Características generales (Marco 2018: incluye coma en el nombre)
    ruta_cg = encontrar_modulo(dir_anio, "Caracteristicas_generales,")
    if ruta_cg is None:
        # Variante con tilde
        ruta_cg = encontrar_modulo(dir_anio, "Características_generales,")
    if ruta_cg is None:
        # Variante sin tildes ni coma (fallback)
        ruta_cg = encontrar_modulo(dir_anio, "Caractersticas_generales")
    if ruta_cg is None:
        print(f"  [FATAL] Características_generales no encontrado — saltando")
        return None

    print(f"  Leyendo CG ({os.path.getsize(ruta_cg)/1e6:.0f} MB)…")
    cg = leer_csv(ruta_cg, cols_pedir=COLS_CG)
    print(f"  → {len(cg):,} filas, {len(cg.columns)} cols")

    # Verificar que P3039 existe
    if "P3039" not in cg.columns:
        print(f"  [FATAL] P3039 no encontrada en CG {anio} — saltando")
        return None

    # ── Variables armonizadas ────────────────────────────────────────────────

    dt = pd.DataFrame()

    # Llaves de cruce
    for col in ["DIRECTORIO", "SECUENCIA_P", "ORDEN", "HOGAR"]:
        dt[col] = cg[col].str.strip() if col in cg.columns else pd.NA

    # Mes
    dt["mes"] = cg["MES"].str.strip().str.lower() if "MES" in cg.columns else pd.NA

    # Año
    dt["anio"] = pd.array([anio] * len(cg), dtype="Int64")

    # Departamento (2 dígitos)
    if "DPTO" in cg.columns:
        dpto_num = pd.to_numeric(cg["DPTO"], errors="coerce")
        dt["dpto_cod"] = dpto_num.dropna().astype(int).astype(str).str.zfill(2)
        dt["dpto_cod"] = dt["dpto_cod"].where(dpto_num.notna(), other=pd.NA)
    else:
        dt["dpto_cod"] = pd.NA

    # Zona (1=cabecera, 2=resto)
    dt["clase_zona"] = parse_int_series(cg["CLASE"]) if "CLASE" in cg.columns else pd.NA

    # Edad
    dt["edad"] = parse_int_series(cg["P6040"]) if "P6040" in cg.columns else pd.NA

    # Factor de expansión
    dt["fex"] = pd.to_numeric(
        cg["FEX_C18"].astype(str).str.strip().str.replace(",", ".", regex=False),
        errors="coerce"
    ) if "FEX_C18" in cg.columns else float("nan")

    # Sexo al nacer (P3271): 1=Hombre, 2=Mujer
    dt["sexo_nacer"] = parse_int_series(cg["P3271"]) if "P3271" in cg.columns else pd.NA

    # Identidad de género (P3039): 1=Hombre, 2=Mujer, 3=Trans, 4=No binario
    dt["identidad_genero"] = parse_int_series(cg["P3039"])

    # Orientación sexual (P3038): 1=Heterosexual, 2=Gay/Lesbiana, 3=Bisexual, 4=NS/NR
    dt["orientacion_sex"] = parse_int_series(cg["P3038"]) if "P3038" in cg.columns else pd.NA

    # Variable derivada: género diverso
    # Diccionario DANE P3039:
    #   1=Hombre, 2=Mujer, 3=Hombre trans, 4=Mujer trans, 5=Otro
    #
    # Lógica:
    #   0 = cisgénero: P3039 ∈ {1,2} Y concuerda con sexo al nacer
    #       (nacido hombre + se identifica hombre, o nacida mujer + se identifica mujer)
    #   1 = género diverso: P3039 ∈ {3,4,5} (trans u otro)
    #       O discordancia entre P3039 (1 o 2) y P3271
    #   NA = P3039 sin respuesta
    sexo = dt["sexo_nacer"]
    iden = dt["identidad_genero"]
    genero_div = pd.Series(pd.NA, index=dt.index, dtype="Int64")

    # Solo clasificar cuando P3039 tiene respuesta válida
    tiene_resp = iden.notna()

    # Cisgénero: nacido hombre + se identifica hombre, o nacida mujer + se identifica mujer
    cis_mask = tiene_resp & (((sexo == 1) & (iden == 1)) | ((sexo == 2) & (iden == 2)))
    genero_div = genero_div.where(~cis_mask, other=pd.array([0]*len(dt), dtype="Int64"))

    # Diverso: hombre trans (3), mujer trans (4), otro (5),
    # o discordancia sexo-identidad en categorías binarias
    div_mask = tiene_resp & (
        iden.isin([3, 4, 5]) |
        ((sexo == 1) & (iden == 2)) |
        ((sexo == 2) & (iden == 1))
    )
    genero_div = genero_div.where(~div_mask, other=pd.array([1]*len(dt), dtype="Int64"))

    dt["genero_diverso"] = genero_div

    # ── Reporte ──────────────────────────────────────────────────────────────
    n = len(dt)
    n_con_p3039 = dt["identidad_genero"].notna().sum()
    n_diverso = (dt["genero_diverso"] == 1).sum()
    n_htrans = (dt["identidad_genero"] == 3).sum()
    n_mtrans = (dt["identidad_genero"] == 4).sum()
    n_otro = (dt["identidad_genero"] == 5).sum()
    n_discord = n_diverso - n_htrans - n_mtrans - n_otro  # discordantes binarios

    print(f"\n  Resumen {anio}:")
    print(f"    Total personas: {n:,}")
    print(f"    Con P3039 respondido: {n_con_p3039:,} ({100*n_con_p3039/n:.1f}%)")
    print(f"    Género diverso: {n_diverso:,} ({100*n_diverso/n_con_p3039:.2f}% de respondentes)")
    print(f"      Hombre trans (P3039=3): {n_htrans:,}")
    print(f"      Mujer trans  (P3039=4): {n_mtrans:,}")
    print(f"      Otro         (P3039=5): {n_otro:,}")
    print(f"      Discordantes binarios:  {n_discord:,}")

    if "fex" in dt.columns:
        fex_diverso = dt.loc[dt["genero_diverso"] == 1, "fex"].sum()
        fex_total = dt.loc[dt["identidad_genero"].notna(), "fex"].sum()
        print(f"    Población expandida diversa: {fex_diverso:,.0f} ({100*fex_diverso/fex_total:.2f}%)")

    del cg
    return dt


# ---------------------------------------------------------------------------
# EJECUCIÓN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import time
    t0 = time.time()
    print("=" * 60)
    print("  PARQUET GÉNERO DIVERSO — GEIH 2022-2025")
    print("  DAT4CCIÓN 2026 — EQUILAB")
    print("=" * 60)

    frames = []
    for anio in ANOS:
        resultado = procesar_anio(anio)
        if resultado is not None:
            frames.append(resultado)

    if not frames:
        print("\n  [FATAL] No se procesó ningún año")
        exit(1)

    # Combinar panel
    panel = pd.concat(frames, ignore_index=True)
    del frames

    print(f"\n{'='*60}")
    print(f"  PANEL COMBINADO 2022-2025")
    print(f"{'='*60}")
    print(f"  Total filas: {len(panel):,}")

    # Resumen por año
    resumen = (panel.groupby("anio")
               .agg(
                   filas=("anio", "count"),
                   con_p3039=("identidad_genero", lambda x: x.notna().sum()),
                   diverso=("genero_diverso", lambda x: (x == 1).sum()),
                   h_trans=("identidad_genero", lambda x: (x == 3).sum()),
                   m_trans=("identidad_genero", lambda x: (x == 4).sum()),
                   otro=("identidad_genero", lambda x: (x == 5).sum()),
               )
               .reset_index())
    print(resumen.to_string(index=False))

    # Guardar
    tabla = pa.Table.from_pandas(panel, preserve_index=False)
    ruta_out = os.path.join(DIR_OUTPUT, "geih_genero_2022_2025.parquet")
    pq.write_table(tabla, ruta_out, compression="snappy")
    sz = os.path.getsize(ruta_out) / 1e6
    print(f"\n  ✓ Guardado: datos/geih_genero_2022_2025.parquet ({sz:.1f} MB)")

    elapsed = time.time() - t0
    print(f"  Tiempo: {elapsed:.1f} seg")
    print("=" * 60)
    print("  COMPLETADO")
    print("=" * 60)
