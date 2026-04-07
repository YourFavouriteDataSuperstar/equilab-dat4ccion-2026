# =============================================================================
# 03_crear_parquet_ocupados.py — GEIH 2019-2025 | DAT4CCIÓN 2026 — EQUILAB
# Crea parquet de ocupados armonizados — adaptación de armonizar.py
#
# Qué hace:
#   1. Lee Ocupados + Características_generales + Fuerza_de_trabajo por año
#   2. Une por DIRECTORIO+SECUENCIA_P+ORDEN+HOGAR (left join sobre Ocupados)
#   3. Crea variables armonizadas unificadas Marco 2005 ↔ Marco 2018
#   4. Aplica corrección: swap posicion_ocup 7↔8 en Marco 2005 para armonizar a Marco 2018
#   5. Guarda: armonizados/{año}/ocupados_armon_{año}.parquet  (por año — temp)
#              ../datos/geih_ocupados_2019_2025.parquet        (panel final)
#
# Variantes de columnas resueltas aquí:
#   FEX : fex_c_2011 (2019) | FEX_C (2020) | FEX_C18 (2021-2025)
#   SEXO: P6020 (Marco 2005) | P3271 (Marco 2018)
#   EDU : P6210+P6210S1+ESC (Marco 2005) | P3042+P3042S1 (Marco 2018)
#   RAMA: RAMA2D (2019) | RAMA2D_R4 (2020-2025)
#   ANO : presente en 2020; inyectado desde dir en 2019 y 2021-2025
# =============================================================================

import os
import re
import csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
DIR_BASE         = os.path.dirname(os.path.abspath(__file__))
DIR_INPUT        = os.path.join(DIR_BASE, "extraidos")
DIR_OUTPUT       = os.path.join(DIR_BASE, "..", "datos")
DIR_TEMP_ANIO    = os.path.join(DIR_BASE, "armonizados")
ANOS             = list(range(2019, 2026))
LLAVES           = ["DIRECTORIO", "SECUENCIA_P", "ORDEN", "HOGAR", "MES"]
SEP              = ";"
ENC              = "latin-1"

os.makedirs(DIR_OUTPUT, exist_ok=True)
os.makedirs(DIR_TEMP_ANIO, exist_ok=True)

# ---------------------------------------------------------------------------
# TABLA DE ESCOLARIDAD — Marco 2018 (P3042 = nivel, P3042S1 = grado último)
# Fuente: DANE, Diccionario GEIH 2023
# ---------------------------------------------------------------------------
ANIOS_BASE_EDU_M2018 = {
    1:  0,   # Ninguno
    2:  0,   # Preescolar
    3:  0,   # Básica primaria (grado 1-5)
    4:  5,   # Básica secundaria (grado 6-9)
    5:  9,   # Media (grado 10-13)
    6: 11,   # Técnica profesional
    7: 11,   # Tecnológica
    8: 11,   # Universitaria
    9: 15,   # Especialización
   10: 16,   # Maestría
   11: 18,   # Doctorado
}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def leer_csv(ruta, cols_pedir=None):
    """Lee CSV con sep=; encoding=latin-1, leyendo solo las columnas necesarias."""
    if not os.path.exists(ruta):
        return None
    # Primero leer solo el header para saber qué columnas existen
    with open(ruta, encoding=ENC) as fh:
        raw_header = fh.readline().lstrip('\ufeff').rstrip('\n')
    header = raw_header.split(SEP)
    header = [c.strip() for c in header]

    if cols_pedir:
        usecols = [c for c in header if c in cols_pedir]
    else:
        usecols = None

    df = pd.read_csv(ruta, sep=SEP, encoding=ENC, dtype=str,
                     low_memory=False, usecols=usecols)
    df.columns = [c.lstrip('\ufeff').strip() for c in df.columns]
    return df


def encontrar_modulo(dir_anio, patron):
    """Devuelve la primera ruta cuyo nombre EMPIEZA con el patrón (case-insensitive).
    Anclar al inicio evita que 'Ocupados' coincida con 'Desocupados'."""
    pat = re.compile(r'^' + re.escape(patron), re.IGNORECASE)
    for f in sorted(os.listdir(dir_anio)):
        if pat.match(f) and f.endswith(".csv"):
            return os.path.join(dir_anio, f)
    return None


def parse_float_series(s):
    """Vectorizado: convierte series string→float manejando coma decimal colombiana."""
    return pd.to_numeric(
        s.astype(str).str.strip().str.replace(",", ".", regex=False),
        errors="coerce"
    )


def parse_int_series(s):
    """Vectorizado: string→Int64 nullable."""
    return parse_float_series(s).astype("Int64")


def calc_esc_m2018(nivel, grado):
    """Años de escolaridad a partir de P3042 (nivel) + P3042S1 (grado)."""
    try:
        niv = int(float(nivel))
        base = ANIOS_BASE_EDU_M2018.get(niv)
        if base is None:
            return float("nan")
        try:
            g = int(float(grado))
            g = max(0, g)
        except Exception:
            g = 0
        return float(base + g)
    except Exception:
        return float("nan")


# ---------------------------------------------------------------------------
# PROCESAMIENTO POR AÑO
# ---------------------------------------------------------------------------

def procesar_anio(anio):
    print(f"\n{'='*60}")
    print(f"  Año {anio}")
    print(f"{'='*60}")

    marco = "2005" if anio <= 2020 else "2018"
    dir_anio = os.path.join(DIR_INPUT, str(anio))

    # Columnas que necesitamos de cada módulo (por marco)
    COLS_OCUP = set(LLAVES + [
        "MES", "ANO", "DPTO", "CLASE",
        "INGLABO", "P6500", "P6800", "P6430",
        "RAMA2D", "RAMA2D_R4", "OFICIO", "OFICIO_C8",
        "OCI", "fex_c_2011", "FEX_C", "FEX_C18",
        "P6240",  # a veces viene en Ocupados también
    ])
    COLS_CG_2005 = set(LLAVES + ["MES", "CLASE",  # CLASE distingue Cabecera/Resto/Área en 2005
                                  "P6020", "P6040", "P6210", "P6210S1", "ESC",
                                  "P6160",          # pertenencia étnica (binario: 1=no, 2=sí)
                                  "fex_c_2011", "FEX_C"])
    COLS_CG_2018 = set(LLAVES + ["MES", "P3271", "P6040", "P3042", "P3042S1", "FEX_C18",
                                  "P6160"])         # pertenencia étnica (binario: 1=no, 2=sí)
    COLS_FT      = set(LLAVES + ["MES", "P6240", "FT", "PET", "FFT",
                                  "fex_c_2011", "FEX_C", "FEX_C18"])

    COLS_CG = COLS_CG_2005 if marco == "2005" else COLS_CG_2018

    # ── 1. Ocupados ─────────────────────────────────────────────────────────
    ruta_ocup = encontrar_modulo(dir_anio, "Ocupados")
    if ruta_ocup is None:
        print("  [FATAL] No se encontró Ocupados — saltando año")
        return None
    print(f"  Leyendo Ocupados ({os.path.getsize(ruta_ocup)/1e6:.0f} MB)…")
    ocup = leer_csv(ruta_ocup, cols_pedir=COLS_OCUP)
    print(f"  → {len(ocup):,} filas, {len(ocup.columns)} cols")

    # ── 2. Características generales ────────────────────────────────────────
    # 2021 tiene residuos del Marco 2005 — usar solo el Marco 2018
    if anio == 2021:
        ruta_cg = encontrar_modulo(dir_anio, "Caracteristicas_generales,")
    elif marco == "2005":
        ruta_cg = encontrar_modulo(dir_anio, "Caracteristicas_generales")
    else:
        ruta_cg = encontrar_modulo(dir_anio, "Caracteristicas_generales,")

    if ruta_cg is None:
        print("  [WARN] Características_generales no encontrado")
        cg = None
    else:
        print(f"  Leyendo CG ({os.path.getsize(ruta_cg)/1e6:.0f} MB)…")
        cg = leer_csv(ruta_cg, cols_pedir=COLS_CG)
        print(f"  → {len(cg):,} filas, {len(cg.columns)} cols")

    # ── 3. Fuerza de trabajo ─────────────────────────────────────────────────
    ruta_ft = encontrar_modulo(dir_anio, "Fuerza")
    if ruta_ft is None:
        print("  [WARN] Fuerza_de_trabajo no encontrado")
        ft = None
    else:
        print(f"  Leyendo FT ({os.path.getsize(ruta_ft)/1e6:.0f} MB)…")
        ft = leer_csv(ruta_ft, cols_pedir=COLS_FT)
        print(f"  → {len(ft):,} filas, {len(ft.columns)} cols")

    # ── 4. JOIN — semi-join primero para reducir CG/FT antes del merge ───────
    print("  Uniendo módulos…")
    dt = ocup.copy()

    def llaves_para_join(df_a, df_b, base_keys):
        """Llaves comunes entre df_a y df_b; añade CLASE si está en ambos (evita
        cruce entre áreas geográficas Marco 2005: Cabecera/Resto/Área)."""
        keys = [k for k in base_keys if k in df_a.columns and k in df_b.columns]
        if "CLASE" not in keys and "CLASE" in df_a.columns and "CLASE" in df_b.columns:
            keys.append("CLASE")
        return keys

    if cg is not None:
        llaves_ok = llaves_para_join(dt, cg, LLAVES)
        # Semi-join: filtrar CG a solo las filas que están en Ocupados
        keys_ocup = dt[llaves_ok].drop_duplicates()
        cg_fil = cg.merge(keys_ocup, on=llaves_ok, how="inner")
        del cg  # liberar memoria del CG completo
        # Deduplicar CG en llaves (en Marco 2005 hay overlap de DIRECTORIO entre áreas)
        cg_fil = cg_fil.drop_duplicates(subset=llaves_ok)
        # Solo columnas nuevas (no duplicar lo que ya tiene Ocupados)
        cols_nuevas_cg = llaves_ok + [c for c in cg_fil.columns
                                       if c not in dt.columns and c not in llaves_ok]
        dt = dt.merge(cg_fil[cols_nuevas_cg], on=llaves_ok, how="left")
        del cg_fil, keys_ocup
        print(f"    [debug] tras CG: {len(dt):,} filas")

    if ft is not None:
        llaves_ok = llaves_para_join(dt, ft, LLAVES)
        print(f"    [debug] FT llaves: {llaves_ok}, FT shape: {ft.shape}")
        keys_ocup = dt[llaves_ok].drop_duplicates()
        print(f"    [debug] FT keys_ocup: {len(keys_ocup):,}")
        ft_fil = ft.merge(keys_ocup, on=llaves_ok, how="inner")
        print(f"    [debug] ft_fil semi-join: {len(ft_fil):,}")
        del ft
        ft_fil = ft_fil.drop_duplicates(subset=llaves_ok)
        print(f"    [debug] ft_fil dedup: {len(ft_fil):,}")
        cols_nuevas_ft = llaves_ok + [c for c in ft_fil.columns
                                       if c not in dt.columns and c not in llaves_ok]
        print(f"    [debug] cols_nuevas_ft: {cols_nuevas_ft}")
        dt = dt.merge(ft_fil[cols_nuevas_ft], on=llaves_ok, how="left")
        print(f"    [debug] tras FT: {len(dt):,} filas")
        del ft_fil, keys_ocup

    print(f"  → {len(dt):,} filas tras join")

    # ── 5. VARIABLES ARMONIZADAS (vectorizado — sin .apply por fila) ─────────
    print("  Creando variables armonizadas…")

    def num(col):
        """Vectorizado string→float, maneja coma decimal; NaN si col no existe."""
        return parse_float_series(dt[col]) if col in dt.columns else pd.NA

    # marco y anio
    dt["marco"] = marco
    if "ANO" in dt.columns:
        dt["anio"] = pd.to_numeric(dt["ANO"], errors="coerce").astype("Int64")
    else:
        dt["anio"] = pd.array([anio] * len(dt), dtype="Int64")

    # mes (ya en español desde Python de extracción)
    dt["mes"] = dt["MES"].str.strip().str.lower() if "MES" in dt.columns else pd.NA

    # dpto (2 dígitos) — vectorizado
    if "DPTO" in dt.columns:
        dpto_num = pd.to_numeric(dt["DPTO"], errors="coerce")
        dt["dpto_cod"] = dpto_num.dropna().astype(int).astype(str).str.zfill(2)
        dt["dpto_cod"] = dt["dpto_cod"].where(dpto_num.notna(), other=pd.NA)
    else:
        dt["dpto_cod"] = pd.NA

    # clase zona
    dt["clase_zona"] = num("CLASE").astype("Int64") if "CLASE" in dt.columns else pd.NA

    # factor de expansión unificado → fex
    if marco == "2005":
        fex_col = "fex_c_2011" if "fex_c_2011" in dt.columns else \
                  "FEX_C"      if "FEX_C"      in dt.columns else None
    else:
        fex_col = "FEX_C18" if "FEX_C18" in dt.columns else None
    if fex_col:
        dt["fex"] = parse_float_series(dt[fex_col])
    else:
        print(f"  [WARN] FEX col no encontrada para {anio}")
        dt["fex"] = float("nan")

    # sexo (1=Hombre, 2=Mujer)
    sexo_col = "P6020" if (marco == "2005" and "P6020" in dt.columns) else \
               "P3271" if (marco == "2018" and "P3271" in dt.columns) else None
    dt["sexo"] = num(sexo_col).astype("Int64") if sexo_col else pd.NA

    # edad
    dt["edad"] = num("P6040").astype("Int64") if "P6040" in dt.columns else pd.NA

    # nivel educativo original + grado
    if marco == "2005":
        dt["niv_edu_orig"]  = num("P6210").astype("Int64")  if "P6210"   in dt.columns else pd.NA
        dt["niv_edu_grado"] = num("P6210S1").astype("Int64") if "P6210S1" in dt.columns else pd.NA
    else:
        dt["niv_edu_orig"]  = num("P3042").astype("Int64")  if "P3042"   in dt.columns else pd.NA
        dt["niv_edu_grado"] = num("P3042S1").astype("Int64") if "P3042S1" in dt.columns else pd.NA

    # nivel educativo armonizado — vectorizado con pd.Series.case_when / where
    if marco == "2005":
        dt["niv_edu_armon"] = dt["niv_edu_orig"]
    else:
        niv = pd.to_numeric(dt["niv_edu_orig"], errors="coerce")
        armon = pd.Series(pd.NA, index=dt.index, dtype="Int64")
        armon = armon.where(~niv.between(1, 5),   niv.astype("Int64"))   # 1-5 directo
        armon = armon.where(~niv.between(6, 11),  pd.array([6]*len(dt), dtype="Int64"))
        armon = armon.where(~(niv == 99),         pd.array([9]*len(dt), dtype="Int64"))
        dt["niv_edu_armon"] = armon

    # años de escolaridad — vectorizado
    if marco == "2005" and "ESC" in dt.columns:
        dt["esc_anios"] = parse_float_series(dt["ESC"])
    elif marco == "2018" and "P3042" in dt.columns:
        # base por nivel + grado
        niv_series = dt["niv_edu_orig"]
        grado_series = num("P3042S1").clip(lower=0).fillna(0) if "P3042S1" in dt.columns else pd.Series(0, index=dt.index)
        base_series = niv_series.map(ANIOS_BASE_EDU_M2018)
        dt["esc_anios"] = base_series + grado_series
        dt["esc_anios"] = dt["esc_anios"].where(base_series.notna())
    else:
        dt["esc_anios"] = float("nan")

    # ingreso laboral mensual (P6500) e ingreso total (INGLABO)
    dt["ing_mes"]   = parse_float_series(dt["P6500"])   if "P6500"   in dt.columns else float("nan")
    dt["ing_total"] = parse_float_series(dt["INGLABO"]) if "INGLABO" in dt.columns else float("nan")

    # horas trabajadas por semana
    dt["horas_sem"] = num("P6800").astype("Int64") if "P6800" in dt.columns else pd.NA

    # posición ocupacional
    dt["posicion_ocup"] = num("P6430").astype("Int64") if "P6430" in dt.columns else pd.NA

    # CORRECCIÓN: swap códigos 7↔8 en Marco 2005 para armonizar al esquema Marco 2018
    # Marco 2005 original: 7 = Sin remuneración otras emp, 8 = Jornalero
    # Marco 2018:          7 = Jornalero, 8 = Sin remuneración otras emp
    # Estandarizamos al esquema Marco 2018 en toda la serie
    if marco == "2005":
        mask_7 = dt["posicion_ocup"] == 7
        mask_8 = dt["posicion_ocup"] == 8
        dt.loc[mask_7, "posicion_ocup"] = 80   # temporal
        dt.loc[mask_8, "posicion_ocup"] = 7
        dt.loc[dt["posicion_ocup"] == 80, "posicion_ocup"] = 8
        n_swapped = mask_7.sum() + mask_8.sum()
        print(f"    [FIX] Swap posicion_ocup 7↔8 aplicado: {n_swapped:,} registros (Marco 2005)")

    # rama de actividad (2 dígitos) — código original por marco
    if "RAMA2D" in dt.columns:
        dt["rama2d_orig"] = dt["RAMA2D"].astype(str).str.strip()
    elif "RAMA2D_R4" in dt.columns:
        dt["rama2d_orig"] = dt["RAMA2D_R4"].astype(str).str.strip()
    else:
        dt["rama2d_orig"] = pd.NA

    # oficio (código original por marco)
    if "OFICIO" in dt.columns:
        dt["oficio_orig"] = dt["OFICIO"].astype(str).str.strip()
    elif "OFICIO_C8" in dt.columns:
        dt["oficio_orig"] = dt["OFICIO_C8"].astype(str).str.strip()
    else:
        dt["oficio_orig"] = pd.NA

    # condición de actividad (P6240 — igual en ambos marcos)
    dt["condicion_activ"] = num("P6240").astype("Int64") if "P6240" in dt.columns else pd.NA

    # OCI (ocupado flag)
    dt["oci"] = num("OCI").astype("Int64") if "OCI" in dt.columns else pd.NA

    # pertenencia étnica binaria (P6160 — disponible 2019-2025, ambos marcos)
    # Codificación original DANE: 1 = no pertenece a ningún grupo étnico (mayoría ~91%)
    #                             2 = sí pertenece a algún grupo étnico   (minoría ~9%)
    # etnia_bin: 0 = no pertenece, 1 = pertenece, NA = sin respuesta
    if "P6160" in dt.columns:
        p6160 = parse_float_series(dt["P6160"])          # string → float
        etnia = pd.Series(pd.NA, index=dt.index, dtype="Int64")
        etnia = etnia.where(p6160 != 1, other=pd.array([0]*len(dt), dtype="Int64"))
        etnia = etnia.where(p6160 != 2, other=pd.array([1]*len(dt), dtype="Int64"))
        dt["etnia_bin"] = etnia
    else:
        print(f"  [WARN] P6160 (etnia) no encontrado en CG {anio}")
        dt["etnia_bin"] = pd.NA

    # ── 6. Selección final de columnas armonizadas ────────────────────────────
    VARS_ARMON = [
        "marco", "anio", "mes", "dpto_cod", "clase_zona", "fex",
        "sexo", "edad",
        "niv_edu_orig", "niv_edu_armon", "niv_edu_grado", "esc_anios",
        "ing_mes", "ing_total", "horas_sem",
        "posicion_ocup", "rama2d_orig", "oficio_orig",
        "condicion_activ", "oci",
        "etnia_bin",   # 0=no pertenece grupo étnico, 1=sí, NA=sin respuesta (P6160)
        # llaves por si se necesita para joins posteriores
        "DIRECTORIO", "SECUENCIA_P", "ORDEN", "HOGAR",
    ]
    cols_out = [c for c in VARS_ARMON if c in dt.columns]
    dt_out = dt[cols_out].copy()

    # ── 7. Reporte de cobertura ───────────────────────────────────────────────
    n = len(dt_out)
    def pct_ok(col):
        if col not in dt_out.columns: return "N/A"
        v = dt_out[col]
        ok = v.notna() & (v.astype(str).str.strip() != "") & (v.astype(str).str.strip() != "nan")
        return f"{100*ok.mean():.1f}%"

    print(f"\n  Cobertura ({n:,} ocupados):")
    print(f"    sexo={pct_ok('sexo')}  edad={pct_ok('edad')}  "
          f"esc_anios={pct_ok('esc_anios')}  ing_total={pct_ok('ing_total')}  "
          f"fex={pct_ok('fex')}  etnia_bin={pct_ok('etnia_bin')}")
    print(f"    meses={sorted(dt_out['mes'].dropna().unique().tolist())}")

    # ── 8. Guardar parquet por año (en directorio temp) ──────────────────────
    dir_out_anio = os.path.join(DIR_TEMP_ANIO, str(anio))
    os.makedirs(dir_out_anio, exist_ok=True)
    ruta_pq = os.path.join(dir_out_anio, f"ocupados_armon_{anio}.parquet")
    tabla = pa.Table.from_pandas(dt_out, preserve_index=False)
    pq.write_table(tabla, ruta_pq, compression="snappy")
    print(f"  ✓ Guardado: armonizados/{anio}/ocupados_armon_{anio}.parquet "
          f"({os.path.getsize(ruta_pq)/1e6:.1f} MB)")

    # Liberar memoria inmediatamente tras guardar
    del dt, dt_out
    return True   # solo señal de éxito


# ---------------------------------------------------------------------------
# EJECUCIÓN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import time
    t0 = time.time()
    print("=" * 60)
    print("  CREACIÓN PARQUET OCUPADOS — GEIH 2019-2025 — DAT4CCIÓN 2026")
    print("=" * 60)

    anos_ok = []
    for anio in ANOS:
        ok = procesar_anio(anio)
        if ok:
            anos_ok.append(anio)

    # Panel combinado — leer desde parquets por año (sin acumular en RAM)
    print(f"\n\n{'='*60}")
    print("  Construyendo panel combinado 2019-2025…")
    tablas = []
    for anio in anos_ok:
        ruta_pq = os.path.join(DIR_TEMP_ANIO, str(anio), f"ocupados_armon_{anio}.parquet")
        tablas.append(pq.read_table(ruta_pq))
    tabla_panel = pa.concat_tables(tablas, promote=True)
    print(f"  Total filas panel: {tabla_panel.num_rows:,}")

    ruta_panel = os.path.join(DIR_OUTPUT, "geih_ocupados_2019_2025.parquet")
    pq.write_table(tabla_panel, ruta_panel, compression="snappy")
    print(f"  ✓ Panel final: ../datos/geih_ocupados_2019_2025.parquet "
          f"({os.path.getsize(ruta_panel)/1e6:.1f} MB)")

    # Resumen final — estadísticas básicas desde panel (liviano ahora)
    panel = tabla_panel.to_pandas()
    print(f"\n{'='*60}")
    print("  RESUMEN DE COBERTURA POR AÑO")
    print(f"{'='*60}")
    resumen = (panel.groupby("anio")
               .agg(
                   filas=("anio", "count"),
                   pct_sexo=("sexo", lambda x: round(100 * x.notna().mean(), 1)),
                   pct_edad=("edad", lambda x: round(100 * x.notna().mean(), 1)),
                   pct_edu =("niv_edu_armon", lambda x: round(100 * x.notna().mean(), 1)),
                   pct_ing =("ing_total", lambda x: round(100 * (x.notna() & (x > 0)).mean(), 1)),
               )
               .reset_index())
    print(resumen.to_string(index=False))

    elapsed = time.time() - t0
    print(f"\n  Tiempo total: {elapsed/60:.1f} min")
    print(f"  Output final: {DIR_OUTPUT}")
    print(f"  (Temp por año: {DIR_TEMP_ANIO})")
    print("=" * 60)
    print("  COMPLETADO")
    print("=" * 60)
