# =============================================================================
# 04_crear_parquet_pet.py — GEIH 2019-2025 | DAT4CCIÓN 2026 — EQUILAB
#
# Genera: datos/geih_pet_2019_2025.parquet
#
# Qué hace:
#   1. Lee Fuerza_de_trabajo (base: TODA la PET del hogar)
#   2. Une con Características_generales para demografía
#   3. Filtra a PET (edad >= 15)
#   4. Genera variables armonizadas de condición de actividad
#
# Para qué sirve:
#   Calcular TGP, TO, TD por sexo × año × interseccionalidad.
#   El parquet de ocupados (geih_ocupados_2019_2025.parquet) NO tiene
#   el denominador completo (PET) — este parquet sí lo tiene.
#
# Condición de actividad (condicion_activ):
#   1 = Ocupado
#   2 = Desocupado (busca trabajo activamente)
#   3 = Inactivo
#   Otros = revisar con diccionario DANE
#
# Variables resultado:
#   Para TGP : filter(edad >= 15)               → PET
#   Para PEA : filter(condicion_activ %in% 1:2) → PEA
#   Para TO  : filter(condicion_activ == 1)      → Ocupados
#   Para TD  : desocupados / PEA
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
DIR_TEMP_ANIO = os.path.join(DIR_BASE, "armonizados")
ANOS       = list(range(2019, 2026))
LLAVES     = ["DIRECTORIO", "SECUENCIA_P", "ORDEN", "HOGAR", "MES"]
SEP        = ";"
ENC        = "latin-1"

os.makedirs(DIR_OUTPUT, exist_ok=True)
os.makedirs(DIR_TEMP_ANIO, exist_ok=True)

# Años de escolaridad base por nivel educativo (Marco 2018)
ANIOS_BASE_EDU_M2018 = {
    1: 0, 2: 0, 3: 0, 4: 5, 5: 9,
    6: 11, 7: 11, 8: 11, 9: 15, 10: 16, 11: 18,
}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def leer_csv(ruta, cols_pedir=None):
    if not os.path.exists(ruta):
        return None
    with open(ruta, encoding=ENC) as fh:
        raw_header = fh.readline().lstrip('\ufeff').rstrip('\n')
    header = [c.strip() for c in raw_header.split(SEP)]
    usecols = [c for c in header if c in cols_pedir] if cols_pedir else None
    df = pd.read_csv(ruta, sep=SEP, encoding=ENC, dtype=str,
                     low_memory=False, usecols=usecols)
    df.columns = [c.lstrip('\ufeff').strip() for c in df.columns]
    return df


def encontrar_modulo(dir_anio, patron):
    pat = re.compile(r'^' + re.escape(patron), re.IGNORECASE)
    for f in sorted(os.listdir(dir_anio)):
        if pat.match(f) and f.endswith(".csv"):
            return os.path.join(dir_anio, f)
    return None


def parse_float(s):
    return pd.to_numeric(
        s.astype(str).str.strip().str.replace(",", ".", regex=False),
        errors="coerce"
    )


# ---------------------------------------------------------------------------
# PROCESAMIENTO POR AÑO
# ---------------------------------------------------------------------------

def procesar_anio_pet(anio):
    print(f"\n{'='*60}\n  Año {anio} (PET)\n{'='*60}")

    marco    = "2005" if anio <= 2020 else "2018"
    dir_anio = os.path.join(DIR_INPUT, str(anio))

    # Columnas que necesitamos de cada módulo
    COLS_FT = set(LLAVES + [
        "MES", "DPTO", "CLASE",
        "P6240",                          # condición de actividad
        "FT", "PET", "FFT",               # flags internos DANE
        "fex_c_2011", "FEX_C", "FEX_C18" # factores de expansión
    ])

    COLS_CG_2005 = set(LLAVES + ["MES", "CLASE",
                                  "P6020",  # sexo Marco 2005
                                  "P6040",  # edad
                                  "P6160",  # etnia
                                  "P6210", "P6210S1", "ESC",
                                  "fex_c_2011", "FEX_C"])
    COLS_CG_2018 = set(LLAVES + ["MES",
                                  "P3271",  # sexo Marco 2018
                                  "P6040",  # edad
                                  "P6160",  # etnia
                                  "P3042", "P3042S1", "FEX_C18"])
    COLS_CG = COLS_CG_2005 if marco == "2005" else COLS_CG_2018

    # ── 1. Fuerza de Trabajo ────────────────────────────────────────────────
    ruta_ft = encontrar_modulo(dir_anio, "Fuerza")
    if ruta_ft is None:
        print("  [FATAL] No se encontró Fuerza_de_trabajo — saltando año")
        return None
    print(f"  Leyendo FT ({os.path.getsize(ruta_ft)/1e6:.0f} MB)…")
    ft = leer_csv(ruta_ft, cols_pedir=COLS_FT)
    print(f"  → {len(ft):,} filas")

    # ── 2. Características generales ────────────────────────────────────────
    if anio == 2021:
        ruta_cg = encontrar_modulo(dir_anio, "Caracteristicas_generales,")
    elif marco == "2005":
        ruta_cg = encontrar_modulo(dir_anio, "Caracteristicas_generales")
    else:
        ruta_cg = encontrar_modulo(dir_anio, "Caracteristicas_generales,")

    if ruta_cg is None:
        print("  [WARN] CG no encontrado")
        cg = None
    else:
        print(f"  Leyendo CG ({os.path.getsize(ruta_cg)/1e6:.0f} MB)…")
        cg = leer_csv(ruta_cg, cols_pedir=COLS_CG)
        print(f"  → {len(cg):,} filas")

    # ── 3. JOIN FT ← CG ─────────────────────────────────────────────────────
    print("  Uniendo FT + CG…")
    dt = ft.copy()

    if cg is not None:
        llaves_ok = [k for k in LLAVES if k in dt.columns and k in cg.columns]
        if "CLASE" not in llaves_ok and "CLASE" in dt.columns and "CLASE" in cg.columns:
            llaves_ok.append("CLASE")
        # Semi-join: solo CG que está en FT
        keys_ft  = dt[llaves_ok].drop_duplicates()
        cg_fil   = cg.merge(keys_ft, on=llaves_ok, how="inner")
        cg_fil   = cg_fil.drop_duplicates(subset=llaves_ok)
        del cg
        cols_nuevas = llaves_ok + [c for c in cg_fil.columns
                                    if c not in dt.columns and c not in llaves_ok]
        dt = dt.merge(cg_fil[cols_nuevas], on=llaves_ok, how="left")
        del cg_fil, keys_ft
    print(f"  → {len(dt):,} filas tras join")

    # ── 4. VARIABLES ARMONIZADAS ─────────────────────────────────────────────
    def num(col):
        return parse_float(dt[col]) if col in dt.columns else pd.NA

    dt["marco"] = marco
    dt["anio"]  = pd.array([anio] * len(dt), dtype="Int64")
    dt["mes"]   = dt["MES"].str.strip().str.lower() if "MES" in dt.columns else pd.NA

    # Departamento
    if "DPTO" in dt.columns:
        dpto_num    = pd.to_numeric(dt["DPTO"], errors="coerce")
        dt["dpto_cod"] = dpto_num.dropna().astype(int).astype(str).str.zfill(2)
        dt["dpto_cod"] = dt["dpto_cod"].where(dpto_num.notna(), other=pd.NA)
    else:
        dt["dpto_cod"] = pd.NA

    # Zona
    dt["clase_zona"] = num("CLASE").astype("Int64") if "CLASE" in dt.columns else pd.NA

    # Factor de expansión
    if marco == "2005":
        fex_col = "fex_c_2011" if "fex_c_2011" in dt.columns else \
                  "FEX_C"      if "FEX_C"      in dt.columns else None
    else:
        fex_col = "FEX_C18" if "FEX_C18" in dt.columns else None
    dt["fex"] = parse_float(dt[fex_col]) if fex_col else float("nan")

    # Sexo
    sexo_col = "P6020" if (marco == "2005" and "P6020" in dt.columns) else \
               "P3271" if (marco == "2018" and "P3271" in dt.columns) else None
    dt["sexo"] = num(sexo_col).astype("Int64") if sexo_col else pd.NA

    # Edad
    dt["edad"] = num("P6040").astype("Int64") if "P6040" in dt.columns else pd.NA

    # Condición de actividad (P6240)
    dt["condicion_activ"] = num("P6240").astype("Int64") if "P6240" in dt.columns else pd.NA

    # Etnia binaria (P6160): 0=no pertenece, 1=sí pertenece
    if "P6160" in dt.columns:
        p6160 = parse_float(dt["P6160"])
        etnia = pd.Series(pd.NA, index=dt.index, dtype="Int64")
        etnia = etnia.where(p6160 != 1, other=pd.array([0]*len(dt), dtype="Int64"))
        etnia = etnia.where(p6160 != 2, other=pd.array([1]*len(dt), dtype="Int64"))
        dt["etnia_bin"] = etnia
    else:
        dt["etnia_bin"] = pd.NA

    # Nivel educativo armonizado
    if marco == "2005":
        dt["niv_edu_armon"] = num("P6210").astype("Int64") if "P6210" in dt.columns else pd.NA
        dt["esc_anios"]     = parse_float(dt["ESC"]) if "ESC" in dt.columns else float("nan")
    else:
        niv = num("P3042")
        armon = pd.Series(pd.NA, index=dt.index, dtype="Int64")
        armon = armon.where(~niv.between(1, 5),  niv.astype("Int64"))
        armon = armon.where(~niv.between(6, 11), pd.array([6]*len(dt), dtype="Int64"))
        armon = armon.where(~(niv == 99),        pd.array([9]*len(dt), dtype="Int64"))
        dt["niv_edu_armon"] = armon
        # Años de escolaridad
        if "P3042" in dt.columns and "P3042S1" in dt.columns:
            grado      = num("P3042S1").clip(lower=0).fillna(0)
            base_serie = niv.map(ANIOS_BASE_EDU_M2018)
            dt["esc_anios"] = (base_serie + grado).where(base_serie.notna())
        else:
            dt["esc_anios"] = float("nan")

    # ── 5. Filtro PET (edad >= 15) ───────────────────────────────────────────
    n_antes = len(dt)
    dt = dt[dt["edad"].fillna(0) >= 15].copy()
    print(f"  Filtro PET (≥15 años): {n_antes:,} → {len(dt):,} filas")

    # ── 6. Selección final de columnas ───────────────────────────────────────
    VARS_PET = [
        "marco", "anio", "mes", "dpto_cod", "clase_zona", "fex",
        "sexo", "edad", "etnia_bin",
        "niv_edu_armon", "esc_anios",
        "condicion_activ",
        "DIRECTORIO", "SECUENCIA_P", "ORDEN", "HOGAR",
    ]
    cols_out = [c for c in VARS_PET if c in dt.columns]
    dt_out   = dt[cols_out].copy()

    # ── 7. Reporte de cobertura ───────────────────────────────────────────────
    n = len(dt_out)
    def pct_ok(col):
        if col not in dt_out.columns: return "N/A"
        v  = dt_out[col]
        ok = v.notna() & (v.astype(str).str.strip() != "nan")
        return f"{100*ok.mean():.1f}%"

    cond_dist = dt_out["condicion_activ"].value_counts(dropna=False).sort_index()
    print(f"\n  Cobertura PET ({n:,} personas ≥15 años):")
    print(f"    sexo={pct_ok('sexo')}  etnia={pct_ok('etnia_bin')}  "
          f"edu={pct_ok('niv_edu_armon')}  fex={pct_ok('fex')}")
    print(f"    condicion_activ: {cond_dist.to_dict()}")

    # ── 8. Guardar parquet por año ────────────────────────────────────────────
    dir_out_anio = os.path.join(DIR_TEMP_ANIO, str(anio))
    os.makedirs(dir_out_anio, exist_ok=True)
    ruta_pq = os.path.join(dir_out_anio, f"pet_armon_{anio}.parquet")
    tabla   = pa.Table.from_pandas(dt_out, preserve_index=False)
    pq.write_table(tabla, ruta_pq, compression="snappy")
    print(f"  ✓ Guardado: armonizados/{anio}/pet_armon_{anio}.parquet "
          f"({os.path.getsize(ruta_pq)/1e6:.1f} MB)")

    del dt, dt_out
    return True


# ---------------------------------------------------------------------------
# EJECUCIÓN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import time
    t0 = time.time()
    print("=" * 60)
    print("  CREACIÓN PARQUET PET GEIH 2019-2025 — DAT4CCIÓN 2026")
    print("=" * 60)

    anos_ok = []
    for anio in ANOS:
        ok = procesar_anio_pet(anio)
        if ok:
            anos_ok.append(anio)

    # Panel combinado
    print(f"\n\n{'='*60}")
    print("  Construyendo panel PET combinado 2019-2025…")
    tablas = []
    for anio in anos_ok:
        ruta = os.path.join(DIR_TEMP_ANIO, str(anio), f"pet_armon_{anio}.parquet")
        tablas.append(pq.read_table(ruta))
    tabla_panel = pa.concat_tables(tablas, promote=True)
    print(f"  Total filas: {tabla_panel.num_rows:,}")

    ruta_panel = os.path.join(DIR_OUTPUT, "geih_pet_2019_2025.parquet")
    pq.write_table(tabla_panel, ruta_panel, compression="snappy")
    print(f"  ✓ Panel: datos/geih_pet_2019_2025.parquet "
          f"({os.path.getsize(ruta_panel)/1e6:.1f} MB)")

    # Resumen
    panel = tabla_panel.to_pandas()
    print(f"\n{'='*60}")
    print("  RESUMEN POR AÑO")
    print(f"{'='*60}")
    resumen = (panel.groupby("anio")
               .agg(
                   filas      = ("anio", "count"),
                   pct_sexo   = ("sexo",          lambda x: round(100*x.notna().mean(),1)),
                   pct_etnia  = ("etnia_bin",      lambda x: round(100*x.notna().mean(),1)),
                   pct_edu    = ("niv_edu_armon",  lambda x: round(100*x.notna().mean(),1)),
                   n_ocupados = ("condicion_activ",lambda x: (x==1).sum()),
                   n_desocup  = ("condicion_activ",lambda x: (x==2).sum()),
                   n_inactivo = ("condicion_activ",lambda x: (x==3).sum()),
               )
               .reset_index())
    print(resumen.to_string(index=False))

    elapsed = time.time() - t0
    print(f"\n  Tiempo total: {elapsed/60:.1f} min")
    print(f"  Output: {DIR_OUTPUT}")
    print("=" * 60)
    print("  COMPLETADO")
    print("=" * 60)
