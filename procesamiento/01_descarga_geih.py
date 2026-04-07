"""
DAT4CCIÓN — Descarga automatizada GEIH (2019–2025)
====================================================
Portal: microdatos.dane.gov.co

Hallazgo clave: el reCAPTCHA del portal es puramente client-side.
El servidor responde 200 OK a las URLs de descarga sin validar ningún token.
Las URLs se extraen del atributo onclick: mostrarModal('archivo.zip', 'URL')

Uso:
    pip install requests beautifulsoup4
    python descarga_geih.py

    # Solo algunos años:
    python descarga_geih.py --anios 2023 2024 2025

    # Cambiar destino:
    python descarga_geih.py --destino "D:/MisCarpetas/DANE"
"""

import argparse
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DESTINO_DEFAULT = Path(__file__).parent / "datos_crudos"

CATALOGO_GEIH = {
    2019: 599,
    2020: 780,
    2021: 701,
    2022: 771,
    2023: 782,
    2024: 819,
    2025: 853,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "es-CO,es;q=0.9",
}

# ---------------------------------------------------------------------------
# Funciones
# ---------------------------------------------------------------------------

def extraer_urls_descarga(session: requests.Session, catalog_id: int) -> list[dict]:
    """Scrape la página get-microdata y retorna lista de {archivo, url}."""
    url_pagina = (
        f"https://microdatos.dane.gov.co/index.php/catalog/{catalog_id}/get-microdata"
    )
    print(f"  Leyendo catálogo {catalog_id} ...", end=" ", flush=True)

    try:
        resp = session.get(url_pagina, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR: {e}")
        return []

    # Extraer onclick="mostrarModal('Mes Año.zip', 'URL')"
    patron = r"mostrarModal\('([^']+\.zip)'\s*,\s*'([^']+)'\)"
    matches = re.findall(patron, resp.text)

    # Deduplicar por URL
    seen = set()
    archivos = []
    for nombre, url in matches:
        url = url.strip()
        if url not in seen:
            seen.add(url)
            archivos.append({"archivo": nombre.strip(), "url": url})

    print(f"{len(archivos)} archivos encontrados")
    return archivos


def descargar_zip(
    session: requests.Session,
    url: str,
    ruta_destino: Path,
    reintentos: int = 3,
) -> bool:
    """Descarga un ZIP con barra de progreso textual. Retorna True si OK."""
    if ruta_destino.exists():
        size_mb = ruta_destino.stat().st_size / 1024**2
        print(f"    [ya existe {size_mb:.1f} MB, saltando] {ruta_destino.name}")
        return True

    ruta_destino.parent.mkdir(parents=True, exist_ok=True)
    ruta_tmp = ruta_destino.with_suffix(".tmp")

    for intento in range(1, reintentos + 1):
        try:
            print(f"    Descargando: {ruta_destino.name} ", end="", flush=True)
            t0 = time.time()

            with session.get(url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                descargado = 0

                with open(ruta_tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 512):  # 512 KB
                        f.write(chunk)
                        descargado += len(chunk)
                        if total:
                            pct = descargado / total * 100
                            print(f"\r    Descargando: {ruta_destino.name} {pct:5.1f}%", end="", flush=True)

            ruta_tmp.rename(ruta_destino)
            elapsed = time.time() - t0
            size_mb = ruta_destino.stat().st_size / 1024**2
            print(f"\r    ✓ {ruta_destino.name} [{size_mb:.1f} MB en {elapsed:.0f}s]")
            return True

        except requests.RequestException as e:
            print(f"\r    Intento {intento}/{reintentos} falló: {e}")
            if ruta_tmp.exists():
                ruta_tmp.unlink()
            if intento < reintentos:
                time.sleep(5 * intento)

    print(f"    ✗ No se pudo descargar: {ruta_destino.name}")
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Descarga GEIH microdatos del DANE (2019–2025)"
    )
    parser.add_argument(
        "--anios",
        nargs="+",
        type=int,
        default=sorted(CATALOGO_GEIH.keys()),
        help="Años a descargar (ej: --anios 2023 2024 2025)",
    )
    parser.add_argument(
        "--destino",
        default=DESTINO_DEFAULT,
        help="Carpeta raíz de destino",
    )
    args = parser.parse_args()

    destino = Path(args.destino)
    anios = [a for a in args.anios if a in CATALOGO_GEIH]
    anios_invalidos = [a for a in args.anios if a not in CATALOGO_GEIH]

    if anios_invalidos:
        print(f"⚠ Años sin catalog ID conocido (ignorados): {anios_invalidos}")

    print("\n" + "=" * 60)
    print(f"  DAT4CCIÓN — Descarga GEIH {min(anios)}–{max(anios)}")
    print(f"  Destino: {destino}")
    print("=" * 60 + "\n")

    resumen = {}

    with requests.Session() as session:
        session.headers.update(HEADERS)

        for anio in sorted(anios):
            catalog_id = CATALOGO_GEIH[anio]
            print(f"► Año {anio}  (catalog ID: {catalog_id})")

            archivos = extraer_urls_descarga(session, catalog_id)
            if not archivos:
                resumen[anio] = "ERROR al obtener lista"
                continue

            carpeta_anio = destino / str(anio)
            ok = fail = 0

            for item in archivos:
                ruta = carpeta_anio / item["archivo"]
                exito = descargar_zip(session, item["url"], ruta)
                if exito:
                    ok += 1
                else:
                    fail += 1

            resumen[anio] = f"{ok} OK, {fail} fallos"
            print(f"  Resumen {anio}: {resumen[anio]}\n")

    print("=" * 60)
    print("  RESUMEN FINAL")
    print("=" * 60)
    for anio, resultado in sorted(resumen.items()):
        print(f"  {anio}: {resultado}")
    print(f"\nArchivos guardados en: {destino}")


if __name__ == "__main__":
    main()
