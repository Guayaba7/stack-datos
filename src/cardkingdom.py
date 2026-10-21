"""
Card Kingdom para Stack: venta, stock y BUYLIST (cuanto paga CK y cuantas quiere) de cada version del catalogo >= $2.
Ctrl+T -> CARD KINGDOM -> Ins.

No es un scraper de paginas: Card Kingdom publica su lista completa en un JSON (la misma que usa MTGJSON):
    https://api.cardkingdom.com/api/v2/pricelist        {"meta": {...}, "data": [producto, ...]}
Cada producto trae scryfall_id, is_foil, variation, edition, sku, url, price_retail / qty_retail (venta y stock),
price_buy / qty_buying (buylist) y condition_values (precio y stock por condicion: NM, EX, VG, G).
Por eso el emparejado es directo: id de Scryfall + acabado (foil / etched por 'variation'), sin reglas de SKU.

Una descarga al dia basta (CK actualiza su lista una vez al dia). A veces CK regresa una pagina HTML vacia en vez del
JSON: se detecta y se reintenta; no se pisa lo bueno que ya habia.

Archivos (data/tiendas/cardkingdom/):
  descargas/ck_pricelist_AAAAmmdd_HHMMSS.json.gz   la lista tal cual (se guardan las 2 mas recientes)
  ck.sqlite        precios (lo que usa Stack), historial por dia (venta, stock, buylist, cuantas quiere)
  progreso.json    avance (lo muestra Stack)        registro.txt   lo que imprime la descarga

Uso:   python src/cardkingdom.py              (descarga y empareja)
       python src/cardkingdom.py emparejar    (vuelve a emparejar la ultima descarga, sin conectarse)
"""
import gzip
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

TIENDA = "cardkingdom"
URL_PRICELIST = "https://api.cardkingdom.com/api/v2/pricelist"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Stack/1.0 (precios para tienda local)", "Accept": "application/json"}
REINTENTOS = 4
DESCARGAS_A_CONSERVAR = 2

RAIZ = Path(os.environ.get("STACK_RAIZ") or Path(__file__).resolve().parent.parent)
RUTA_HISTORIAL = RAIZ / "data" / "precios" / "historial.sqlite"
CARPETA = RAIZ / "data" / "tiendas" / "cardkingdom"
CARPETA_DESC = CARPETA / "descargas"
RUTA_DB = CARPETA / "ck.sqlite"
RUTA_PROGRESO = CARPETA / "progreso.json"
RUTA_DETENER = CARPETA_DESC / "DETENER.txt"

DDL = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS precios (scryfall_id TEXT NOT NULL, acabado TEXT NOT NULL, venta REAL, stock INTEGER, venta_nm REAL, stock_nm INTEGER,
    compra REAL, quiere INTEGER, sku TEXT, url TEXT, variacion TEXT, edicion TEXT, PRIMARY KEY (scryfall_id, acabado));
CREATE TABLE IF NOT EXISTS historial (dia TEXT NOT NULL, scryfall_id TEXT NOT NULL, acabado TEXT NOT NULL, venta REAL, stock INTEGER,
    compra REAL, quiere INTEGER, PRIMARY KEY (dia, scryfall_id, acabado));
CREATE TABLE IF NOT EXISTS dias (dia TEXT PRIMARY KEY, archivo TEXT, productos INTEGER, versiones INTEGER, registrado_utc TEXT);
"""


def ahora_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def escribir_progreso(datos):
    try:
        CARPETA.mkdir(parents=True, exist_ok=True)
        tmp = RUTA_PROGRESO.with_name(RUTA_PROGRESO.name + ".part")
        tmp.write_text(json.dumps({"tienda": TIENDA, "actualizado": ahora_utc(), "pid": os.getpid(), "procesos": 1, **datos},
                                  ensure_ascii=False), encoding="utf-8")
        tmp.replace(RUTA_PROGRESO)
    except OSError:
        pass


def num(v):
    try:
        return None if v in (None, "") else float(v)
    except (TypeError, ValueError):
        return None


def entero(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def acabado_de(p):
    """foil / etched / nonfoil. CK marca el etched en 'variation' (p. ej. 'Foil Etched'); is_foil viene como texto."""
    var = str(p.get("variation") or "").lower()
    if "etched" in var:
        return "etched"
    return "foil" if str(p.get("is_foil")).strip().lower() in ("true", "1", "yes") else "nonfoil"


# ---------------------------------------------------------------- descarga
def descargar_lista():
    """Baja el JSON completo a descargas/ (gzip). -> ruta, o lanza error con explicacion."""
    CARPETA_DESC.mkdir(parents=True, exist_ok=True)
    destino = CARPETA_DESC / f"ck_pricelist_{datetime.now():%Y%m%d_%H%M%S}.json.gz"
    ultimo = None
    for intento in range(REINTENTOS):
        if RUTA_DETENER.exists():
            raise RuntimeError("detenido")
        tmp = destino.with_name(destino.name + ".part")
        try:
            with requests.get(URL_PRICELIST, headers=HEADERS, timeout=(15, 180), stream=True) as r:
                r.raise_for_status()
                total, bajado, t0 = int(r.headers.get("Content-Length") or 0), 0, time.time()
                with gzip.open(tmp, "wb", compresslevel=6) as out:
                    for bloque in r.iter_content(1 << 16):
                        out.write(bloque)
                        bajado += len(bloque)
                        if time.time() - t0 > 1:
                            t0 = time.time()
                            escribir_progreso({"estado": "descargando", "hechas": bajado, "total": total or max(bajado, 1),
                                               "productos": 0, "fase": f"bajando la lista ({bajado / 1e6:,.0f} MB)"})
            with gzip.open(tmp, "rt", encoding="utf-8") as f:   # que sea el JSON y no una pagina vacia
                cabeza = f.read(200).lstrip()
            if not cabeza.startswith("{"):
                raise ValueError("Card Kingdom regreso una pagina en vez de la lista (se reintenta)")
            tmp.replace(destino)
            return destino
        except (requests.RequestException, ValueError, OSError) as e:
            ultimo = e
            tmp.unlink(missing_ok=True)
            print(f"AVISO: intento {intento + 1}: {type(e).__name__}: {e}")
            time.sleep(5 * 2 ** intento)
    raise RuntimeError(f"No se pudo bajar la lista de Card Kingdom ({type(ultimo).__name__}: {ultimo})")


def limpiar_descargas():
    for viejo in sorted(CARPETA_DESC.glob("ck_pricelist_*.json.gz"), reverse=True)[DESCARGAS_A_CONSERVAR:]:
        viejo.unlink(missing_ok=True)


# ---------------------------------------------------------------- emparejado (directo por scryfall_id)
def claves_catalogo():
    if not RUTA_HISTORIAL.exists():
        raise RuntimeError(f"No existe {RUTA_HISTORIAL}. Corre scryfall.py primero.")
    con = sqlite3.connect(f"{RUTA_HISTORIAL.resolve().as_uri()}?mode=ro", uri=True)
    try:
        return {f"{i}|{fin}" for i, fin in con.execute("SELECT id, finish FROM cards")}
    finally:
        con.close()


def emparejar(ruta):
    """Lee la lista guardada y llena precios + historial del dia de la descarga. -> resumen."""
    claves = claves_catalogo()
    with gzip.open(ruta, "rt", encoding="utf-8") as f:
        datos = json.load(f)                        # ~40-60 MB de JSON: se lee una vez y se suelta al terminar
    productos = datos.get("data") or []
    creado = (datos.get("meta") or {}).get("created_at")
    base = ((datos.get("meta") or {}).get("base_url") or "https://www.cardkingdom.com/").rstrip("/") + "/"
    del datos
    filas, sin_id = {}, 0
    for p in productos:
        sid = p.get("scryfall_id")
        if not sid:
            sin_id += 1
            continue
        k = (sid, acabado_de(p))
        if f"{k[0]}|{k[1]}" not in claves:
            continue
        cv = p.get("condition_values") or {}
        fila = (num(p.get("price_retail")), entero(p.get("qty_retail")), num(cv.get("nm_price")), entero(cv.get("nm_qty")),
                num(p.get("price_buy")) or None, entero(p.get("qty_buying")), p.get("sku"),   # 0.00 = CK no la compra
                (p.get("url") if str(p.get("url") or "").startswith("http") else base + str(p.get("url") or "").lstrip("/")) if p.get("url") else None,
                p.get("variation"), p.get("edition"))
        previa = filas.get(k)                        # dos productos para la misma version: se queda el que CK compra mas caro
        if previa is None or (fila[4] or 0) > (previa[4] or 0):
            filas[k] = fila
    n_prod = len(productos)
    del productos
    m = re.search(r"(\d{4})(\d{2})(\d{2})_", Path(ruta).name)
    dia = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else datetime.now().date().isoformat()
    CARPETA.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(RUTA_DB, timeout=30)
    try:
        con.executescript(DDL)
        con.execute("DELETE FROM precios")
        con.executemany("INSERT INTO precios VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [(*k, *v) for k, v in filas.items()])
        con.execute("DELETE FROM historial WHERE dia = ?", (dia,))
        con.execute("INSERT INTO historial SELECT ?, scryfall_id, acabado, venta, stock, compra, quiere FROM precios", (dia,))
        con.execute("INSERT OR REPLACE INTO dias VALUES (?, ?, ?, ?, ?)", (dia, Path(ruta).name, n_prod, len(filas), ahora_utc()))
        inicio = datetime.fromtimestamp(Path(ruta).stat().st_mtime, timezone.utc).isoformat(timespec="seconds")
        con.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)", [(k, str(v)) for k, v in {
            "capturado_utc": inicio, "ck_creado": creado, "archivo": Path(ruta).name, "dia": dia, "productos": n_prod,
            "versiones": len(filas), "sin_scryfall_id": sin_id, "emparejado_utc": ahora_utc()}.items()])
        con.commit()
    finally:
        con.close()
    con_compra = sum(1 for v in filas.values() if v[4])
    print(f"Card Kingdom: {n_prod:,} productos en la lista · {len(filas):,} versiones del catalogo · {con_compra:,} con buylist · {sin_id:,} sin id de Scryfall")
    return {"productos": n_prod, "versiones": len(filas), "con_compra": con_compra, "dia": dia}


def descargar():
    RUTA_DETENER.unlink(missing_ok=True)
    escribir_progreso({"estado": "descargando", "hechas": 0, "total": 1, "productos": 0, "fase": "conectando con Card Kingdom"})
    try:
        ruta = descargar_lista()
        escribir_progreso({"estado": "descargando", "hechas": 1, "total": 1, "productos": 0, "fase": "emparejando"})
        res = emparejar(ruta)
        limpiar_descargas()
    except Exception as e:                            # noqa: BLE001
        detenido = str(e) == "detenido"
        escribir_progreso({"estado": "detenido" if detenido else "error", "detalle": str(e)[:300], "hechas": 0, "total": 1})
        print(("Detenido." if detenido else f"ERROR: {e}"))
        RUTA_DETENER.unlink(missing_ok=True)
        return 0 if detenido else 1
    escribir_progreso({"estado": "terminado", "hechas": 1, "total": 1, "productos": res["versiones"], "versiones": res["versiones"],
                       "con_compra": res["con_compra"], "fase": f"{res['versiones']:,} versiones · {res['con_compra']:,} con buylist"})
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    accion = (argv[0] if argv else "descargar").lower()
    if accion == "emparejar":
        ultimas = sorted(CARPETA_DESC.glob("ck_pricelist_*.json.gz"))
        if not ultimas:
            print("No hay ninguna lista descargada todavia.")
            return 1
        emparejar(ultimas[-1])
        return 0
    if accion == "detener":
        RUTA_DETENER.parent.mkdir(parents=True, exist_ok=True)
        RUTA_DETENER.write_text("detener", encoding="utf-8")
        return 0
    return descargar()


if __name__ == "__main__":
    sys.exit(main())
