"""
MTGJSON para Stack (antes importar_mtgjson.py). Amplia el historial de precios de Stack con los ultimos ~90 dias de MTGJSON (sin esperar a irlo juntando dia con dia).

Que hace:
  1. Descarga de MTGJSON (https://mtgjson.com, datos abiertos):
       Meta.json            fecha de la version
       AllPrices.json.xz    precios de los ultimos 90 dias de cada impresion (TCGplayer, Card Kingdom, ...)
       AllIdentifiers.json.xz  solo cuando hace falta: de ahi sale la equivalencia uuid de MTGJSON -> id de Scryfall
     Todo queda en data/precios/mtgjson/ (solo la descarga mas reciente; la equivalencia se guarda en ids.sqlite).
  2. TCGplayer (el mismo precio que Scryfall usa como "usd"):
       - se compara contra los dias que YA tienes (los que bajo scryfall.py). Si no se parecen, NO se importa nada (--forzar).
       - se agregan a data/precios/historial.sqlite los dias que te faltan (anteriores a tu ultimo dia), con el mismo corte
         de >= $2 de scryfall. Esos dias quedan marcados con origen "mtgjson tcgplayer".
       - Nunca se toca un dia que bajaste tu de Scryfall.
       - Las impresiones que valieron >= $2 en esos dias y que no estaban en el historial se agregan a la tabla cartas con
         los datos del bulk de Scryfall mas reciente (data/precios/scryfall/).
       - Al final se recalculan las entradas a >= $2 (NUEVAS de Stack).
  3. Card Kingdom: por ahora solo se guarda el precio mas reciente (venta y compra) de cada version del historial, en la
     tabla ck_precios de historial.sqlite. Stack lo muestra en el detalle de la carta.

Uso:  python src/mtgjson.py            (descarga si hay version nueva e importa)
      python src/mtgjson.py nombres    (nombre de cada carta en otros idiomas -> tabla nombres_idioma; busqueda de Stack)
      python src/mtgjson.py --forzar   (importa aunque la comparacion contra tus dias no cuadre)
      python src/mtgjson.py --sin-descargar   (usa lo que ya esta en data/precios/mtgjson)
Stack lo puede iniciar desde Ctrl+T (TIENDAS) y muestra su avance en la esquina.
"""
import argparse
import gzip
import json
import lzma
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

for _flujo in (sys.stdout, sys.stderr):              # Windows: que un caracter raro (⚠, ¥, acentos) nunca tumbe el programa
    try:
        _flujo.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError, OSError):
        pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import scryfall  # noqa: E402  (rutas del historial, corte de $2, lectura del bulk, entradas y revision de precios)

URL_BASE = "https://mtgjson.com/api/v5/"
CARPETA = scryfall.CARPETA_PRECIOS / "mtgjson"
RUTA_IDS = CARPETA / "ids.sqlite"
RUTA_STAGING = CARPETA / "staging.sqlite"
RUTA_PROGRESO = CARPETA / "progreso.json"
RUTA_DETENER = CARPETA / "DETENER.txt"
RUTA_REPORTE = CARPETA / "importacion.txt"
ACABADO = {"normal": 0, "foil": 1, "etched": 2}
ORIGEN = "mtgjson tcgplayer"
MIN_COINCIDEN = 0.80        # de los precios que ya tienes y MTGJSON tambien trae, al menos 80% a menos de 5%
HEADERS = {"User-Agent": "Stack-MTG/0.4 (importar_mtgjson)"}


class Detenido(Exception):
    pass


# ---------------------------------------------------------------- avance (lo lee Stack)
_AV = {"inicio": None}


def avance(estado, pct, fase, **extra):
    if RUTA_DETENER.exists() and estado == "descargando":
        RUTA_DETENER.unlink(missing_ok=True)
        avance("detenido", pct, "detenido por el usuario")
        raise Detenido()
    try:
        CARPETA.mkdir(parents=True, exist_ok=True)
        tmp = RUTA_PROGRESO.with_name(RUTA_PROGRESO.name + ".part")
        tmp.write_text(json.dumps({"tienda": "mtgjson", "estado": estado, "hechas": round(pct, 1), "total": 100, "fase": fase,
                                   "actualizado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                   "inicio": _AV["inicio"], "pid": os.getpid(), **extra}, ensure_ascii=False), encoding="utf-8")
        tmp.replace(RUTA_PROGRESO)
    except OSError:
        pass
    print(f"[{pct:5.1f}%] {fase}", flush=True)


# ---------------------------------------------------------------- descarga
def descargar(nombre, destino, pct0, pct1):
    """Baja URL_BASE/nombre a destino (via .part). Avance de pct0 a pct1."""
    tmp = destino.with_name(destino.name + ".part")
    with requests.get(URL_BASE + nombre, headers=HEADERS, stream=True, timeout=60) as r:
        r.raise_for_status()
        total, hecho, ultimo = int(r.headers.get("Content-Length") or 0), 0, 0
        with open(tmp, "wb") as f:
            for trozo in r.iter_content(1 << 20):
                f.write(trozo)
                hecho += len(trozo)
                if time.time() - ultimo > 1:
                    ultimo = time.time()
                    p = hecho / total if total else 0
                    avance("descargando", pct0 + (pct1 - pct0) * p, f"descargando {nombre} {hecho / 1e6:,.0f}"
                           + (f" de {total / 1e6:,.0f} MB" if total else " MB"))
    tmp.replace(destino)


def meta_remota():
    r = requests.get(URL_BASE + "Meta.json", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return (r.json().get("data") or r.json().get("meta") or {})


def meta_local():
    try:
        return json.loads((CARPETA / "Meta.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


# ---------------------------------------------------------------- lectura en flujo de {"meta":..., "data": {clave: valor}}
def abrir(ruta):
    n = ruta.name.lower()
    if n.endswith(".xz"):
        return lzma.open(ruta, "rt", encoding="utf-8")
    if n.endswith(".gz"):
        return gzip.open(ruta, "rt", encoding="utf-8")
    return open(ruta, "r", encoding="utf-8")


RE_INICIO_DATA = re.compile(r'"data"\s*:\s*\{')
RE_CLAVE = re.compile(r'\s*,?\s*"((?:[^"\\]|\\.)*)"\s*:\s*')
RE_FIN = re.compile(r'\s*\}')


def entradas_data(ruta, al_leer=None, trozo=1 << 22):
    """Recorre el objeto "data" de un archivo de MTGJSON sin cargarlo entero: (clave, valor) de uno en uno. Cada valor se
    decodifica con el decodificador de JSON de Python (en C), asi que es rapido aunque el archivo pese GB."""
    dec = json.JSONDecoder()
    with abrir(ruta) as f:
        buf, pos, fin_archivo, leidos = "", 0, False, 0

        def mas():
            nonlocal buf, pos, fin_archivo, leidos
            t = f.read(trozo)
            if not t:
                fin_archivo = True
                return False
            leidos += len(t)
            buf = buf[pos:] + t
            pos = 0
            if al_leer:
                al_leer(leidos)
            return True

        while True:                                   # hasta el inicio de "data"
            m = RE_INICIO_DATA.search(buf, pos)
            if m:
                pos = m.end()
                break
            if not mas():
                raise ValueError(f"{ruta.name}: no tiene la seccion data")
        while True:
            if len(buf) - pos < 1024 and not fin_archivo:
                mas()
            m = RE_FIN.match(buf, pos)
            if m:
                return
            m = RE_CLAVE.match(buf, pos)
            if not m:
                if fin_archivo:
                    raise ValueError(f"{ruta.name}: formato inesperado cerca de {buf[pos:pos + 80]!r}")
                mas()
                continue
            clave = json.loads(f'"{m.group(1)}"')
            while True:
                try:
                    valor, fin = dec.raw_decode(buf, m.end())
                    break
                except ValueError:
                    if fin_archivo:
                        raise
                    mas()                               # el valor quedo cortado: se lee mas y se vuelve a intentar
                    m = RE_CLAVE.match(buf, 0)          # mas() recorta el buffer: la clave queda al inicio
                    if not m:
                        raise ValueError(f"{ruta.name}: no se pudo releer la clave {clave}")
            pos = fin
            yield clave, valor
            if pos > trozo:
                buf, pos = buf[pos:], 0


# ---------------------------------------------------------------- equivalencia uuid -> Scryfall
def abrir_ids():
    con = sqlite3.connect(RUTA_IDS)
    con.execute("CREATE TABLE IF NOT EXISTS ids (uuid TEXT PRIMARY KEY, scryfall_id TEXT) WITHOUT ROWID")
    con.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    return con


def actualizar_ids(pct0, pct1, sin_descargar=False):
    ruta = CARPETA / "AllIdentifiers.json.xz"
    if not sin_descargar or not ruta.exists():
        descargar("AllIdentifiers.json.xz", ruta, pct0, pct0 + (pct1 - pct0) * 0.6)
    con = abrir_ids()
    n = 0
    try:
        con.execute("BEGIN")
        lote = []
        ultimo = [0.0]

        def al_leer(_):
            if time.time() - ultimo[0] > 1:
                ultimo[0] = time.time()
                avance("descargando", pct0 + (pct1 - pct0) * 0.6, f"leyendo identificadores ({n:,})")

        for uuid, carta in entradas_data(ruta, al_leer):
            sid = ((carta or {}).get("identifiers") or {}).get("scryfallId")
            if sid:
                lote.append((uuid, sid))
                n += 1
            if len(lote) >= 50_000:
                con.executemany("INSERT OR REPLACE INTO ids VALUES (?, ?)", lote)
                lote.clear()
        con.executemany("INSERT OR REPLACE INTO ids VALUES (?, ?)", lote)
        con.execute("INSERT OR REPLACE INTO meta VALUES ('actualizado', ?)", (datetime.now(timezone.utc).isoformat(),))
        con.execute("COMMIT")
    finally:
        con.close()
    if not sin_descargar:
        ruta.unlink(missing_ok=True)                  # pesa mucho y ya no hace falta: la equivalencia queda en ids.sqlite
    return n


# ---------------------------------------------------------------- nombres de las cartas en otros idiomas (busqueda de Stack)
IDIOMAS_MTGJSON = {"Spanish": "es", "French": "fr", "German": "de", "Italian": "it", "Portuguese (Brazil)": "pt",
                   "Japanese": "ja", "Chinese Simplified": "zhs", "Chinese Traditional": "zht", "Korean": "ko", "Russian": "ru"}
DDL_NOMBRES = """CREATE TABLE IF NOT EXISTS nombres_idioma (   -- nombre oficial de cada carta en otros idiomas (de MTGJSON)
    name TEXT NOT NULL, idioma TEXT NOT NULL, nombre TEXT NOT NULL, PRIMARY KEY (name, idioma, nombre)) WITHOUT ROWID"""


def actualizar_nombres(sin_descargar=False):
    """Guarda en historial.sqlite (tabla nombres_idioma) el nombre de cada carta de la search_list en cada idioma que
    trae MTGJSON (AtomicCards: una entrada por carta, con foreignData). Stack los usa para buscar 'Relampago' y encontrar
    Lightning Bolt. Se reemplaza la tabla completa (o entra todo o no entra nada). -> (cartas con nombre, filas)."""
    ruta = CARPETA / "AtomicCards.json.xz"
    if not sin_descargar or not ruta.exists():
        descargar("AtomicCards.json.xz", ruta, 5, 50)
    con = scryfall.abrir_historial()
    try:
        buscadas = {r[0] for r in con.execute("SELECT name FROM search_list")}
        buscadas |= {r[0] for r in con.execute("SELECT DISTINCT name FROM cartas")}
        filas, ultimo = set(), [0.0]

        def al_leer(_):
            if time.time() - ultimo[0] > 1:
                ultimo[0] = time.time()
                avance("descargando", 60, f"leyendo nombres en otros idiomas ({len(filas):,})")

        for nombre, caras in entradas_data(ruta, al_leer):
            if nombre not in buscadas:
                continue
            for cara in caras or []:
                for fd in (cara or {}).get("foreignData") or []:
                    idioma = IDIOMAS_MTGJSON.get(fd.get("language"))
                    if not idioma:
                        continue
                    for x in {fd.get("name"), fd.get("faceName")}:
                        x = (x or "").strip()
                        if x and x.casefold() != nombre.casefold():
                            filas.add((nombre, idioma, x))
        if not filas:
            raise RuntimeError("AtomicCards no trajo ningun nombre en otro idioma para las cartas del catalogo")
        con.execute("BEGIN")
        con.execute(DDL_NOMBRES)
        con.execute("DELETE FROM nombres_idioma")
        con.executemany("INSERT OR IGNORE INTO nombres_idioma VALUES (?, ?, ?)", sorted(filas))
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    if not sin_descargar:
        ruta.unlink(missing_ok=True)
    return len({f[0] for f in filas}), len(filas)


def borrar_staging():
    """Borra el archivo de trabajo. Si Windows lo tiene ocupado, no pasa nada: la siguiente corrida lo reemplaza."""
    for _ in range(5):
        try:
            RUTA_STAGING.unlink(missing_ok=True)
            return
        except OSError:
            time.sleep(0.5)


# ---------------------------------------------------------------- historial
def dias_propios(con):
    return {d: (o or "") for d, o in con.execute("SELECT dia, origen FROM dias")}


def leer_precios(ruta, ids, dias_comparar, desde, hasta, pct0, pct1):
    """Pasa AllPrices a staging.sqlite: TCGplayer de los dias a importar (>= corte) y de los dias que ya tienes (todos, para
    comparar); Card Kingdom solo el dato mas reciente. -> (sin_equivalencia, fechas_vistas)."""
    borrar_staging()
    st = sqlite3.connect(RUTA_STAGING)
    st.executescript("""
        DROP TABLE IF EXISTS tcg; DROP TABLE IF EXISTS ck;
        CREATE TABLE tcg (dia INTEGER, sid TEXT, acabado INTEGER, centavos INTEGER, PRIMARY KEY (dia, sid, acabado)) WITHOUT ROWID;
        CREATE TABLE ck (sid TEXT, acabado INTEGER, fecha TEXT, venta INTEGER, compra INTEGER, PRIMARY KEY (sid, acabado)) WITHOUT ROWID;
    """)
    st.execute("BEGIN")
    try:
        tam = ruta.stat().st_size
        # el archivo comprimido se lee descomprimido: el avance se estima con la proporcion tipica (~12x)
        estimado = tam * 12
        ultimo, sin_eq, fechas, lote_t, lote_c = [0.0], 0, set(), [], []
        corte = int(round(scryfall.UMBRAL_USD * 100))

        def al_leer(leidos):
            if time.time() - ultimo[0] > 1:
                ultimo[0] = time.time()
                avance("descargando", pct0 + (pct1 - pct0) * min(0.99, leidos / estimado), "leyendo precios de MTGJSON")

        for uuid, formatos in entradas_data(ruta, al_leer):
            papel = (formatos or {}).get("paper") or {}
            tcg, ck = papel.get("tcgplayer") or {}, papel.get("cardkingdom") or {}
            if not tcg and not ck:
                continue
            sid = ids.get(uuid)
            if sid is None:
                sin_eq += 1
                continue
            for fin, serie in ((tcg.get("retail") or {}).items()):
                a = ACABADO.get(fin)
                if a is None:
                    continue
                for fecha, precio in (serie or {}).items():
                    fechas.add(fecha)
                    try:
                        c = int(round(float(precio) * 100))
                        d = scryfall.dia_num(fecha)
                    except (TypeError, ValueError):
                        continue
                    if d in dias_comparar or (desde <= d < hasta and c >= corte):
                        lote_t.append((d, sid, a, c))
            venta, compra = ck.get("retail") or {}, ck.get("buylist") or {}
            for fin in set(venta) | set(compra):
                a = ACABADO.get(fin)
                if a is None:
                    continue
                fv, fc = max((venta.get(fin) or {}), default=None), max((compra.get(fin) or {}), default=None)
                if not (fv or fc):
                    continue
                fecha = max(x for x in (fv, fc) if x)
                v = (venta.get(fin) or {}).get(fecha)
                cpr = (compra.get(fin) or {}).get(fecha)
                lote_c.append((sid, a, fecha, None if v is None else int(round(float(v) * 100)),
                               None if cpr is None else int(round(float(cpr) * 100))))
            if len(lote_t) > 100_000:
                st.executemany("INSERT OR IGNORE INTO tcg VALUES (?, ?, ?, ?)", lote_t)
                lote_t.clear()
            if len(lote_c) > 50_000:
                st.executemany("INSERT OR REPLACE INTO ck VALUES (?, ?, ?, ?, ?)", lote_c)
                lote_c.clear()
        st.executemany("INSERT OR IGNORE INTO tcg VALUES (?, ?, ?, ?)", lote_t)
        st.executemany("INSERT OR REPLACE INTO ck VALUES (?, ?, ?, ?, ?)", lote_c)
        st.execute("COMMIT")
    finally:
        if st.in_transaction:
            st.execute("ROLLBACK")
        st.close()                                   # siempre: si no, Windows no deja borrar staging.sqlite
    return sin_eq, fechas


def comparar(con, dias_comparar):
    """Tus precios (>= $2) contra los de MTGJSON en los dias que ya tienes."""
    if not dias_comparar:
        return None
    marcas = ",".join("?" * len(dias_comparar))
    total, encontrados, exactos, cerca = con.execute(
        f"""SELECT COUNT(*), COUNT(t.centavos), SUM(ABS(t.centavos - p.centavos) <= 1),
                   SUM(ABS(t.centavos - p.centavos) <= p.centavos * 0.05)
            FROM precios p JOIN cartas c ON c.n = p.n
            LEFT JOIN st.tcg t ON t.dia = p.dia AND t.sid = c.id AND t.acabado = p.acabado
            WHERE p.dia IN ({marcas})""", sorted(dias_comparar)).fetchone()
    return {"dias": len(dias_comparar), "tuyos": total, "en_mtgjson": encontrados, "exactos": exactos or 0, "cerca": cerca or 0}


def cartas_faltantes(con, faltan):
    """Datos de las impresiones que no estaban en el historial, del bulk de Scryfall mas reciente."""
    bulk = scryfall.CARPETA_SCRYFALL / scryfall.NOMBRE_BULK
    if not faltan or not bulk.exists():
        return {}
    datos = {}
    for carta in scryfall.leer_cartas(bulk):
        if carta.get("id") in faltan:
            datos[carta["id"]] = scryfall.datos_de_carta(carta)
            if len(datos) == len(faltan):
                break
    return datos


# ---------------------------------------------------------------- cuando Scryfall y MTGJSON no coinciden en UNA carta
DISPUTA = 2.0            # mediana de (tu precio / precio MTGJSON) >= 2 o <= 1/2 en los dias en comun
GANA_POR = 1.5           # el arbitro (Card Kingdom) tiene que quedar al menos 1.5x mas cerca de una fuente que de la otra
DDL_DISPUTAS = """CREATE TABLE IF NOT EXISTS disputas_fuente (
    n INTEGER NOT NULL, acabado INTEGER NOT NULL, dias INTEGER, centavos_scryfall INTEGER, centavos_mtgjson INTEGER,
    centavos_ck INTEGER, decision TEXT NOT NULL, revisado TEXT, PRIMARY KEY (n, acabado)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS correcciones_fuente (           -- precios tuyos reemplazados por los de MTGJSON (para deshacer)
    dia INTEGER NOT NULL, n INTEGER NOT NULL, acabado INTEGER NOT NULL, centavos_antes INTEGER, centavos_despues INTEGER,
    corregido TEXT, PRIMARY KEY (dia, n, acabado)) WITHOUT ROWID;"""


def arbitro_hareruya(con):
    """Segundo arbitro: el precio NM en ingles mas barato de Hareruya (emparejado mas reciente), en centavos de dolar.
    -> {(n, acabado): centavos}. Vacio si no hay emparejado o no hay tipo de cambio guardado."""
    carpeta = scryfall.CARPETA_DATA / "tiendas" / "hareruya" / "emparejado"
    rutas = sorted(carpeta.glob("emparejado_*.sqlite"), key=lambda r: r.stat().st_mtime, reverse=True) if carpeta.is_dir() else []
    try:
        tasa = float(json.loads((scryfall.CARPETA_DATA / "app" / "config.json").read_text(encoding="utf-8")).get("jpy_usd") or 0)
    except (OSError, ValueError, TypeError):
        tasa = 0
    if not rutas or tasa <= 0:
        return {}
    n_de = dict(con.execute("SELECT id, n FROM cartas"))
    salida = {}
    try:
        h = sqlite3.connect(f"{rutas[0].resolve().as_uri()}?mode=ro", uri=True)
        try:
            for sid, fin, jpy in h.execute(
                    """SELECT d.scryfall_id, d.acabado_scryfall, MIN(p.precio_jpy) FROM decisiones d
                       JOIN hareruya_productos p ON p.source_id = d.source_id
                       WHERE d.estado = 'confirmado' AND p.condicion = 'NM' AND p.idioma = 'en' AND p.precio_jpy > 0
                       GROUP BY 1, 2"""):
                n, a = n_de.get(sid), {"nonfoil": 0, "foil": 1, "etched": 2}.get(fin)
                if n is not None and a is not None:
                    salida[(n, a)] = int(round(jpy * tasa * 100))
        finally:
            h.close()
    except sqlite3.Error:
        return {}
    return salida


def disputas(con, mios):
    """Versiones donde tus precios (Scryfall) y los de MTGJSON no coinciden en los dias en comun. Decide quien tiene razon
    con Card Kingdom como arbitro: 'mtgjson' (Scryfall trae un precio equivocado), 'scryfall' (MTGJSON se equivoca) o
    'disputa' (no se puede saber). -> {(n, a): {...}}"""
    import math
    import statistics
    if not mios:
        return {}
    marcas = ",".join("?" * len(mios))
    pares = {}
    for n, a, dia, tuyo, mtg in con.execute(
            f"""SELECT p.n, p.acabado, p.dia, p.centavos, t.centavos FROM precios p JOIN cartas c ON c.n = p.n
                JOIN st.tcg t ON t.dia = p.dia AND t.sid = c.id AND t.acabado = p.acabado
                WHERE p.dia IN ({marcas}) AND t.centavos > 0""", sorted(mios)):
        pares.setdefault((n, a), []).append((dia, tuyo, mtg))
    ck = {(n, a): v for n, a, v in con.execute(
        "SELECT c.n, k.acabado, k.venta FROM st.ck k JOIN cartas c ON c.id = k.sid WHERE k.venta > 0")}
    har = arbitro_hareruya(con)
    salida = {}
    for clave, filas in pares.items():
        r = statistics.median(t / m for _, t, m in filas)
        if 1 / DISPUTA < r < DISPUTA:
            continue
        filas.sort()
        tuyo, mtg = filas[-1][1], filas[-1][2]
        arb, quien = ck.get(clave), "ck"
        if not arb:                                    # sin Card Kingdom: Hareruya
            arb, quien = har.get(clave), "hareruya"
        decision = "disputa"
        if arb:
            d_t, d_m = abs(math.log(tuyo / arb)), abs(math.log(mtg / arb))
            if d_m + math.log(GANA_POR) < d_t:
                decision = "mtgjson"
            elif d_t + math.log(GANA_POR) < d_m:
                decision = "scryfall"
        salida[clave] = {"dias": len(filas), "tuyo": tuyo, "mtg": mtg, "ck": arb, "arbitro": quien if arb else None,
                         "decision": decision, "filas": filas}
    return salida


def importar(con, hasta, pct0, pct1):
    """Pasa los dias de staging al historial en UNA transaccion. -> resumen."""
    con.execute(f"ATTACH DATABASE '{RUTA_STAGING.resolve().as_posix()}' AS st")
    try:
        propios = set(dias_propios(con))
        nuevos = [d for (d,) in con.execute("SELECT DISTINCT dia FROM st.tcg WHERE dia < ? ORDER BY dia", (hasta,))
                  if d not in propios]
        corte = int(round(scryfall.UMBRAL_USD * 100))
        faltan = {sid for (sid,) in con.execute(
            "SELECT DISTINCT t.sid FROM st.tcg t LEFT JOIN cartas c ON c.id = t.sid WHERE c.n IS NULL AND t.centavos >= ?", (corte,))}
        avance("descargando", pct0, f"datos de {len(faltan):,} impresiones nuevas para el historial")
        datos = cartas_faltantes(con, faltan)
        cols = ["id", "oracle_id", "name", "set_code", "set_name", "collector_number", "rarity", "lang", "released_at",
                "scryfall_uri", "promo_types", "tratamiento_foil", "imagen", "legal", "digital"]
        origen = dias_propios(con)
        mios = {d for d in propios if not origen[d].startswith("mtgjson")}
        disp = disputas(con, mios)
        con.execute("BEGIN")
        for sentencia in DDL_DISPUTAS.split(";"):
            if sentencia.strip():
                con.execute(sentencia)
        ahora0 = datetime.now(timezone.utc).isoformat(timespec="seconds")
        con.execute("DELETE FROM disputas_fuente")
        if "arbitro" not in [r[1] for r in con.execute("PRAGMA table_info(disputas_fuente)")]:
            con.execute("ALTER TABLE disputas_fuente ADD COLUMN arbitro TEXT")
        con.executemany("INSERT INTO disputas_fuente (n, acabado, dias, centavos_scryfall, centavos_mtgjson, centavos_ck, decision, "
                        "revisado, arbitro) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [(n, a, x["dias"], x["tuyo"], x["mtg"], x["ck"], x["decision"], ahora0, x["arbitro"]) for (n, a), x in disp.items()])
        corte0 = int(round(scryfall.UMBRAL_USD * 100))
        corregidos = 0
        for (n, a), x in disp.items():
            if x["decision"] == "mtgjson":            # Scryfall trae un precio equivocado: tus dias toman el de MTGJSON
                for dia, tuyo, mtg in x["filas"]:
                    con.execute("INSERT OR IGNORE INTO correcciones_fuente VALUES (?, ?, ?, ?, ?, ?)", (dia, n, a, tuyo, mtg, ahora0))
                    if mtg < corte0:
                        con.execute("DELETE FROM precios WHERE dia = ? AND n = ? AND acabado = ?", (dia, n, a))
                    else:
                        con.execute("UPDATE precios SET centavos = ? WHERE dia = ? AND n = ? AND acabado = ?", (mtg, dia, n, a))
                    corregidos += 1
            else:                                     # no se mezclan: fuera los dias de MTGJSON de esa version
                con.execute("""DELETE FROM precios WHERE n = ? AND acabado = ?
                               AND dia IN (SELECT dia FROM dias WHERE origen LIKE 'mtgjson%')""", (n, a))
        con.execute("CREATE TEMP TABLE IF NOT EXISTS sin_mtgjson (n INTEGER, acabado INTEGER, PRIMARY KEY (n, acabado))")
        con.execute("DELETE FROM sin_mtgjson")
        con.executemany("INSERT INTO sin_mtgjson VALUES (?, ?)", [k for k, x in disp.items() if x["decision"] != "mtgjson"])
        con.executemany(f"INSERT OR IGNORE INTO cartas ({', '.join(cols)}, actualizado) VALUES ({', '.join('?' * len(cols))}, 0)",
                        [[datos[i][k] for k in cols] for i in datos])
        ahora = datetime.now(timezone.utc).isoformat(timespec="seconds")
        por_dia = {}
        for k, d in enumerate(nuevos):
            avance("descargando", pct0 + (pct1 - pct0) * (k + 1) / max(1, len(nuevos)), f"agregando {scryfall.dia_txt(d)} al historial")
            con.execute("""INSERT INTO precios (dia, n, acabado, centavos)
                           SELECT t.dia, c.n, t.acabado, t.centavos FROM st.tcg t JOIN cartas c ON c.id = t.sid
                           WHERE t.dia = ? AND t.centavos >= ?
                             AND NOT EXISTS (SELECT 1 FROM sin_mtgjson x WHERE x.n = c.n AND x.acabado = t.acabado)
                           ORDER BY c.n, t.acabado""", (d, corte))
            nv, ni = con.execute("SELECT COUNT(*), COUNT(DISTINCT n) FROM precios WHERE dia = ?", (d,)).fetchone()
            if nv == 0:
                continue
            con.execute("INSERT INTO dias VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (d, scryfall.dia_txt(d), None, None, nv, ni, None, ahora, ORIGEN))
            por_dia[d] = nv
        # Card Kingdom: el dato mas reciente de cada version del historial
        con.execute("""CREATE TABLE IF NOT EXISTS ck_precios (n INTEGER NOT NULL, acabado INTEGER NOT NULL, fecha TEXT,
                       venta_centavos INTEGER, compra_centavos INTEGER, PRIMARY KEY (n, acabado)) WITHOUT ROWID""")
        con.execute("DELETE FROM ck_precios")
        con.execute("""INSERT INTO ck_precios SELECT c.n, k.acabado, k.fecha, k.venta, k.compra
                       FROM st.ck k JOIN cartas c ON c.id = k.sid""")
        n_ck = con.execute("SELECT COUNT(*) FROM ck_precios").fetchone()[0]
        for d in {dia for x in disp.values() for dia, _, _ in x["filas"]} | set(por_dia):
            con.execute("UPDATE dias SET versiones = (SELECT COUNT(*) FROM precios WHERE dia = ?) WHERE dia = ?", (d, d))
        scryfall.recalcular_entradas(con)
        con.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                        [("mtgjson_importado", ahora), ("revision_precios", ahora), ("mtgjson_dias", ",".join(scryfall.dia_txt(d) for d in por_dia)),
                         ("ck_fecha", (con.execute("SELECT MAX(fecha) FROM ck_precios").fetchone()[0] or ""))])
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.execute("DETACH DATABASE st")
    return {"dias": por_dia, "cartas_nuevas": len(datos), "sin_datos": len(faltan) - len(datos), "ck": n_ck,
            "disputas": disp, "corregidos": corregidos}


# ---------------------------------------------------------------- programa
def main(argv=None):
    ap = argparse.ArgumentParser(description="Amplia el historial de Stack con los ultimos 90 dias de MTGJSON.")
    ap.add_argument("accion", nargs="?", default="importar", help=argparse.SUPPRESS)   # Stack.exe pasa 'descargar'
    ap.add_argument("--forzar", action="store_true", help="importar aunque la comparacion contra tus dias no cuadre")
    ap.add_argument("--sin-descargar", action="store_true", help="usar los archivos que ya estan en data/precios/mtgjson")
    a = ap.parse_args(argv)
    _AV["inicio"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    CARPETA.mkdir(parents=True, exist_ok=True)
    if a.accion == "nombres":                         # python src/mtgjson.py nombres: solo los nombres en otros idiomas
        cartas, n = actualizar_nombres(a.sin_descargar)
        avance("terminado", 100, f"nombres en otros idiomas: {cartas:,} cartas, {n:,} nombres")
        return 0
    RUTA_DETENER.unlink(missing_ok=True)
    lineas = [f"Importacion de MTGJSON  {datetime.now():%Y-%m-%d %H:%M}", ""]
    try:
        if not scryfall.RUTA_HISTORIAL.exists():
            raise RuntimeError("No hay historial (data/precios/historial.sqlite). Corre antes scryfall.py.")
        precios = CARPETA / "AllPrices.json.xz"
        avance("descargando", 1, "consultando la version de MTGJSON")
        local = meta_local()
        if a.sin_descargar:
            if not precios.exists():
                raise RuntimeError(f"No esta {precios}.")
            remota = local
        else:
            remota = meta_remota()
            if not precios.exists() or remota.get("date") != local.get("date"):
                descargar("AllPrices.json.xz", precios, 2, 45)
                (CARPETA / "Meta.json").write_text(json.dumps(remota), encoding="utf-8")
            else:
                avance("descargando", 45, f"AllPrices ya es la version {remota.get('date')}")
        lineas.append(f"MTGJSON version {remota.get('version', '?')} del {remota.get('date', '?')}")

        # equivalencia uuid -> Scryfall (se baja la primera vez o si quedo vieja)
        con_ids = abrir_ids()
        hay_ids = con_ids.execute("SELECT COUNT(*) FROM ids").fetchone()[0]
        fecha_ids = (con_ids.execute("SELECT value FROM meta WHERE key = 'actualizado'").fetchone() or [""])[0]
        con_ids.close()
        vieja = not fecha_ids or (datetime.now(timezone.utc) - datetime.fromisoformat(fecha_ids)).days >= 14
        if not hay_ids or (vieja and not a.sin_descargar):
            n = actualizar_ids(45, 60, a.sin_descargar)
            lineas.append(f"Equivalencias uuid -> Scryfall actualizadas: {n:,}")
        con_ids = abrir_ids()
        ids = dict(con_ids.execute("SELECT uuid, scryfall_id FROM ids"))
        con_ids.close()

        con = scryfall.abrir_historial()
        try:
            propios = dias_propios(con)
            mios = {d for d, o in propios.items() if not o.startswith("mtgjson")}
            if not mios:
                raise RuntimeError("El historial no tiene ningun dia propio todavia.")
            hasta = max(mios)                                   # solo dias ANTERIORES a tu ultimo dia
            sin_eq, fechas = leer_precios(precios, ids, mios, 0, hasta, 60, 85)
            if sin_eq > 0.02 * max(1, len(ids)) and not a.sin_descargar:
                lineas.append(f"Aviso: {sin_eq:,} productos sin equivalencia a Scryfall; se actualizan y se vuelve a leer.")
                actualizar_ids(60, 70)
                con_ids = abrir_ids()
                ids = dict(con_ids.execute("SELECT uuid, scryfall_id FROM ids"))
                con_ids.close()
                sin_eq, fechas = leer_precios(precios, ids, mios, 0, hasta, 70, 85)
            if fechas:
                lineas.append(f"MTGJSON trae precios del {min(fechas)} al {max(fechas)} ({len(fechas)} dias)")
            con.execute(f"ATTACH DATABASE '{RUTA_STAGING.resolve().as_posix()}' AS st")
            comp = comparar(con, mios & {scryfall.dia_num(f) for f in fechas})
            con.execute("DETACH DATABASE st")
            if comp:
                lineas.append(f"Comparacion en {comp['dias']} dias que ya tenias: {comp['tuyos']:,} precios tuyos >= $2; "
                              f"MTGJSON trae {comp['en_mtgjson']:,}; iguales al centavo {comp['exactos']:,}; "
                              f"a menos de 5%: {comp['cerca']:,}")
                parecido = comp["cerca"] / comp["en_mtgjson"] if comp["en_mtgjson"] else 0
                if parecido < MIN_COINCIDEN and not a.forzar:
                    raise RuntimeError(f"Solo {parecido:.0%} de los precios se parecen (minimo {MIN_COINCIDEN:.0%}): no se importo nada. "
                                       "Revisa el reporte; con --forzar se importa de todos modos.")
            else:
                lineas.append("No hay dias en comun para comparar (MTGJSON no cubre ninguno de tus dias).")
            r = importar(con, hasta, 86, 99)
            try:                                       # tu dia mas reciente (MTGJSON aun no lo trae): mismas correcciones
                scryfall.revisar_dia(con, scryfall.dia_txt(hasta), red=False, solo_disputas=True)
            except Exception as e:                     # noqa: BLE001
                lineas.append(f"Aviso: no se pudo revisar tu dia mas reciente ({type(e).__name__}: {e})")
            total = con.execute("SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM dias").fetchone()
        finally:
            con.close()
        borrar_staging()
        lineas += ["", f"Dias agregados al historial: {len(r['dias'])}" + (f" ({scryfall.dia_txt(min(r['dias']))} a {scryfall.dia_txt(max(r['dias']))})" if r["dias"] else ""),
                   f"Impresiones agregadas a cartas: {r['cartas_nuevas']:,}" + (f" ({r['sin_datos']:,} sin datos en el bulk, se omitieron)" if r["sin_datos"] else ""),
                   f"Card Kingdom: {r['ck']:,} versiones con precio guardado (venta y compra, el mas reciente)",
                   f"Historial ahora: {total[0]} dias ({total[1]} a {total[2]})"]
        disp = r["disputas"]
        cuenta = {k: sum(1 for x in disp.values() if x["decision"] == k) for k in ("mtgjson", "scryfall", "disputa")}
        lineas += ["", f"Cartas donde Scryfall y MTGJSON NO coinciden (x{DISPUTA:g} o mas): {len(disp):,}",
                   f"  Scryfall equivocado (Card Kingdom o Hareruya le dan la razon a MTGJSON): {cuenta['mtgjson']:,} -> tus dias se corrigieron "
                   f"({r['corregidos']:,} precios; los originales quedan en correcciones_fuente)",
                   f"  MTGJSON equivocado: {cuenta['scryfall']:,} -> no se usan sus dias para esas cartas",
                   f"  Sin arbitro claro: {cuenta['disputa']:,} -> no se mezclan; Stack las marca con \u26a0"]
        if disp:
            nombres = {}
            try:
                c2 = scryfall.abrir_historial()
                nombres = dict(c2.execute("SELECT n, name || ' (' || UPPER(set_code) || ' #' || collector_number || ')' FROM cartas"))
                c2.close()
            except sqlite3.Error:
                pass
            usd = lambda x: "-" if x is None else f"${x / 100:,.2f}"
            for (n, a_), x in sorted(disp.items(), key=lambda kv: -max(kv[1]["tuyo"], kv[1]["mtg"]))[:200]:
                lineas.append(f"  {x['decision']:9} {nombres.get(n, n)} {['normal', 'foil', 'etched'][a_]}: Scryfall {usd(x['tuyo'])} | "
                              f"MTGJSON {usd(x['mtg'])} | {'Hareruya' if x['arbitro'] == 'hareruya' else 'Card Kingdom'} {usd(x['ck'])} | {x['dias']} dias")
        lineas += ["", "Dias agregados:"] + [f"  {scryfall.dia_txt(d)}  {n:,} versiones >= $2" for d, n in sorted(r["dias"].items())]
        RUTA_REPORTE.write_text("\n".join(lineas) + "\n", encoding="utf-8")
        avance("terminado", 100, f"{len(r['dias'])} dias agregados", dias_agregados=len(r["dias"]))
        print("\n".join(lineas))
        return 0
    except Detenido:
        borrar_staging()
        return 1
    except Exception as e:                                # noqa: BLE001
        lineas += ["", f"ERROR: {e}"]
        RUTA_REPORTE.write_text("\n".join(lineas) + "\n", encoding="utf-8")
        avance("error", 0, str(e)[:300], detalle=str(e)[:300])
        print("\n".join(lineas))
        return 1


if __name__ == "__main__":
    sys.exit(main())
