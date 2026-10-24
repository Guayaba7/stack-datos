"""
SCRYFALL para Stack (antes Main1.py + revisar_precios.py): descarga el Default Cards del dia y lo SUMA al historial.

  1. Consulta /bulk-data de Scryfall (Default Cards) y lee su fecha de actualizacion (updated_at, UTC).
  2. Si ese dia ya esta en el historial, no hace nada. Si no, descarga el bulk (o usa el que ya este bajado y sea
     valido) a data/precios/scryfall/ (se guarda SOLO el mas reciente; el nuevo reemplaza al anterior al validarse).
  3. Lee el bulk UNA vez y agrega a data/precios/historial.sqlite, en una sola transaccion (o entra el dia completo o
     no entra nada):
         dias          el dia cargado: updated_at, registros leidos, versiones >= $2, nombres
         cartas        datos de cada impresion que alguna vez valio >= $2 (se actualizan con el dato mas reciente):
                       nombre, set, numero, rareza, idioma, lanzamiento, tratamiento foil, imagen, legalidad por formato
         precios       precio de cada version (impresion + acabado) >= $2 de ese dia, en centavos
         search_list   nombres unicos a buscar en las tiendas (UN renglon por nombre, aunque tenga muchas versiones)
         sets_formato  sets que acepta cada formato que depende del set (Standard, Pioneer, Modern, Premodern)
         entradas      versiones que entraron a la lista >= $2 y se mantuvieron ahi al menos 3 dias
     y escribe data/precios/validacion/AAAA-MM-DD.txt con los conteos y verificaciones del dia.
  4. Si Scryfall no responde o la descarga falla, usa el bulk valido que haya en data/precios/scryfall/ y lo avisa.

Ya no genera Excel ni un SQLite por dia (todo esta en el historial). La vista `cards` del historial tiene las mismas
columnas que el antiguo cards_over_2usd.sqlite y muestra el dia mas reciente.

  5. Revisa los precios "ridiculos" del dia (saltos contra la mediana de 7 dias) y los vuelve a consultar en la API de
     Scryfall: los corrige o los marca con aviso en Stack (tabla revision_precios; ver REVISION DE PRECIOS abajo).

Uso:  python src/scryfall.py                              (el dia de hoy)
      python src/scryfall.py revisar [--dia AAAA-MM-DD] [--sin-red]   (solo vuelve a revisar los precios dudosos)
      python src/scryfall.py bulk                         (solo deja el default-cards vigente en disco)
"""
import gzip
import json
import math
import os
import re
import sqlite3
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

for _flujo in (sys.stdout, sys.stderr):              # Windows: que un caracter raro (⚠, ¥, acentos) nunca tumbe el programa
    try:
        _flujo.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError, OSError):
        pass

import requests

UMBRAL_USD = 2.0  # se conservan precios >= este valor
# campo de precio de Scryfall -> acabado
CAMPOS_PRECIO = {"usd": "nonfoil", "usd_foil": "foil", "usd_etched": "etched"}
ACABADOS = {"nonfoil": 0, "foil": 1, "etched": 2}
CARPETA_DATA = Path(os.environ.get("STACK_RAIZ") or Path(__file__).resolve().parent.parent) / "data"   # STACK_RAIZ: Stack instalado
CARPETA_PRECIOS = CARPETA_DATA / "precios"
CARPETA_SCRYFALL = CARPETA_PRECIOS / "scryfall"           # solo el bulk mas reciente
CARPETA_VALIDACION = CARPETA_PRECIOS / "validacion"
RUTA_HISTORIAL = CARPETA_PRECIOS / "historial.sqlite"
# Scryfall no tiene precio aparte para surge foil, galaxy foil, etc.: su precio esta en usd_foil y el
# tratamiento se indica en promo_types. Se cuentan como tratamiento foil todos los promo_types que
# contienen "foil" (surgefoil, galaxyfoil, confettifoil, halofoil, raisedfoil, ...; incluye los que
# Scryfall agregue despues) y estos otros que no lo dicen en el nombre:
TRATAMIENTOS_FOIL_EXTRA = {"textured", "doublerainbow", "oilslick", "neonink", "gilded",
                           "stepandcompleat", "embossed", "invisibleink"}

URL_LISTA_BULK = "https://api.scryfall.com/bulk-data"
TIPO_BULK = "default_cards"
# Scryfall EXIGE estos dos headers.
HEADERS = {
    "User-Agent": "MTGLearningProject/0.1",
    "Accept": "application/json;q=0.9,*/*;q=0.8",
}
NOMBRE_BULK = "default-cards.jsonl.gz"
NOMBRE_META_BULK = "default-cards.meta.json"
PATRON_FECHA = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Entradas a la lista >= $2: la version debe seguir >= $2 en TODAS las capturas de los 3 dias desde que entra
# (el dia de entrada y los 2 siguientes). Si baja antes, no se registra. Si la impresion salio hace poco, se marca
# como impresion nueva (preventa/lanzamiento) en lugar de 'subio'.
DIAS_CONFIRMAR_ENTRADA = 3
DIAS_IMPRESION_NUEVA = 30

# Formatos: legalidad por carta (legalities de Scryfall) y sets de los formatos que dependen del set.
FORMATOS = ("standard", "pioneer", "modern", "legacy", "vintage", "pauper", "commander", "premodern")
CODIGO_LEGAL = {"legal": "L", "restricted": "R", "banned": "B", "not_legal": "N"}
FORMATO_POR_SET = {"standard": (None, None, {"core", "expansion"}),
                   "pioneer": ("2012-10-05", None, {"core", "expansion", "starter"}),          # desde Return to Ravnica
                   "modern": ("2003-07-28", None, {"core", "expansion", "draft_innovation", "starter"}),   # desde 8th Ed.
                   "premodern": ("1995-04-01", "2003-05-26", {"core", "expansion"})}           # 4th Ed. a Scourge
FRACCION_SET_FORMATO = 0.9     # un set entra al formato si >= 90% de sus cartas son legales (o prohibidas) ahi


class BulkInvalido(Exception):
    """El archivo bulk esta cortado, roto o no coincide con lo que se descargo."""


# ---------------------------------------------------------------- carpetas
def carpeta_tienda(tienda):
    """data/tiendas/<tienda>/ -- la crea si no existe. Para que cada tienda guarde ahi sus resultados."""
    carpeta = CARPETA_DATA / "tiendas" / tienda
    carpeta.mkdir(parents=True, exist_ok=True)
    return carpeta


def fecha_de_bulk(updated_at):
    """'2026-10-11T09:05:36.123+00:00' -> '2026-10-11' (fecha UTC de la actualizacion en Scryfall)."""
    momento = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    if momento.tzinfo is None:
        momento = momento.replace(tzinfo=timezone.utc)
    return momento.astimezone(timezone.utc).date().isoformat()


def dia_num(fecha):
    """'2026-10-16' -> 20261016 (asi se guarda en el historial: ocupa menos que el texto)."""
    return int(fecha.replace("-", ""))


def dia_txt(n):
    s = str(n)
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


def temporal_de(ruta):
    """Nombre provisional mientras se escribe; conserva la extension para que gzip lo reconozca."""
    return ruta.with_name("_parcial_" + ruta.name)


# ---------------------------------------------------------------- bulk: consultar, validar, descargar
def consultar_scryfall():
    """Metadatos del bulk Default Cards, o None si Scryfall no responde."""
    try:
        respuesta = requests.get(URL_LISTA_BULK, headers=HEADERS, timeout=30)
        respuesta.raise_for_status()
        for item in respuesta.json()["data"]:
            if item["type"] == TIPO_BULK:
                return item
        print(f"AVISO: Scryfall no devolvio un bulk de tipo '{TIPO_BULK}'.")
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"AVISO: no pude consultar Scryfall ({e}).")
    return None


def leer_cartas(ruta):
    """Una carta a la vez. Acepta .jsonl(.gz) y el .json(.gz) clasico."""
    abrir = gzip.open if ruta.name.lower().endswith(".gz") else open
    with abrir(ruta, "rt", encoding="utf-8") as f:
        if ".jsonl" in ruta.name.lower():
            for linea in f:
                if linea.strip():
                    yield json.loads(linea)
        else:
            yield from json.load(f)


def contar_registros(ruta):
    """Lee el archivo completo. Si el gzip esta cortado o hay JSON roto, BulkInvalido."""
    try:
        return sum(1 for _ in leer_cartas(ruta))
    except (OSError, EOFError, ValueError, UnicodeDecodeError) as e:
        raise BulkInvalido(f"{ruta.name}: {e}") from e


def leer_meta_bulk(carpeta):
    try:
        return json.loads((carpeta / NOMBRE_META_BULK).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def bulk_valido(carpeta, updated_at=None):
    """Meta del bulk de esta carpeta si esta completo (y, si se pide, si es esa actualizacion); si no, None.
    El .meta.json solo se escribe despues de una descarga completa y validada."""
    meta = leer_meta_bulk(carpeta)
    bulk = carpeta / NOMBRE_BULK
    if not meta or not bulk.is_file() or not meta.get("registros"):
        return None
    if bulk.stat().st_size != meta.get("bytes"):
        return None
    if updated_at and meta.get("updated_at") != updated_at:
        return None
    return meta


def descargar_bulk(info, carpeta):
    """Descarga a un archivo provisional, comprueba bytes y contenido, y solo entonces reemplaza al anterior."""
    url = info.get("jsonl_download_uri")
    if not url:
        raise BulkInvalido("Scryfall no dio jsonl_download_uri")
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / NOMBRE_BULK
    temporal = temporal_de(destino)
    descargado = 0
    try:
        with requests.get(url, headers=HEADERS, stream=True, timeout=60) as respuesta:
            respuesta.raise_for_status()
            esperado = respuesta.headers.get("Content-Length")
            with open(temporal, "wb") as f:
                while trozo := respuesta.raw.read(1024 * 1024):  # bytes tal cual llegan (.gz sin tocar)
                    f.write(trozo)
                    descargado += len(trozo)
                    print(f"\r  Descargados {descargado / 1_000_000:.0f} MB", end="", flush=True)
        print()
        if esperado and descargado != int(esperado):
            raise BulkInvalido(f"descarga incompleta: {descargado:,} de {int(esperado):,} bytes")
        print("  Validando el archivo descargado (se lee completo) ...")
        registros = contar_registros(temporal)
        if registros == 0:
            raise BulkInvalido("el archivo descargado no tiene registros")
    except BaseException:
        temporal.unlink(missing_ok=True)
        raise
    (carpeta / NOMBRE_META_BULK).unlink(missing_ok=True)     # el bulk anterior deja de contar como valido
    temporal.replace(destino)
    meta = {
        "updated_at": info["updated_at"],
        "fecha_bulk": fecha_de_bulk(info["updated_at"]),
        "download_uri": url,
        "bytes": descargado,
        "registros": registros,
        "descargado_local": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    (carpeta / NOMBRE_META_BULK).write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  OK: {registros:,} registros, {descargado:,} bytes")
    return meta


def asegurar_bulk(info=None):
    """Deja en data/precios/scryfall/ el bulk vigente aunque su dia ya este en el historial. Hareruya y StarCityGames lo
    leen para conocer TODAS las impresiones de cada carta (tambien las de menos de $2): sin el, una version barata no
    puede competir y el emparejado sale incompleto. -> ruta del bulk valido, o None si no se pudo."""
    info = info or consultar_scryfall()
    if info and bulk_valido(CARPETA_SCRYFALL, info["updated_at"]):
        return CARPETA_SCRYFALL / NOMBRE_BULK
    if info:
        try:
            print("Descargando el bulk (lo necesitan las tiendas para emparejar) ...")
            descargar_bulk(info, CARPETA_SCRYFALL)
            return CARPETA_SCRYFALL / NOMBRE_BULK
        except (requests.RequestException, OSError, BulkInvalido) as e:
            print(f"\nAVISO: la descarga del bulk fallo ({e}).")
    if bulk_valido(CARPETA_SCRYFALL):
        print("AVISO: se usa el ultimo bulk valido que ya estaba descargado.")
        return CARPETA_SCRYFALL / NOMBRE_BULK
    print("AVISO: no hay bulk de Scryfall en disco: las tiendas emparejaran sin el indice completo.")
    return None


def obtener_bulk():
    """(ruta del bulk, meta del bulk, como se obtuvo) o None si no hay nada nuevo que cargar. Se detiene si no hay
    ningun bulk valido."""
    info = consultar_scryfall()
    if info:
        fecha = fecha_de_bulk(info["updated_at"])
        print(f"Scryfall: '{info.get('name', TIPO_BULK)}' actualizado {info['updated_at']} -> dia {fecha}")
        if dia_cargado(fecha, info["updated_at"]):
            print(f"El dia {fecha} ya esta en el historial ({RUTA_HISTORIAL}). No hay precios nuevos que cargar.")
            asegurar_bulk(info)          # una corrida repetida (reintento de Hareruya) igual necesita el bulk en disco
            return None
        meta = bulk_valido(CARPETA_SCRYFALL, info["updated_at"])
        if meta:
            return CARPETA_SCRYFALL / NOMBRE_BULK, meta, "ya estaba descargado y es valido (no se descargo otra vez)"
        try:
            print("Descargando el bulk ...")
            return CARPETA_SCRYFALL / NOMBRE_BULK, descargar_bulk(info, CARPETA_SCRYFALL), "descargado en esta ejecucion"
        except (requests.RequestException, OSError, BulkInvalido) as e:
            print(f"\nAVISO: la descarga fallo ({e}).")
    meta = bulk_valido(CARPETA_SCRYFALL)
    if meta:
        fecha = fecha_de_bulk(meta["updated_at"])
        if dia_cargado(fecha, meta["updated_at"]):
            print(f"AVISO: no hay bulk nuevo; el ultimo valido ({fecha}) ya esta en el historial.")
            return None
        print(f"AVISO: uso el bulk valido de {CARPETA_SCRYFALL} (actualizado {meta['updated_at']}).")
        return CARPETA_SCRYFALL / NOMBRE_BULK, meta, "ultimo bulk valido local (Scryfall no respondio o la descarga fallo)"
    sys.exit("No hay un bulk valido: Scryfall no respondio o la descarga fallo, y no hay ninguno completo "
             f"en {CARPETA_SCRYFALL}. No se genero nada. Revisa tu conexion y vuelve a correr.")


# ---------------------------------------------------------------- filtro de precio
def a_precio(valor):
    """Precio USD como numero, o None si falta o no es un numero valido."""
    try:
        precio = float(valor)
    except (TypeError, ValueError):
        return None
    return precio if math.isfinite(precio) and precio >= 0 else None


def es_tratamiento_foil(promo):
    return "foil" in promo or promo in TRATAMIENTOS_FOIL_EXTRA


def tratamiento_de(carta, acabado):
    """Tratamiento foil de esta version ('surgefoil', 'galaxyfoil', ...), o None si es foil normal.
    Solo aplica a la fila foil: en una impresion nonfoil + surge foil, la fila nonfoil no es surge."""
    if acabado != "foil":
        return None
    tratamientos = [p for p in (carta.get("promo_types") or []) if es_tratamiento_foil(p)]
    return ", ".join(tratamientos) or None


def versiones_de_carta(carta):
    """[(acabado, precio)] de cada acabado de esta impresion cuyo precio llega al umbral."""
    precios = carta.get("prices") or {}
    filas = []
    for campo, acabado in CAMPOS_PRECIO.items():
        precio = a_precio(precios.get(campo))
        if precio is not None and precio >= UMBRAL_USD:
            filas.append((acabado, precio))
    return filas


def imagen_de(carta):
    uris = carta.get("image_uris") or ((carta.get("card_faces") or [{}])[0].get("image_uris")) or {}
    return uris.get("normal")


def legalidad_de(carta):
    lg = carta.get("legalities") or {}
    return "".join(CODIGO_LEGAL.get(lg.get(f), "N") for f in FORMATOS)


def datos_de_carta(carta):
    """Lo que se guarda de cada impresion en la tabla cartas."""
    return {"id": carta.get("id"), "oracle_id": carta.get("oracle_id"), "name": (carta.get("name") or "").strip(),
            "set_code": carta.get("set"), "set_name": carta.get("set_name"), "collector_number": carta.get("collector_number"),
            "rarity": carta.get("rarity"), "lang": carta.get("lang"), "released_at": carta.get("released_at"),
            "scryfall_uri": carta.get("scryfall_uri"), "promo_types": ", ".join(carta.get("promo_types") or []) or None,
            "tratamiento_foil": tratamiento_de(carta, "foil"), "imagen": imagen_de(carta), "legal": legalidad_de(carta),
            "digital": int(bool(carta.get("digital")))}


def procesar(ruta):
    """Lee TODO el bulk una vez. -> (cartas {id: datos}, versiones [(id, acabado, precio)], conteos, sets, sin_precio).
    sin_precio = [(id, acabado)] de versiones que existen (Scryfall las lista en 'finishes') pero HOY no traen precio:
    Stack muestra para ellas el ultimo precio conocido, marcado como del dia anterior."""
    cartas, versiones, c, sets, sin_precio = {}, [], Counter(), {}, []
    try:
        for carta in leer_cartas(ruta):
            c["leidas"] += 1
            digital = bool(carta.get("digital"))
            c["digitales"] += digital
            if not digital and carta.get("set"):         # estadisticas por set para los formatos (todas las cartas)
                s = sets.get(carta["set"])
                if s is None:
                    s = sets[carta["set"]] = {"tipo": carta.get("set_type") or "", "fecha": carta.get("released_at") or "",
                                              "nombre": carta.get("set_name") or carta["set"], "n": 0,
                                              "si": dict.fromkeys(FORMATO_POR_SET, 0)}
                s["n"] += 1
                legal = legalidad_de(carta)
                for f in FORMATO_POR_SET:
                    if legal[FORMATOS.index(f)] != "N":
                        s["si"][f] += 1
            precios = carta.get("prices") or {}
            acabados = set(carta.get("finishes") or [])
            for campo, acabado in CAMPOS_PRECIO.items():
                if acabado in acabados and a_precio(precios.get(campo)) is None:
                    sin_precio.append((carta.get("id"), acabado))
            validos = {campo: a_precio(precios.get(campo)) for campo in CAMPOS_PRECIO}
            validos = {campo: p for campo, p in validos.items() if p is not None}
            for campo, precio in validos.items():
                c[f"validos_{campo}"] += 1
                c[f"pasan_{campo}"] += precio >= UMBRAL_USD
            if not validos:
                c["sin_precio"] += 1
                continue
            c["con_precio"] += 1
            nuevas = versiones_de_carta(carta)
            if nuevas:
                c["pasan"] += 1
                c["digitales_que_pasan"] += digital
                cartas[carta["id"]] = datos_de_carta(carta)
                versiones += [(carta["id"], acabado, precio) for acabado, precio in nuevas]
            else:
                c["bajo_umbral"] += 1
    except (OSError, EOFError, ValueError, UnicodeDecodeError) as e:
        raise BulkInvalido(f"error leyendo {ruta.name} en el registro {c['leidas'] + 1:,}: {e}") from e
    return cartas, versiones, c, sets, sin_precio


def sets_por_formato(sets):
    """{formato: [(SET, nombre, fecha)]}: set del tipo y fecha que permite la regla y con >= 90% de cartas legales ahi."""
    salida = {}
    for f, (desde, hasta, tipos) in FORMATO_POR_SET.items():
        salida[f] = sorted(((cod.upper(), s["nombre"], s["fecha"]) for cod, s in sets.items()
                            if s["tipo"] in tipos and (not desde or s["fecha"] >= desde) and (not hasta or s["fecha"] <= hasta)
                            and s["n"] and s["si"][f] / s["n"] >= FRACCION_SET_FORMATO), key=lambda x: x[2])
    return salida


def lista_de_busqueda(cartas, versiones):
    """Un renglon por nombre (sin distinguir mayusculas): las tiendas buscan el NOMBRE una vez, no cada version.
    'buscar_como' es el nombre que se teclea en las tiendas: la cara frontal si es de doble cara ('A // B' -> 'A')."""
    grupos = {}
    for id_, acabado, precio in sorted(versiones, key=lambda v: (cartas[v[0]]["name"], cartas[v[0]]["released_at"] or "",
                                                              cartas[v[0]]["set_code"] or "", cartas[v[0]]["collector_number"] or "", v[1])):
        nombre = cartas[id_]["name"]
        g = grupos.setdefault(nombre.casefold(), {"name": nombre, "ids": set(), "versiones": 0, "min": precio, "max": precio,
                                                  "normal": False, "foil_o_etched": False})
        g["ids"].add(id_)
        g["versiones"] += 1
        g["min"], g["max"] = min(g["min"], precio), max(g["max"], precio)
        g["normal"] |= acabado == "nonfoil"
        g["foil_o_etched"] |= acabado in ("foil", "etched")
    filas = [(g["name"], g["name"].split(" // ")[0].strip(), len(g["ids"]), g["versiones"], g["min"], g["max"],
              int(g["normal"]), int(g["foil_o_etched"])) for g in grupos.values()]
    return sorted(filas, key=lambda f: f[0].casefold())


def verificar(cartas, versiones, c, meta_bulk):
    """Comprueba que no se perdio nada y que todo cumple el umbral. Devuelve la lista de errores."""
    errores = []
    if c["leidas"] != meta_bulk.get("registros"):
        errores.append(f"se leyeron {c['leidas']:,} registros pero la descarga tenia {meta_bulk.get('registros')}")
    if c["con_precio"] + c["sin_precio"] != c["leidas"]:
        errores.append("con precio + sin precio no suman los registros leidos")
    if c["pasan"] + c["bajo_umbral"] != c["con_precio"]:
        errores.append("cumplen + bajo umbral no suman los registros con precio")
    if any(p < UMBRAL_USD for _, _, p in versiones):
        errores.append("hay versiones con precio < umbral")
    if len({(i, a) for i, a, _ in versiones}) != len(versiones):
        errores.append("hay versiones (id + acabado) repetidas")
    if len(cartas) != c["pasan"]:
        errores.append(f"impresiones en el catalogo ({len(cartas):,}) != impresiones que cumplen ({c['pasan']:,})")
    if len(versiones) != sum(c[f"pasan_{campo}"] for campo in CAMPOS_PRECIO):
        errores.append("versiones != suma de acabados que cumplen")
    return errores


# ---------------------------------------------------------------- historial.sqlite
DDL_HISTORIAL = """
CREATE TABLE IF NOT EXISTS dias (            -- un dia SOLO aparece aqui si entro completo
    dia INTEGER PRIMARY KEY, fecha TEXT NOT NULL UNIQUE, updated_at TEXT, registros INTEGER, versiones INTEGER,
    impresiones INTEGER, nombres INTEGER, cargado_utc TEXT, origen TEXT);
CREATE TABLE IF NOT EXISTS cartas (          -- cada impresion que alguna vez valio >= umbral (datos del dia mas reciente)
    n INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE, oracle_id TEXT, name TEXT NOT NULL, set_code TEXT, set_name TEXT,
    collector_number TEXT, rarity TEXT, lang TEXT, released_at TEXT, scryfall_uri TEXT, promo_types TEXT,
    tratamiento_foil TEXT, imagen TEXT, legal TEXT, digital INTEGER, actualizado INTEGER);
CREATE INDEX IF NOT EXISTS idx_cartas_nombre ON cartas(name COLLATE NOCASE);
CREATE TABLE IF NOT EXISTS precios (         -- precio de cada version >= umbral por dia (acabado 0 normal, 1 foil, 2 etched)
    dia INTEGER NOT NULL, n INTEGER NOT NULL, acabado INTEGER NOT NULL, centavos INTEGER NOT NULL,
    PRIMARY KEY (dia, n, acabado)) WITHOUT ROWID;   -- ordenado por dia: sin indices extra (es la tabla que crece)
CREATE TABLE IF NOT EXISTS search_list (     -- nombres unicos a buscar (del dia mas reciente)
    name TEXT PRIMARY KEY, buscar_como TEXT NOT NULL, impresiones INTEGER, versiones INTEGER,
    precio_min_usd REAL, precio_max_usd REAL, tiene_normal INTEGER, tiene_foil_o_etched INTEGER);
CREATE TABLE IF NOT EXISTS sets_formato (    -- sets que acepta cada formato que depende del set (del dia mas reciente)
    formato TEXT NOT NULL, set_code TEXT NOT NULL, set_name TEXT, fecha_set TEXT, PRIMARY KEY (formato, set_code));
CREATE TABLE IF NOT EXISTS entradas (        -- versiones que entraron a >= umbral y se mantuvieron ahi
    n INTEGER NOT NULL, acabado INTEGER NOT NULL, dia_entrada INTEGER NOT NULL, centavos_entrada INTEGER,
    dia_confirmada INTEGER, motivo TEXT, estado TEXT NOT NULL, PRIMARY KEY (n, acabado, dia_entrada));
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS sin_precio (      -- versiones conocidas (alguna vez >= umbral) que ese dia Scryfall dio SIN precio
    dia INTEGER NOT NULL, n INTEGER NOT NULL, acabado INTEGER NOT NULL, PRIMARY KEY (dia, n, acabado)) WITHOUT ROWID;
DROP VIEW IF EXISTS cards;
CREATE VIEW cards AS                         -- el catalogo del dia mas reciente, con las columnas del antiguo cards_over_2usd
    SELECT c.n * 4 + p.acabado AS rowid, c.name, c.set_code, c.set_name, c.collector_number,
           CASE p.acabado WHEN 0 THEN 'nonfoil' WHEN 1 THEN 'foil' ELSE 'etched' END AS finish,
           p.centavos / 100.0 AS price_usd, c.rarity, c.lang, c.released_at, c.id, c.oracle_id, c.scryfall_uri,
           CASE WHEN p.acabado = 1 THEN c.tratamiento_foil END AS tratamiento, c.promo_types
    FROM cartas c CROSS JOIN precios p           -- CROSS JOIN: primero la carta (por nombre usa su indice), luego su precio
    WHERE p.n = c.n AND p.dia = (SELECT MAX(dia) FROM dias);
"""


def abrir_historial(ruta=None):
    ruta = Path(ruta or RUTA_HISTORIAL)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(ruta, timeout=30, isolation_level=None)     # transacciones a mano (BEGIN / COMMIT)
    con.executescript(DDL_HISTORIAL)
    return con


def dia_cargado(fecha, updated_at=None, ruta=None):
    """True si ese dia ya entro al historial (y, si se da, con esa misma actualizacion de Scryfall)."""
    ruta = Path(ruta or RUTA_HISTORIAL)
    if not ruta.exists():
        return False
    try:
        con = sqlite3.connect(f"{ruta.resolve().as_uri()}?mode=ro", uri=True)
        try:
            r = con.execute("SELECT updated_at FROM dias WHERE fecha = ?", (fecha,)).fetchone()
        finally:
            con.close()
    except sqlite3.Error:
        return False
    return r is not None and (updated_at is None or r[0] == updated_at)


def recalcular_entradas(con, desde=None):
    """Entradas a la lista >= umbral. Una version 'entra' el dia que aparece y en la captura anterior no estaba; queda
    confirmada si sigue en TODAS las capturas de los DIAS_CONFIRMAR_ENTRADA dias desde que entra (si baja antes, no se
    registra). Con datos todavia insuficientes queda 'pendiente'. desde = dia (int): solo se recalculan entradas desde
    ese dia (las anteriores ya estan decididas)."""
    dias = [d for (d,) in con.execute("SELECT dia FROM dias ORDER BY dia")]
    if len(dias) < 2:
        con.execute("DELETE FROM entradas")
        return 0
    fecha = lambda d: date(d // 10000, d // 100 % 100, d % 100)
    if desde is None:
        desde = dias[1]
    i0 = max(1, next((k for k, d in enumerate(dias) if d >= desde), len(dias)))
    previo = dias[i0 - 1]
    con.execute("DELETE FROM entradas WHERE dia_entrada >= ?", (dias[i0] if i0 < len(dias) else 99999999,))
    presencia = {}
    for n, a, d, cent in con.execute("SELECT n, acabado, dia, centavos FROM precios WHERE dia >= ? ORDER BY n, acabado, dia", (previo,)):
        presencia.setdefault((n, a), {})[d] = cent
    lanzamiento = dict(con.execute("SELECT n, released_at FROM cartas"))
    filas = []
    for (n, a), serie in presencia.items():
        for k in range(i0, len(dias)):
            d = dias[k]
            if d not in serie or dias[k - 1] in serie:
                continue
            # ventana: las capturas desde que entra hasta la PRIMERA captura con fecha >= entrada + 2 dias (si falta la captura
            # de ese dia, cuenta la siguiente: tiene que verse que SIGUE >= umbral al menos 2 dias despues de entrar)
            fin = fecha(d) + timedelta(days=DIAS_CONFIRMAR_ENTRADA - 1)
            antes = [x for x in dias[k:] if fecha(x) < fin]
            cierre = next((x for x in dias[k:] if fecha(x) >= fin), None)
            ventana = antes + ([cierre] if cierre else [])
            if not all(x in serie for x in ventana):
                continue                                  # bajo de nuevo antes de 3 dias: no cuenta
            confirmada = cierre is not None
            rel = lanzamiento.get(n)
            nueva = bool(rel) and rel >= (fecha(d) - timedelta(days=DIAS_IMPRESION_NUEVA)).isoformat()
            filas.append((n, a, d, serie[d], ventana[-1] if confirmada else None,
                          "impresion_nueva" if nueva else "subio", "confirmada" if confirmada else "pendiente"))
    con.executemany("INSERT INTO entradas VALUES (?, ?, ?, ?, ?, ?, ?)", filas)
    return len(filas)


def agregar_dia(con, ruta_bulk, meta_bulk, origen="", avisar=print):
    """Lee el bulk y agrega su dia al historial (una transaccion). -> (texto de validacion, errores)."""
    fecha = fecha_de_bulk(meta_bulk["updated_at"])
    d = dia_num(fecha)
    avisar(f"Leyendo {ruta_bulk} ...")
    cartas, versiones, c, sets, sin_precio = procesar(ruta_bulk)
    if not versiones:
        raise BulkInvalido("ninguna impresion cumplio el corte de precio; eso no es normal")
    errores = verificar(cartas, versiones, c, meta_bulk)
    if errores:
        return reporte(fecha, c, cartas, versiones, [], meta_bulk, origen, ruta_bulk, errores), errores
    lista = lista_de_busqueda(cartas, versiones)
    es_ultimo = (con.execute("SELECT MAX(dia) FROM dias").fetchone()[0] or 0) <= d
    try:
        con.execute("BEGIN")
        con.execute("DELETE FROM precios WHERE dia = ?", (d,))
        con.execute("DELETE FROM sin_precio WHERE dia = ?", (d,))
        con.execute("DELETE FROM dias WHERE dia = ?", (d,))
        cols = ["id", "oracle_id", "name", "set_code", "set_name", "collector_number", "rarity", "lang", "released_at",
                "scryfall_uri", "promo_types", "tratamiento_foil", "imagen", "legal", "digital"]
        # los datos de la carta se actualizan solo si este dia es el mas reciente que tiene esa carta
        con.executemany(
            f"INSERT INTO cartas ({', '.join(cols)}, actualizado) VALUES ({', '.join('?' * len(cols))}, ?) "
            f"ON CONFLICT(id) DO UPDATE SET {', '.join(f'{k} = excluded.{k}' for k in cols[1:])}, actualizado = excluded.actualizado "
            "WHERE excluded.actualizado >= cartas.actualizado",
            [[cartas[i][k] for k in cols] + [d] for i in cartas])
        n_de = dict(con.execute("SELECT id, n FROM cartas"))
        con.executemany("INSERT INTO precios (dia, n, acabado, centavos) VALUES (?, ?, ?, ?)",
                        sorted((d, n_de[i], ACABADOS[a], int(round(p * 100))) for i, a, p in versiones))
        con.executemany("INSERT OR IGNORE INTO sin_precio (dia, n, acabado) VALUES (?, ?, ?)",     # solo las que ya conocemos
                        sorted({(d, n_de[i], ACABADOS[a]) for i, a in sin_precio if i in n_de}))
        if es_ultimo:
            con.execute("DELETE FROM search_list")
            con.executemany("INSERT INTO search_list VALUES (?, ?, ?, ?, ?, ?, ?, ?)", lista)
            con.execute("DELETE FROM sets_formato")
            con.executemany("INSERT INTO sets_formato VALUES (?, ?, ?, ?)",
                            [(f, cod, nombre, fs) for f, ls in sets_por_formato(sets).items() for cod, nombre, fs in ls])
            con.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                            [("bulk_updated_at", meta_bulk["updated_at"]), ("umbral_usd", str(UMBRAL_USD)),
                             ("ultimo_dia", fecha), ("formatos", ",".join(FORMATOS))])
        con.execute("INSERT INTO dias VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (d, fecha, meta_bulk["updated_at"], c["leidas"], len(versiones), len(cartas), len(lista),
                     datetime.now(timezone.utc).isoformat(timespec="seconds"), origen))
        # verificacion dentro de la transaccion: si no cuadra, el dia no entra
        n_p, minimo = con.execute("SELECT COUNT(*), MIN(centavos) FROM precios WHERE dia = ?", (d,)).fetchone()
        if n_p != len(versiones):
            errores.append(f"el historial tiene {n_p:,} precios del dia; se esperaban {len(versiones):,}")
        if minimo is not None and minimo < UMBRAL_USD * 100:
            errores.append(f"el historial tiene precios < umbral (minimo {minimo} centavos)")
        if errores:
            raise RuntimeError("; ".join(errores))
        pendiente = con.execute("SELECT MIN(dia_entrada) FROM entradas WHERE estado = 'pendiente'").fetchone()[0]
        recalcular_entradas(con, desde=min(d, pendiente or d))     # tambien se deciden las que estaban pendientes
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    nuevas = con.execute("SELECT COUNT(*) FROM entradas WHERE dia_entrada = ?", (d,)).fetchone()[0]
    return reporte(fecha, c, cartas, versiones, lista, meta_bulk, origen, ruta_bulk, errores, nuevas), errores


# ---------------------------------------------------------------- reporte
def reporte(fecha, c, cartas, versiones, lista, meta_bulk, como, ruta_bulk, errores, nuevas=0):
    u = f"${UMBRAL_USD:.2f}"
    lineas = [
        "Validacion del dia (catalogo de referencia Scryfall)",
        "",
        f"Dia (fecha del bulk, UTC):                {fecha}",
        f"Fecha de ejecucion (tu PC):               {datetime.now().astimezone().isoformat(timespec='seconds')}",
        f"Actualizacion del bulk (Scryfall, UTC):   {meta_bulk['updated_at']}",
        f"Bulk:                                     {como}",
        f"Archivo de entrada:                       {ruta_bulk}",
        f"Historial:                                {RUTA_HISTORIAL}",
        "",
        f"Registros leidos del bulk:                {c['leidas']:>9,}   (registros de la descarga: {meta_bulk.get('registros', 0):,})",
        f"Con al menos un precio USD valido:        {c['con_precio']:>9,}",
        f"Cumplen el corte (alguna version >= {u}): {c['pasan']:>8,}",
        f"Excluidos por no tener precio USD valido: {c['sin_precio']:>9,}",
        f"Excluidos por precio inferior a {u}:     {c['bajo_umbral']:>9,}",
        "",
        "Por acabado (precio valido -> cumplen):",
    ]
    for campo, acabado in CAMPOS_PRECIO.items():
        lineas.append(f"  {campo:<11} ({acabado:<7}) {c[f'validos_{campo}']:>9,} -> {c[f'pasan_{campo}']:>7,}")
    lineas += [
        "",
        f"Versiones agregadas al historial:          {len(versiones):,}",
        f"Nombres unicos en search_list:             {len(lista):,}",
        f"Entradas nuevas a >= {u} (dia de entrada hoy, pendientes de confirmar 3 dias): {nuevas:,}",
        f"Impresiones solo digitales leidas: {c['digitales']:,} (cumplen el corte: {c['digitales_que_pasan']:,}).",
        "",
        "Verificaciones: " + ("OK (todas las versiones >= umbral, sin repetidos, los conteos cuadran)"
                              if not errores else "FALLARON (el dia NO entro al historial)"),
        *[f"  - {e}" for e in errores],
    ]
    return "\n".join(lineas)


def guardar_validacion(fecha, texto):
    CARPETA_VALIDACION.mkdir(parents=True, exist_ok=True)
    ruta = CARPETA_VALIDACION / f"{fecha}.txt"
    ruta.write_text(texto + "\n", encoding="utf-8")
    return ruta



# ====================================================================================================
# REVISION DE PRECIOS (antes revisar_precios.py)
# Revisa los precios "ridiculos" del dia: versiones que de un dia para otro saltaron (o se desplomaron) contra su historial.
# 
# Por que pasa: el archivo diario de Scryfall trae el precio "market" de TCGplayer de ese momento; a veces una venta rara o un
# dato viejo lo dispara (de $10 a $100) y al dia siguiente regresa. Aqui se detectan y se vuelven a consultar una por una
# en la API de Scryfall (que da el dato mas fresco):
# 
#   - Sospechosa: el precio de hoy es >= 3 veces (o <= 1/3) la mediana de sus ultimos 7 dias con datos, y la diferencia es de
#     al menos $5. Tambien una carta que antes valia < $2 y hoy aparece en >= $15.
#   - La API dice algo cercano a su mediana  -> CORREGIDO: el precio de hoy se reemplaza por el de la API.
#   - La API confirma el salto              -> CONFIRMADO: se deja, pero Stack lo marca con ⚠ (revisa antes de pagar).
#   - La API da otro valor                  -> se usa el de la API; si sigue siendo un salto, queda marcado.
#   - Sin respuesta de la API               -> se deja y se marca.
# Todo queda en la tabla revision_precios de historial.sqlite (el precio original tambien), y en
# data/precios/validacion/<fecha>_revision.txt.
# ====================================================================================================
FACTOR = 3.0              # cuantas veces arriba (o abajo) de la mediana se considera salto
MIN_DIF_USD = 5.0         # y con al menos esta diferencia en dolares (un salto de $2 a $7 no importa tanto)
NUEVA_MIN_USD = 15.0      # una version que antes valia < $2 y hoy aparece en >= esto tambien se revisa
DIAS_REF = 7              # mediana de los ultimos N dias con datos
MIN_DIAS = 3              # con menos dias de historial no se juzga
CERCA = 0.25              # la API "coincide con la mediana" si queda a menos de 25%
MAX_CONSULTAS = 400       # tope por dia (Scryfall pide no mas de ~10 consultas por segundo)
PAUSA = 0.12
URL_CARTA = "https://api.scryfall.com/cards/{}"
CAMPO = {0: "usd", 1: "usd_foil", 2: "usd_etched"}

DDL_REVISION = """CREATE TABLE IF NOT EXISTS revision_precios (
    dia INTEGER NOT NULL, n INTEGER NOT NULL, acabado INTEGER NOT NULL,
    centavos_bulk INTEGER, centavos_api INTEGER, mediana_centavos INTEGER, dias_ref INTEGER,
    resultado TEXT NOT NULL, revisado_utc TEXT, PRIMARY KEY (dia, n, acabado)) WITHOUT ROWID"""


def sospechosos(con, d):
    import statistics
    """[(n, acabado, centavos_hoy, mediana_centavos o None, dias_ref)] del dia d, del salto mas grande al mas chico."""
    previos = [x for (x,) in con.execute("SELECT dia FROM dias WHERE dia < ? ORDER BY dia DESC LIMIT ?", (d, DIAS_REF))]
    if len(previos) < MIN_DIAS:
        return []
    hist = {}
    marcas = ",".join("?" * len(previos))
    for n, a, c in con.execute(f"SELECT n, acabado, centavos FROM precios WHERE dia IN ({marcas})", previos):
        hist.setdefault((n, a), []).append(c)
    salida = []
    for n, a, c in con.execute("SELECT n, acabado, centavos FROM precios WHERE dia = ?", (d,)):
        serie = hist.get((n, a))
        if serie:
            # dias sin dato = estaba debajo de $2: cuentan como $2 para que la mediana no se infle
            completa = serie + [int(UMBRAL_USD * 100)] * (len(previos) - len(serie))
            m = statistics.median(completa)
            r = c / m if m else 1
            if (r >= FACTOR or r <= 1 / FACTOR) and abs(c - m) >= MIN_DIF_USD * 100:
                salida.append((n, a, c, int(m), len(previos)))
        elif c >= NUEVA_MIN_USD * 100:
            salida.append((n, a, c, None, len(previos)))
    salida.sort(key=lambda x: -abs(x[2] - (x[3] or UMBRAL_USD * 100)))
    return salida


def de_disputas(con, d):
    """Cartas en las que importar_mtgjson.py encontro que Scryfall y MTGJSON no coinciden. Si hoy Scryfall sigue con el precio
    equivocado: 'provisional_mtgjson' (se usa el ultimo precio de MTGJSON) o 'en_disputa' (solo se marca).
    -> [(n, acabado, centavos_hoy, centavos_mtgjson, resultado)]"""
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name = 'disputas_fuente'").fetchone():
        return []
    salida = []
    for n, a, mtg, decision, hoy in con.execute(
            """SELECT r.n, r.acabado, r.centavos_mtgjson, r.decision, p.centavos FROM disputas_fuente r
               JOIN precios p ON p.n = r.n AND p.acabado = r.acabado AND p.dia = ? WHERE r.decision IN ('mtgjson', 'disputa')""", (d,)):
        if not mtg:
            continue
        r = hoy / mtg
        if r >= 2 or r <= 0.5:                        # sigue sin coincidir
            salida.append((n, a, hoy, mtg, "provisional_mtgjson" if decision == "mtgjson" else "en_disputa"))
    return salida


def precio_api(sesion, id_, acabado):
    """Precio actual de la API de Scryfall (USD) o None. Tambien None si la API no responde."""
    try:
        r = sesion.get(URL_CARTA.format(id_), timeout=20)
        if r.status_code == 429:                     # demasiadas consultas: se espera y se reintenta una vez
            time.sleep(2)
            r = sesion.get(URL_CARTA.format(id_), timeout=20)
        r.raise_for_status()
        return a_precio((r.json().get("prices") or {}).get(CAMPO[acabado]))
    except (requests.RequestException, ValueError):
        return None


def decidir(c_bulk, mediana, c_api):
    if c_api is None:
        return "sin_dato_api"
    ref = mediana if mediana is not None else UMBRAL_USD * 100
    if abs(c_api - ref) <= CERCA * ref:
        return "corregido"
    if abs(c_api - c_bulk) <= 0.10 * c_bulk:
        return "confirmado"
    r = c_api / ref if ref else 1
    return "corregido_aun_dudoso" if (r >= FACTOR or r <= 1 / FACTOR) else "corregido"


def revisar_dia(con, fecha=None, red=True, avisar=print, solo_disputas=False):
    """Detecta y revisa los sospechosos del dia (el mas reciente si no se da). -> texto del reporte."""
    con.execute(DDL_REVISION)
    d = dia_num(fecha) if fecha else con.execute("SELECT MAX(dia) FROM dias").fetchone()[0]
    if d is None:
        return "Sin dias en el historial."
    disp = de_disputas(con, d)
    en_disp = {(n, a) for n, a, *_ in disp}
    lista = [] if solo_disputas else [x for x in sospechosos(con, d) if (x[0], x[1]) not in en_disp][:MAX_CONSULTAS]
    ids = dict(con.execute("SELECT n, id FROM cartas"))
    nombres = dict(con.execute("SELECT n, name || ' (' || UPPER(set_code) || ' #' || collector_number || ')' FROM cartas"))
    filas, ahora = [], datetime.now(timezone.utc).isoformat(timespec="seconds")
    sesion = requests.Session()
    sesion.headers.update(HEADERS)
    if lista:
        avisar(f"Revisando {len(lista)} precios dudosos del {dia_txt(d)}" + (" en la API de Scryfall ..." if red else " (sin red) ..."))
    for k, (n, a, c, m, nd) in enumerate(lista, start=1):
        usd = precio_api(sesion, ids[n], a) if red else None
        c_api = None if usd is None else int(round(usd * 100))
        filas.append((d, n, a, c, c_api, m, nd, decidir(c, m, c_api) if red else "sin_revisar", ahora))
        if red:
            time.sleep(PAUSA)
    for n, a, hoy, mtg, res in disp:                 # (dias_ref = None: la referencia es MTGJSON, no la mediana)
        filas.append((d, n, a, hoy, mtg, mtg, None, res, ahora))
    try:
        con.execute("BEGIN")
        # se conserva lo revisado antes ese mismo dia (un precio ya corregido ya no se ve sospechoso, pero su registro queda);
        # las marcas por disputa de fuentes si se rehacen (la decision puede cambiar al volver a importar MTGJSON)
        con.execute("DELETE FROM revision_precios WHERE dia = ? AND resultado = 'en_disputa'", (d,))
        con.executemany("INSERT OR REPLACE INTO revision_precios VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", filas)
        for (_, n, a, c, c_api, m, nd, res, _) in filas:
            if res == "provisional_mtgjson":
                con.execute("CREATE TABLE IF NOT EXISTS correcciones_fuente (dia INTEGER NOT NULL, n INTEGER NOT NULL, acabado INTEGER NOT NULL, "
                            "centavos_antes INTEGER, centavos_despues INTEGER, corregido TEXT, PRIMARY KEY (dia, n, acabado)) WITHOUT ROWID")
                con.execute("INSERT OR REPLACE INTO correcciones_fuente VALUES (?, ?, ?, ?, ?, ?)", (d, n, a, c, c_api, "provisional " + ahora))
            if res.startswith("corregido") or res == "provisional_mtgjson":
                if c_api < UMBRAL_USD * 100:       # de verdad vale < $2: sale de la lista de hoy
                    con.execute("DELETE FROM precios WHERE dia = ? AND n = ? AND acabado = ?", (d, n, a))
                else:
                    con.execute("UPDATE precios SET centavos = ? WHERE dia = ? AND n = ? AND acabado = ?", (c_api, d, n, a))
        con.execute("UPDATE dias SET versiones = (SELECT COUNT(*) FROM precios WHERE dia = ?) WHERE dia = ?", (d, d))
        recalcular_entradas(con, desde=d)
        con.execute("INSERT OR REPLACE INTO meta VALUES ('revision_precios', ?)", (ahora,))
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    cuenta = {}
    for f in filas:
        cuenta[f[7]] = cuenta.get(f[7], 0) + 1
    usd = lambda x: "-" if x is None else f"${x / 100:,.2f}"
    lineas = [f"Revision de precios dudosos del {dia_txt(d)} (salto >= x{FACTOR:g} contra la mediana de {DIAS_REF} dias, "
              f"diferencia >= ${MIN_DIF_USD:g})", "", f"Revisadas: {len(filas)}  " + "  ".join(f"{k}: {v}" for k, v in sorted(cuenta.items())), ""]
    for (_, n, a, c, c_api, m, nd, res, _) in filas:
        lineas.append(f"  {res:22} {nombres.get(n, n)} {['normal', 'foil', 'etched'][a]}: archivo {usd(c)} | API {usd(c_api)} | "
                      f"mediana {usd(m) if m is not None else '< $2'}")
    texto = "\n".join(lineas)
    try:
        CARPETA_VALIDACION.mkdir(parents=True, exist_ok=True)
        (CARPETA_VALIDACION / f"{dia_txt(d)}_revision.txt").write_text(texto + "\n", encoding="utf-8")
    except OSError:
        pass
    return texto


def descargar():
    obtenido = obtener_bulk()
    if obtenido is None:
        return
    ruta_bulk, meta_bulk, como = obtenido
    con = abrir_historial()
    try:
        try:
            texto, errores = agregar_dia(con, ruta_bulk, meta_bulk, como)
            if not errores:                               # precios que saltaron de forma ridicula: se vuelven a consultar
                try:
                    texto += "\n\n" + revisar_dia(con, fecha_de_bulk(meta_bulk["updated_at"]))
                except Exception as e:                    # noqa: BLE001  (sin red o la API no responde: el dia ya entro)
                    texto += f"\n\nNo se pudo revisar los precios dudosos: {type(e).__name__}: {e}"
        except BulkInvalido as e:
            (ruta_bulk.parent / NOMBRE_META_BULK).unlink(missing_ok=True)   # la siguiente ejecucion lo vuelve a descargar
            sys.exit(f"El bulk esta danado ({e}). Lo marque como invalido; vuelve a correr para descargarlo. "
                     "No se agrego nada al historial.")
    finally:
        con.close()
    ruta = guardar_validacion(fecha_de_bulk(meta_bulk["updated_at"]), texto)
    print()
    print(texto)
    print(f"\nValidacion guardada en {ruta}")
    if errores:
        sys.exit("No se agrego el dia al historial; la verificacion fallo.")



def main(argv=None):
    import argparse
    argv = sys.argv[1:] if argv is None else list(argv)
    ap = argparse.ArgumentParser(prog="scryfall.py", description="Precios de Scryfall para Stack.")
    ap.add_argument("accion", nargs="?", default="descargar", choices=["descargar", "revisar", "bulk"],
                    help="bulk: solo deja el default-cards vigente en disco (para emparejar tiendas)")
    ap.add_argument("--dia", help="revisar: AAAA-MM-DD (por defecto el mas reciente)")
    ap.add_argument("--sin-red", action="store_true", help="revisar: solo detecta y marca, sin consultar la API")
    a = ap.parse_args(argv)
    if a.accion == "bulk":
        sys.exit(0 if asegurar_bulk() else 1)
    if a.accion == "revisar":
        con = abrir_historial()
        try:
            print(revisar_dia(con, a.dia, red=not a.sin_red))
        finally:
            con.close()
        return
    descargar()

if __name__ == "__main__":
    main()
