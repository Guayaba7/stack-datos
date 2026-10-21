"""
HARERUYA para Stack: descarga los precios de Hareruya y los empareja contra el catalogo de Scryfall. Sin ventanas ni
reportes: el avance y las inconsistencias se ven en Stack.

    python src/hareruya.py              descarga toda la search_list con 16 procesos (si quedo una a medias, la retoma)
    python src/hareruya.py --n 20       prueba rapida con 20 nombres al azar
    python src/hareruya.py detener      pide parar la descarga (termina la carta en curso y guarda)
    python src/hareruya.py emparejar    empareja lo descargado (Stack lo hace solo al abrirse)

Stack tambien puede iniciar y detener la descarga (Ctrl+T, ventana TIENDAS).

Secciones:  A scraper de Hareruya (sin cambios) | B reglas de emparejamiento (sin cambios) | C motor sin red |
D emparejado | E descarga | F linea de comandos.

Lee en SOLO LECTURA data/precios/historial.sqlite (catalogo >= $2 y search_list, lo genera scryfall.py) y el default-cards mas
reciente (data/precios/scryfall/: todas las impresiones, para nombres alternativos y variantes).
Escribe solo en data/tiendas/hareruya/:
    descargas/      precios tal como los publica Hareruya (yenes, idioma, condicion, stock), un archivo por descarga
    emparejado/     esos precios emparejados contra el catalogo del dia (y el indice del bulk); se pueden borrar
    progreso.json   avance de la descarga en curso (lo muestra Stack)
"""
import argparse
import contextlib
import difflib
import gzip
import hashlib
import io
import json
import multiprocessing
import os
import random
import re
import shutil
import signal
import sqlite3
import sys
import time
import traceback
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

for _flujo in (sys.stdout, sys.stderr):              # Windows: que un caracter raro (⚠, ¥, acentos) nunca tumbe el programa
    try:
        _flujo.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError, OSError):
        pass
from urllib.parse import quote

import requests

# ====================================================================================================
# A. SCRAPER DE HARERUYA (busqueda y paginacion; sin cambios)
# ====================================================================================================


URL_BUSQUEDA = "https://www.hareruyamtg.com/ja/products/search/unisearch_api"
URL_REFERER = "https://www.hareruyamtg.com/ja/products/search"
FILAS_POR_PAGINA = 60
MAX_PAGINAS = 50
PAUSA_SEGUNDOS = 1.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:157.0) Gecko/20100101 Firefox/157.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}



def a_entero(valor):
    """'400' -> 400. Si no hay valor o no es número -> None (nunca 0)."""
    try:
        return int(float(valor))
    except (TypeError, ValueError):
        return None


def crear_fila(doc, termino, capturado):
    nombre_en = (doc.get("product_name_en") or "").strip()
    set_code = re.search(r"\[([^\]]+)\]", nombre_en)  # "《Counterspell》 [MMQ]" -> MMQ
    card_name = doc.get("card_name") or ""
    return {
        "captured_at": capturado,
        "search_term": termino,
        "exact_match": card_name.strip().casefold() == termino.strip().casefold(),
        "product": doc.get("product"),
        "card_name": card_name,
        "product_name_en": nombre_en,
        "product_name": doc.get("product_name"),
        "set_code": set_code.group(1) if set_code else None,
        "language": doc.get("language"),             # código tal cual lo manda Hareruya
        "finish": "foil" if str(doc.get("foil_flg")) == "1" else "nonfoil",
        "condition": doc.get("card_condition"),      # código tal cual lo manda Hareruya
        "price_jpy": a_entero(doc.get("price")),
        "stock": a_entero(doc.get("stock")),
        "weekly_sales": a_entero(doc.get("weekly_sales")),
        "sale_flg": doc.get("sale_flg"),
    }


def buscar_carta(nombre):
    """Devuelve una lista de filas (diccionarios) con todos los productos de `nombre`."""
    capturado = datetime.now(timezone.utc).isoformat(timespec="seconds")
    referer = f"{URL_REFERER}?suggest_type=all&product={quote(nombre, safe='')}&category="
    filas = []
    total = None

    with requests.Session() as sesion:
        sesion.headers.update(HEADERS)
        for pagina in range(1, MAX_PAGINAS + 1):
            # URL armada a mano: igual que el navegador (con ~ y * literales)
            url = (f"{URL_BUSQUEDA}?kw={quote(nombre, safe='')}"
                   f"&fq.price=1~*&rows={FILAS_POR_PAGINA}&page={pagina}")

            try:  # 1) petición y código HTTP
                respuesta = sesion.get(url, headers={"Referer": referer}, timeout=30)
                respuesta.raise_for_status()
            except requests.RequestException as error:
                print(f"  [Error] Página {pagina}: {error}")
                cuerpo = getattr(getattr(error, "response", None), "text", "")
                if cuerpo:
                    print(f"  [Error] El servidor respondió: {cuerpo[:200]!r}")
                break

            try:  # 2) el cuerpo debe ser JSON con "docs"
                datos = respuesta.json()
                bloque = datos.get("response", datos)
                docs = bloque.get("docs", [])
                total = bloque.get("numFound", total)
            except (ValueError, AttributeError, TypeError):
                print(f"  [Error] La página {pagina} no devolvió JSON "
                      f"(Content-Type: {respuesta.headers.get('Content-Type')}). "
                      f"Inicio: {respuesta.text[:100]!r}")
                break

            if not docs:
                break
            filas.extend(crear_fila(doc, nombre, capturado) for doc in docs)
            if len(docs) < FILAS_POR_PAGINA or (total is not None and len(filas) >= total):
                break
            time.sleep(PAUSA_SEGUNDOS)

    return filas



# ====================================================================================================
# B. COMPARADOR: extraccion, reglas de emparejamiento, validacion de nombres
# ====================================================================================================


CARPETA_DATA = Path(os.environ.get("STACK_RAIZ") or Path(__file__).resolve().parent.parent) / "data"   # STACK_RAIZ: Stack instalado

# ---------------------------------------------------------------- puntuacion y umbrales
# Prioridad pedida: numero > set > acabado > nombre. Lo ausente suma 0 (no es contradiccion).
# Una CONTRADICCION (numero, set, acabado o variante) VETA a la candidata: no se compensa con otros puntos.
PESOS = {
    "numero":   {"coincide": 50, "parcial": 15, "contradice": -60},   # parcial: mismos digitos, distinta letra (39 vs 39s)
    "set":      {"exacto": 30, "alias": 28, "equiv": 26, "base": 24, "base_p": 18, "promo_nombre": 20, "contradice": -40},
    "especial": {"no_declarado": -22},   # impresion Showcase/Borderless/... que el producto no declara, habiendo una normal
    "acabado":  {"coincide": 15, "compatible": 6, "contradice": -50},  # compatible: la tienda no distingue foil/etched
    "nombre":   {"coincide": 5, "ambos": 10},   # ambos: producto con dos nombres y los dos apuntan a esta impresion
    "variante": {"coincide": 4, "contradice": -25, "suave": -4},   # suave: marca visual que no cuadra, pero numero+set coinciden
    "tratamiento": {"coincide": 12, "contradice": -60, "suave": -12},   # Surge/Galaxy/... foil (promo_types de Scryfall)
    "indicador": {"coincide": 8, "contradice": -60},
    "anio":      {"coincide": 10, "contradice": -60},      # Judge: anio del producto (2000Ver.) vs anio del nombre de la edicion   # prerelease / promo stamped / judge: validacion adicional, no identidad
}
MIN_EVIDENCIAS_PROMO = 2   # promos, judge, Secret Lair y cartas de dos nombres: minimo de evidencias INDEPENDIENTES del nombre y del indicador
UMBRAL_CONFIRMAR = 40   # puntaje minimo de la mejor candidata para confirmar
MARGEN_MIN = 20         # ventaja minima sobre la segunda candidata viable (si no, es ambiguo)

# ---------------------------------------------------------------- supuestos configurables (verificar con el resumen)
IDIOMA_HARERUYA = {"1": "ja", "2": "en"}
CONDICION_HARERUYA = {"1": "NM"}
MONEDA = "JPY"
CLASES_NO_CARTA = set()     # valores de product_class que NO son cartas sueltas (vacio hasta verlos)



# Sufijos de set de Hareruya = PISTAS de variante. Solo los listados se interpretan; el resto se conserva sin interpretar.
# regla(extra_de_scryfall, candidata) -> True / False / None
HINTS_SUFIJO = {"pre": ("promo con sello de prerelease", None), "jp": ("arte japones (variante)", None),
                "retro": ("variante retro (se resuelve con SET_ALIAS)", None), "rt": ("variante retro (se resuelve con SET_ALIAS)", None),
                "bf": ("booster fun (promo_types: boosterfun)", None)}   # solo para el resumen: sufijos ya interpretados

# ===== TABLAS PARA AMPLIAR (aqui se agregan casos nuevos; no hay que tocar la logica) =====
# 1) Alias de set: (set base de Hareruya, variante canonica) -> codigos de Scryfall que tambien valen.
SET_ALIAS = {
    ("bro", "retro"): ["brr"],        # The Brothers' War Retro Artifacts
    ("bro", "schematic"): ["brr"],
    ("mh1", "retro"): ["h1r"],        # Modern Horizons 1 Timeshifts ([MH1-RT]); verificar el codigo con el resumen
}
# 1b) Equivalencias de CODIGO de set (Hareruya -> Scryfall). Solo el codigo base COMPLETO; solo abren la busqueda de
#     candidatas, no confirman nada. Se registran en la columna `equivalencia_aplicada`.
# 1c) Un codigo de Hareruya que abarca VARIOS sets de Scryfall: Masterpiece Series (MPS) = Kaladesh Inventions y Amonkhet
#     Invocations. Solo abren la busqueda; el resto de la evidencia (nombre, acabado) decide.
SET_EQUIV_VARIOS = {"mps": ["mps", "mp2"], "mp2": ["mps", "mp2"]}
SET_EQUIV = {"mvm": "dds",      # Duel Decks: Mirrodin Pure vs. New Phyrexia (Hareruya EN dice MvM; Scryfall: dds)
             "cma": "cm1",      # Commander's Arsenal
             "mb1": "plst"}     # The List
SET_EQUIV_POR_NOMBRE = {"commander's arsenal": "cm1", "the list": "plst",
                        "secret lair prize": "slp",
                        "gp promo": "pgpx", "gp promos": "pgpx", "grand prix promo": "pgpx", "grand prix promos": "pgpx"}   # si el texto del producto trae el nombre de la edicion
# 2) Palabras de Hareruya -> variante canonica (se buscan fuera del nombre de la carta, como palabra completa).
ALIAS_VARIANTE = {
    "showcase": "showcase", "borderless": "borderless", "extended art": "extended art", "full art": "full art",
    "promo pack": "promo pack", "retrof": "retro", "retro": "retro", "schematic": "schematic",
    "japanese art": "japanese art", "alternate frame": "alternate frame",
    "booster fun": "booster fun", "phyrexian": "phyrexian",
}
# 2b) Tipo de version que Hareruya anota en el texto (se conserva siempre; solo decide si hay regla en REGLAS_VERSION).
VERSIONES = {"collector booster ver": "collector_booster", "draft/set booster ver": "draft_set_booster",
             "draft set booster ver": "draft_set_booster"}
RE_VERSION = re.compile(r"(collector|draft/set|draft set|draft|set)\s*booster\s*ver\.?", re.I)   # NO es un producto sellado
# Regla por tipo de version -> regla(extra, candidata) -> True / False / None. Vacio a proposito: Scryfall no declara
# 'Collector vs Draft/Set' de forma conocida. Cuando se sepa que campo lo distingue, se agrega aqui (ver resumen).
REGLAS_VERSION = {}
# 3) Regla por variante canonica: regla(extra_scryfall, candidata) -> True / False / None (None = no se puede saber).
def _frame(e):
    return e.get("frame")
REGLAS_VARIANTE = {
    "showcase": lambda e, c: "showcase" in e["frame_effects"],
    "borderless": lambda e, c: e["border_color"] == "borderless",
    "extended art": lambda e, c: "extendedart" in e["frame_effects"],
    "full art": lambda e, c: bool(e["full_art"]),
    "promo pack": lambda e, c: "promopack" in e["promo_types"],
    # retro: marco 1997 o set de nombre 'retro'; NO si es schematic. Si Scryfall no da el marco, no se afirma nada.
    "retro": lambda e, c: (False if "schematic" in e["frame_effects"] else
                           True if _frame(e) == "1997" or "retro" in norm(c.get("set_name")) else
                           False if _frame(e) else None),
    "japanese art": lambda e, c: (True if e.get("lang") == "ja" else False if e.get("lang") else None),
    "booster fun": lambda e, c: True if "boosterfun" in e["promo_types"] else None,
    "phyrexian": lambda e, c: True if e.get("lang") == "ph" else None,   # idioma Phyrexian de Scryfall
    # Alternate Frame de Hareruya = la version con marco especial (showcase, borderless, retro...): la impresion normal no lo es
    "alternate frame": lambda e, c: (True if ("showcase" in e["frame_effects"] or e["border_color"] == "borderless"
                                              or "extendedart" in e["frame_effects"] or "boosterfun" in e["promo_types"]
                                              or e.get("frame") in ("1997", "future")) else False),
    "schematic": lambda e, c: True if ("schematic" in e["frame_effects"] or "schematic" in norm(c.get("set_name"))) else None,
}
# 4) Tipos "especiales" de impresion: indicador del producto -> sufijo esperado del numero de coleccion en Scryfall.
# 5) Sufijo del set de Hareruya -> variante, usado solo si no hay una marca explicita de la misma familia
#    (en [BRO-Retro] 'retro' es la familia del set: la marca ■Schematic■ manda sobre ella).
SUFIJO_A_VARIANTE = {"retro": "retro", "rt": "retro", "jp": "japanese art", "bf": "booster fun"}   # [ELD-BF] = Booster Fun
# 6) Variantes que una IMPRESION puede tener y que el producto debe declarar (si no, esa impresion se descarta).
REGLAS_INVERSAS = {"japanese art": lambda e, c: e.get("lang") == "ja"}
FAMILIAS_VARIANTE = [{"retro", "schematic"}]
# 7) Marcas visuales que, si NO cuadran con Scryfall pero numero+nombre+set SI coinciden, no descartan a la candidata
#    (Scryfall marca mal/no marca border_color, frame_effects en Secret Lair, etc.). Resta puntos y deja una alerta.
VISUALES_SUAVES = {"showcase", "borderless", "extended art", "full art", "alternate frame", "booster fun", "phyrexian"}
# 8) Tratamientos foil de Hareruya (se buscan fuera del nombre y de los [ ]): canon -> (regex, promo_types de Scryfall).
#    Lista vacia = tratamiento reconocido pero sin campo conocido en Scryfall (se conserva, no se compara).
#    Los promo_types estan escritos de memoria: el resumen imprime los reales; corrige aqui si alguno difiere.
_SEP = r"[\s\-_\u30fb\u00b7]*"
TRATAMIENTOS_FOIL = {
    "surge foil": (r"surge" + _SEP + "foil", ["surgefoil"]),
    "galaxy foil": (r"galaxy" + _SEP + "foil", ["galaxyfoil"]),
    "double rainbow foil": (r"(?:(?<![a-z0-9])dr|double" + _SEP + "rainbow)" + _SEP + "foil", ["doublerainbow"]),
    "textured foil": (r"textured" + _SEP + "foil", ["textured"]),
    "step-and-compleat foil": (r"step" + _SEP + "and" + _SEP + "compleat", ["stepandcompleat"]),
    "neon ink": (r"neon" + _SEP + "ink", ["neonink"]),
    "rainbow foil": (r"(?<!double )(?<!double-)rainbow" + _SEP + "foil", ["rainbowfoil"]),
    "confetti foil": (r"confetti" + _SEP + "foil", ["confettifoil"]),
    "raised foil": (r"raised" + _SEP + "foil", ["raisedfoil"]),
    "ripple foil": (r"ripple" + _SEP + "foil", ["ripplefoil"]),
    "fracture foil": (r"fracture" + _SEP + "foil", ["fracturefoil"]),
    "halo foil": (r"halo" + _SEP + "foil", ["halofoil"]),
    "gilded foil": (r"gilded" + _SEP + "foil", ["gilded"]),
    "oil slick": (r"oil" + _SEP + "slick", ["oilslick"]),
    "silver foil": (r"silver" + _SEP + "foil", ["silverfoil"]),   # The Lord of the Rings
    "mana foil": (r"mana" + _SEP + "foil", ["manafoil"]),
    "emblem": (r"emblem", []),     # Jurassic World Collection (Emblem・Foil): sin campo conocido en Scryfall
}
TAGS_TRATAMIENTO = {t for _, tags in TRATAMIENTOS_FOIL.values() for t in tags}
# 9) Texto del set de Hareruya que NO es un codigo: (regex sobre el texto normalizado completo, regex de codigos de Scryfall,
#    palabras que debe traer el NOMBRE del set de Scryfall). Basta con que coincida una de las dos. Las mas especificas, primero.
SET_TEXTO = [
    (r"ftv\b.*|from the vault.*", r"v0\d|v1\d|drb", ["from the vault"]),         # [FtV:Twenty] -> From the Vault (el nombre afina)
    (r"other event anniversary|.*anniversary promos?", r"ppro", ["pro tour", "promo"]),     # Hareruya: Other Event Anniversary = Pro Tour Promos
    (r"premier play.*", r"pw\d\d|pwpn|pspl", ["premier play", "promo"]),                     # Premier Play: se busca en las promos
    (r"media promos?", r"pmei", ["media", "promo"]),
    (r"ubt|ultimate box toppers?", r"puma", ["ultimate box topper"]),
    (r"clash packs?", r"cp\d", ["clash pack"]),
    (r"ampersand(?: promos?)?", None, ["ampersand"]),
    (r"dop|dpo|dpa", r"dpa", ["duels of the planeswalkers", "duel of the planeswalkers"]),
    (r"other event promos?", r"pw\d\d", ["wizards play network"]),
    (r"sdcc|san diego comic[- ]?con", r"ps\d\d|psdc|sdcc", ["san diego comic-con", "comic-con"]),
    (r"arena(?: league)?", r"pal\d\d|parl", ["arena league"]),
    (r"slivers?", r"h09", ["premium deck series: slivers"]),               # Premium Deck Series
    (r"grb|graveyard hordes", r"pd3", ["graveyard hordes"]),
    (r"f&l|fal|fire (?:and|&) lightning", r"pd2", ["fire and lightning"]),
    (r"cma17|commander anthology", r"cma", []),
    (r"cma18|commander anthology (?:volume )?(?:ii|2)", r"cm2", []),
    (r"gateway", None, ["dci promos", "gateway"]),                          # Hareruya: Gateway = promos DCI
    (r"intro packs?", None, ["intro pack"]),
    (r"box promos?", r"pxtc", ["treasure chest", "box topper"]),             # p. ej. XLN Treasure Chest
    (r"store championships?", r"pw\d\d|pwpn", ["wizards play network", "store championship"]),
    (r"promos?|prm", None, ["promo"]),     # [Promo] / [PRM] sin set: cualquier set de Scryfall promo (nombre con 'Promo' o set_type promo)
]
# 10) Emblemas: un producto con 'Emblem' en el nombre de la carta es el emblema de OTRA carta y se ignora, salvo estas cartas reales.
EMBLEMAS_REALES = {"emblem of the warmind", "leering emblem", "earth's mightiest emblem"}
RE_SET_EDICION = re.compile(r"(\d{1,2})ed(?:bb|wb)?")     # 3EDBB -> 3ed (BB = borde negro, se ignora); 10ED -> 10e
RE_SET_XVX = re.compile(r"[a-z]v[a-z]")                      # VvK: Duel Deck (Venser vs. Koth)
SUFIJO_ESPERADO = {"prerelease": "s", "promo_stamped": "p", "serializada": "z"}
SUFIJOS_ESPECIALES = set(SUFIJO_ESPERADO.values())


# ---------------------------------------------------------------- texto
# texto de cada motivo (Stack lo muestra en la lista de inconsistencias)
EXPLICACION = {
    "nombre_distinto": "El producto es ajeno a esta busqueda: su titulo nombra otra carta (se conserva, solo se clasifica).",
    "nombre_no_interpretable": "No se pudo decidir con seguridad si el nombre del producto es la carta buscada (titulo sin 《》, nombre no latino, o titulo y card_name se contradicen). Se conserva para revision.",
    "clase_no_carta": "El tipo de producto esta marcado como no-carta.",
    "promo_sin_parecida": "Hareruya la vende como promo (Premier Play, Anniversary...) y se busco entre TODAS las promos de Scryfall de esta carta: ninguna se parece.",
    "set_sin_candidato": "Ninguna impresion de Scryfall tiene un set compatible con el de Hareruya. Si es una variante con otro set (ej. BRO-Retro -> BRR), agrega un alias en SET_ALIAS.",
    "numero_no_corresponde": "El set coincide, pero el numero del producto no corresponde a ninguna impresion de ese set.",
    "acabado_no_disponible": "La impresion existe, pero no con el acabado del producto.",
    "variante_contradice": "Set y numero encajan, pero la variante que indica Hareruya contradice a Scryfall.",
    "tratamiento_contradice": "Set y numero encajan, pero el tratamiento foil (Surge, Galaxy...) no coincide con los promo_types de Scryfall.",
    "emblema_de_otra_carta": "El producto es el emblema de otra carta (Emblem en el nombre); se ignora. Excepciones reales: EMBLEMAS_REALES.",
    "fuera_del_universo_2usd": "La mejor candidata es una version que vale menos de $2 (fuera de la referencia).",
    "varias_candidatas_similares": "Hay varias impresiones posibles con puntuacion parecida; no se fuerza una.",
    "puntuacion_insuficiente": "La mejor candidata no alcanza la puntuacion minima para confirmar.",
    "evidencia_minima_incompleta": "Falta evidencia de set o acabado para poder confirmar.",
    "sin_set": "El producto no trae set code utilizable.",
    "producto_no_carta": "El texto fuera del nombre de la carta trae una palabra de producto sellado o accesorio.",
    "promo_no_corresponde": "El indicador (prerelease / promo stamped) no concuerda con la impresion: sufijo de numero, set o promo_types.",
    "judge_sin_candidata": "El producto es Judge pero Scryfall no tiene una impresion Judge compatible de esta carta.",
    "identidad_promo_insuficiente": "Promo / Judge / carta de dos nombres: faltan evidencias independientes (numero, set, acabado) para confirmar.",
    "serializada_sin_candidata": "El producto dice (serial number) pero no hay impresion serializada (numero con z) compatible.",
    "judge_vs_jumpstart": "El codigo J## coincide con una impresion Judge pero el producto no dice Judge: no se asume Judge ni Jumpstart.",
    "nombres_no_resueltos": "Producto con dos nombres: no ambos apuntan a la misma impresion de Scryfall (o faltan datos de nombres alternativos).",
    "anio_judge_no_corresponde": "Judge / FNM: el anio que indica Hareruya no coincide con ninguna edicion Judge de Scryfall de esta carta.",
    "indicadores_en_conflicto": "El producto trae indicadores incompatibles entre si (p. ej. prerelease y promo stamped).",
}

def norm(texto):
    t = unicodedata.normalize("NFKD", str(texto or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.replace("\u2019", "'").replace("\u2018", "'").replace("\u201c", '"').replace("\u201d", '"')
    return re.sub(r"\s+", " ", t).strip().casefold()


def norm_num(valor):
    if valor is None:
        return None
    t = unicodedata.normalize("NFKC", str(valor)).strip().casefold()     # digitos de ancho completo -> ASCII
    if re.fullmatch(r"\d+\.0", t):
        t = t[:-2]
    return t.lstrip("0") or ("0" if t else None)



# ---------------------------------------------------------------- carpeta, carta, referencia, universo
PATRON_FECHA = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIENDA = "hareruya"


def abrir_catalogo(ruta):
    return sqlite3.connect(f"{ruta.resolve().as_uri()}?mode=ro", uri=True)  # solo lectura


def cargar_referencia(con, nombre):
    cols = ["cards_rowid", "name", "set_code", "set_name", "collector_number", "finish", "price_usd",
            "rarity", "lang", "released_at", "id", "oracle_id", "scryfall_uri"]
    filas = con.execute(
        "SELECT rowid, name, set_code, set_name, collector_number, finish, price_usd, rarity, lang, released_at, "
        "id, oracle_id, scryfall_uri FROM cards WHERE name = ? COLLATE NOCASE "
        "ORDER BY released_at, set_code, collector_number, finish", (nombre,)).fetchall()
    refs = []
    for n, f in enumerate(filas, start=1):
        r = dict(zip(cols, f))
        r["ref_id"] = f"R-{n:02d}"
        r["set_code"] = (r["set_code"] or "").casefold()
        refs.append(r)
    return refs


def impresion_de_carta(c):
    """Una carta del bulk -> la impresion con los campos que usa el comparador (None si es digital)."""
    if not isinstance(c, dict) or c.get("digital"):
        return None
    n = (c.get("name") or "").strip()
    p = c.get("prices") or {}
    nombres = {norm(n), norm(c.get("flavor_name")), norm(c.get("printed_name"))} | {norm(x) for x in n.split(" // ")}
    for cara in (c.get("card_faces") or []):
        nombres |= {norm(cara.get("name")), norm(cara.get("flavor_name")), norm(cara.get("printed_name"))}
    return {
        "nombres": sorted(x for x in nombres if x),
        "id": c.get("id"), "set_code": (c.get("set") or "").casefold(), "set_name": c.get("set_name"),
        "collector_number": c.get("collector_number"), "finishes": list(c.get("finishes") or []),
        "precios": {"nonfoil": _f(p.get("usd")), "foil": _f(p.get("usd_foil")), "etched": _f(p.get("usd_etched"))},
        "extra": {"frame_effects": c.get("frame_effects") or [], "promo_types": c.get("promo_types") or [],
                  "border_color": c.get("border_color"), "full_art": c.get("full_art"),
                  "set_type": c.get("set_type"), "promo": c.get("promo"), "frame": c.get("frame"),
                  "flavor_name": c.get("flavor_name"), "lang": c.get("lang"), "booster": c.get("booster")}}

def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def construir_candidatos(refs, universo):
    """Candidatas = cada (impresion, acabado) de Scryfall de esa carta. Marca cuales estan en la referencia (>= $2)."""
    en_ref = {(r["id"], r["finish"]): r for r in refs}
    cands, vistos = [], set()
    for p in (universo or []):
        for fin in p["finishes"]:
            if fin not in ("nonfoil", "foil", "etched"):
                continue
            r = en_ref.get((p["id"], fin))
            cands.append({"id": p["id"], "set_code": p["set_code"], "set_name": p["set_name"],
                          "collector_number": p["collector_number"], "finish": fin, "en_ref": r is not None,
                          "ref_id": r["ref_id"] if r else None,
                          "precio_scryfall_usd": r["price_usd"] if r else p["precios"].get(fin),
                          "extra": p["extra"], "nombres": p["nombres"], "cards_rowid": r["cards_rowid"] if r else None})
            vistos.add((p["id"], fin))
    for r in refs:  # filas de la referencia que el bulk no listo (o si no hay bulk)
        if (r["id"], r["finish"]) not in vistos:
            cands.append({"id": r["id"], "set_code": r["set_code"], "set_name": r["set_name"],
                          "collector_number": r["collector_number"], "finish": r["finish"], "en_ref": True,
                          "ref_id": r["ref_id"], "precio_scryfall_usd": r["price_usd"], "extra": None,
                          "nombres": None, "cards_rowid": r["cards_rowid"]})
    for n, c in enumerate(cands, start=1):
        c["cand_id"] = c["ref_id"] or f"X-{n:02d}"   # R-xx = en la referencia; X-xx = fuera de ella (< $2)
    return cands


# ---------------------------------------------------------------- Hareruya: consulta y extraccion
def separar_irrelevantes(registros, nombres_validos):
    """Hareruya, cuando no encuentra la palabra ('Products matching keyword "" could not be found'), devuelve miles de productos
    cualesquiera. Se separan (NO se eliminan) los ajenos a la busqueda: 'conservar' = propios + inciertos, 'descartar' = ajenos
    (se siguen guardando y clasificando como no_es_carta / nombre_distinto)."""
    conservar, descartar = [], []
    for r in registros:
        (descartar if clasificar_nombre(r["fila"], nombres_validos)[0] == "ajeno" else conservar).append(r)
    return conservar, descartar


class BusquedaIrrelevante(Exception):
    """Hareruya no encontro la palabra ('Products matching keyword ... could not be found') y manda productos cualesquiera."""


_VIGILANCIA = {"nombres": None, "vistos": 0, "relevantes": 0, "max_vacias": 2, "vacias": 0, "pag_vistos": 0, "pag_rel": 0, "filas": []}
DESCARTAR_IRRELEVANTES = True    # los productos que no son la carta buscada NO entran a las tablas (quedan en el crudo, marcados)
DESCARTADOS_IRRELEVANTES = {"n": 0}      # cuantos productos vio la ultima busqueda abortada


def activar_captura_cruda():
    """crear_fila descarta casi todo el producto: se envuelve (sin editar el archivo) para guardar el original."""
    global crear_fila
    if getattr(crear_fila, "_con_crudo", False):
        return
    original = crear_fila

    def con_crudo(doc, termino, capturado):
        fila = original(doc, termino, capturado)
        fila["raw_json"] = json.dumps(doc, ensure_ascii=False, sort_keys=True, default=str)
        vig = _VIGILANCIA
        if vig["nombres"]:      # corte temprano: si la 1a pagina completa no trae NINGUNA cara con el nombre buscado, se aborta
            vig["vistos"] += 1
            vig["filas"].append(fila)
            relevante = clasificar_nombre(fila, vig["nombres"])[0] != "ajeno"
            vig["relevantes"] += relevante
            # Corte por paginas: si la 1a pagina completa no trae la carta, o si 'max_vacias' paginas completas SEGUIDAS no la traen
            # (Hareruya ordena por relevancia; 'Alpha Myr' devuelve la carta y luego cientos de Alpha / Myrfors), se deja de pedir.
            vig["pag_vistos"] += 1
            vig["pag_rel"] += relevante
            if vig["pag_vistos"] >= FILAS_POR_PAGINA:
                if vig["pag_rel"] == 0:
                    vig["vacias"] += 1
                    if vig["relevantes"] == 0 or vig["vacias"] >= vig["max_vacias"]:
                        raise BusquedaIrrelevante(vig["vistos"])
                else:
                    vig["vacias"] = 0
                vig["pag_vistos"] = vig["pag_rel"] = 0
        return fila

    con_crudo._con_crudo = True
    crear_fila = con_crudo


def consultar_hareruya(termino, nombres_validos=None, max_vacias=2):
    activar_captura_cruda()
    print("Hareruya...")
    registros = []
    _VIGILANCIA.update(nombres=(nombres_validos if isinstance(nombres_validos, NombresValidos) else NombresValidos(nombres_validos)) if nombres_validos else None, vistos=0, relevantes=0,
                       max_vacias=max_vacias, vacias=0, pag_vistos=0, pag_rel=0, filas=[])
    DESCARTADOS_IRRELEVANTES["n"] = 0
    try:
        filas = buscar_carta(termino)
    except BusquedaIrrelevante as e:
        # Se corta la DESCARGA (no se piden mas paginas), pero NADA de lo ya descargado se elimina: los productos que no son la
        # carta quedan conservados y clasificados como ajenos a esta busqueda (ver separar_irrelevantes).
        v = _VIGILANCIA
        filas = list(v["filas"])
        DESCARTADOS_IRRELEVANTES["n"] = e.args[0]
        if v["relevantes"]:
            print(f"  [Aviso] '{termino}': {v['vacias']} pagina(s) seguidas sin la carta; se corta la descarga y se conservan {len(filas)} productos.")
        else:
            print(f"  [Aviso] Hareruya no encontro '{termino}': la primera pagina ({e.args[0]} productos) no trae esa carta; se corta la descarga "
                  "y esos productos se conservan como ajenos a la busqueda.")
    finally:
        _VIGILANCIA["nombres"] = None
    for n, fila in enumerate(filas, start=1):
        doc = json.loads(fila.pop("raw_json"))
        registros.append({"source_id": f"H-{n:04d}", "search_term": termino, "fila": fila, "doc": doc})
    return registros


def terminos_busqueda(carta):
    """Con que se busca la carta en Hareruya: 'buscar_como' (cara frontal) y, si la carta tiene varios nombres
    ('Rough // Tumble', 'Odds // Ends', doble cara), tambien cada una de las otras caras."""
    terminos, vistos = [], set()
    for t in [carta["buscar_como"]] + [x.strip() for x in carta["name"].split(" // ")]:
        if t and norm(t) not in vistos:
            vistos.add(norm(t))
            terminos.append(t)
    return terminos


def nombres_validos_carta(carta, cands=()):
    """Nombres con los que Hareruya puede llamar a esta carta: nombre, caras y los nombres alternativos de TODAS las
    impresiones en Scryfall (flavor_name / printed_name: p. ej. 'Dracula the Voyager' = Edgar, Charmed Groom)."""
    nombres = {norm(carta["name"]), norm(carta["buscar_como"])} | {norm(x) for x in carta["name"].split(" // ")}
    for c in cands or ():
        nombres |= set(c.get("nombres") or ())
    return NombresValidos(n for n in nombres if n)


def consultar_hareruya_carta(carta, nombres_validos):
    """Una busqueda por cada termino de terminos_busqueda(); los productos se fusionan SIN duplicados (por id de producto)."""
    terminos = terminos_busqueda(carta)
    if len(terminos) == 1:
        return consultar_hareruya(terminos[0], nombres_validos)
    todos, vistos, descartados = [], set(), 0
    for i, termino in enumerate(terminos):
        if i:
            time.sleep(PAUSA_SEGUNDOS)
        regs = consultar_hareruya(termino, nombres_validos, max_vacias=1 if i else 2)
        descartados = max(descartados, DESCARTADOS_IRRELEVANTES["n"])
        for r in regs:
            clave = r["fila"].get("product")
            if clave is not None and (clave, r["fila"].get("finish")) in vistos:
                continue
            vistos.add((clave, r["fila"].get("finish")))
            todos.append(r)
    for n, r in enumerate(todos, start=1):
        r["source_id"] = f"H-{n:04d}"
    DESCARTADOS_IRRELEVANTES["n"] = descartados
    return todos


RE_NUMERO = [r"\u300b\s*(\d{1,4}[a-z\u2605]?)(?=\s*[\[\(\uff3b\uff08]|\s*$)",    # 《Nombre》345 [ELD-BF]
             r"[\(\uff08](\d{1,4}[a-z\u2605]?)[\)\uff09]", r"<(\d{1,4}[a-z\u2605]?)>", r"#\s?(\d{1,4}[a-z\u2605]?)\b"]
RE_CLAVE_NUMERO = re.compile(r"(collector|card_?no|card_?number|^number$|^no$)", re.I)
RE_CARAS = re.compile(r"\u300a(.+?)\u300b")                      # 《nombre》
RE_MARCAS = re.compile(r"[\u25a0\u25c6](.+?)[\u25a0\u25c6]")       # ■Showcase■  ◆Prerelease◆
# Productos que NO son cartas y que Hareruya mezcla en la busqueda: se tiran (no se guardan ni se muestran).
RE_BASURA = re.compile(r"over\s*-?\s*sized?|chibi|sleeves?|deck\s*protectors?|ultra\W{0,3}pro|salvat|\btokens?\b|art\s*card", re.I)


def es_basura(fila):
    """Oversized, stickers chibi, fundas, Ultra Pro, Salvat, tokens y Art Cards. Se busca FUERA de los 《nombres》 (una carta
    puede llamarse como una de esas palabras) y en card_name, que Hareruya marca: 'Pest/Angel Token', '【Art Card】...'."""
    en = re.sub(r"\u300a[^\u300b]*\u300b", " ", str(fila.get("product_name_en") or ""))
    texto = en + " " + str(fila.get("set_code") or "") + " " + str(fila.get("card_name") or "")
    return RE_BASURA.search(unicodedata.normalize("NFKC", texto)) is not None


RE_NO_CARTA = re.compile(r"\b(booster|box|deck|sleeves?|playmat|binder|storage)\b", re.I)
JP_PRERELEASE, JP_JUDGE = "\u30d7\u30ec\u30ea\u30ea\u30fc\u30b9", "\u30b8\u30e3\u30c3\u30b8"


def sin_nombre(texto):
    """El texto sin los 《nombres》: ahi se buscan indicadores para que 'Judge' del nombre de una carta no cuente."""
    return RE_CARAS.sub(" ", texto or "")


def caras_producto(fila):
    """Todos los nombres posibles del producto: cada 《...》 y cada parte separada por '/' (JP/EN). Vale cualquier cara."""
    grupos = []
    for g in RE_CARAS.findall(fila.get("product_name_en") or ""):     # 《Dead // Gone》 = dos nombres, igual que 《Dead》//《Gone》
        grupos += [x.strip() for x in g.split("//") if x.strip()] if "//" in g else [g]
    nombres = []
    for g in (grupos or [fila.get("card_name") or ""]):
        for parte in g.split("/"):
            n = norm(parte)
            if n and n not in nombres:
                nombres.append(n)
    return nombres, grupos


# ---------------------------------------------------------------- coincidencia de NOMBRE (independiente de set / numero / acabado)
# Tres conceptos separados: (1) este bloque: ¿el producto es la carta de ESTA busqueda?  (2) evaluar(): ¿que impresion es?
# (3) motivo_no_comparable / palabra_no_carta: clasificacion comercial. Aqui solo se compara el nombre.
_ESPECIALES_NOMBRE = str.maketrans({"\u00e6": "ae", "\u00c6": "AE", "\u0153": "oe", "\u0152": "OE"})
RE_MARCAS_TITULO = re.compile(r"\[[^\]]*\]|\([^)]*\)|\u3010[^\u3011]*\u3011|\u25a0[^\u25a0]*\u25a0|\u25c6[^\u25c6]*\u25c6|<[^>]*>|\uff1c[^\uff1e]*\uff1e")
_RUIDO = None


def clave_nombre(texto):
    """Clave SOLO para comparar nombres (el nombre original nunca se toca): NFKC, sin diacriticos de letras latinas (los kana y
    kanji se conservan: が no pasa a か), ae/oe, minusculas y sin puntuacion ni espacios (Hareruya escribe 《Willo'theWisp》 por
    Will-o'-the-Wisp). Nunca iguala un prefijo con la palabra completa: 'smother' != 'smothering'."""
    t = unicodedata.normalize("NFKC", str(texto or "")).translate(_ESPECIALES_NOMBRE)
    salida, previo = [], ""
    for ch in unicodedata.normalize("NFD", t):
        if unicodedata.combining(ch) and previo and ord(previo) < 0x250:
            continue
        salida.append(ch)
        previo = ch
    t = unicodedata.normalize("NFC", "".join(salida)).casefold()
    return "".join(ch for ch in t if ch.isalnum())


def _tokens_nombre(texto):
    return re.findall(r"[^\W_]+", unicodedata.normalize("NFKC", str(texto or "")).casefold())


class NombresValidos(set):
    """Nombres con los que Hareruya puede llamar a la carta buscada (normalizados con norm) + sus claves de comparacion."""
    _claves = None

    @property
    def claves(self):
        if self._claves is None:
            self._claves = {clave_nombre(n) for n in self} - {""}
        return self._claves


def _claves_de(nombres):
    return nombres.claves if isinstance(nombres, NombresValidos) else {clave_nombre(n) for n in (nombres or ())} - {""}


def _palabras_ruido():
    """Palabras que acompanan al nombre en un titulo de producto pero NO son parte del nombre (acabado, tratamiento, promo, estado)."""
    global _RUIDO
    if _RUIDO is None:
        fijas = {"foil", "etched", "non", "nonfoil", "borderless", "showcase", "extended", "art", "full", "retro", "frame", "alternate",
                 "alt", "promo", "promos", "stamped", "prerelease", "pre", "release", "judge", "gift", "serialized", "textless", "nm",
                 "sp", "mp", "hp", "english", "japanese", "en", "jp", "ja", "version", "ver", "booster", "fun", "phyrexian"}
        for tabla in (ALIAS_VARIANTE, TRATAMIENTOS_FOIL, SUFIJO_A_VARIANTE):
            for k in tabla:
                fijas |= set(_tokens_nombre(k))
        _RUIDO = fijas
    return _RUIDO


def _solo_ruido(tokens):
    ruido = _palabras_ruido()
    return all(t.isdigit() or t in ruido for t in tokens)


def _tramo_nombre(tokens, claves, solo_prefijo=False):
    """(inicio, fin, resto_es_ruido) del tramo de palabras ENTERAS que forma un nombre valido; None si no hay. Prefiere el tramo
    cuyo resto son solo palabras de producto."""
    mejor = None
    for s in range(len(tokens)):
        if solo_prefijo and s:
            break
        for e in range(s + 1, len(tokens) + 1):
            if clave_nombre("".join(tokens[s:e])) in claves:
                if _solo_ruido(tokens[:s] + tokens[e:]):
                    return s, e, True
                mejor = mejor or (s, e, False)
    return mejor


def clasificar_nombre(fila, nombres_validos):
    """('coincide' | 'ajeno' | 'incierto', detalle): relacion del producto con la carta de ESTA busqueda.
    - coincide: algun nombre del titulo (cada 《...》, cada lado de // y /) es un nombre valido, o es un nombre valido + palabras de
      producto (Foil, Borderless, Retro Frame ...). Nunca basta un prefijo o subcadena: 'Smother' no es 'Smothering Tithe'.
    - ajeno: el titulo nombra otra carta. Se CONSERVA el producto, clasificado como ajeno a esta busqueda.
    - incierto: no se puede decidir con seguridad (titulo sin 《》, nombre no latino, el titulo y card_name se contradicen): se conserva
      como candidato para revision."""
    claves = _claves_de(nombres_validos)
    if not claves:
        return "incierto", "no hay nombres validos con los que comparar"
    titulo = fila.get("product_name_en") or ""
    card_name = (fila.get("card_name") or "").strip()
    cn_ok = any(clave_nombre(x) in claves for x in [card_name] + re.split(r"//|/", card_name))
    grupos = [g.strip() for g in RE_CARAS.findall(titulo) if g.strip()]
    if grupos:
        partes = [x.strip() for g in grupos for x in re.split(r"//|/", g) if x.strip()]
        for n in grupos + partes:
            if clave_nombre(n) in claves:
                return "coincide", f"nombre en el titulo: {n}"
        for x in partes:
            m = _tramo_nombre(_tokens_nombre(x), claves, solo_prefijo=True)
            if m and m[2]:
                return "coincide", f"nombre + palabras de producto: {x}"
        if cn_ok:
            return "incierto", (f"el titulo nombra {partes} pero card_name de Hareruya coincide con la carta buscada: revisar "
                                "(posible nombre alternativo que Scryfall no trae)")
        if not card_name and not any(re.search(r"[A-Za-z]", x) for x in partes):
            return "incierto", f"nombre no latino sin equivalente conocido: {partes}"
        return "ajeno", f"el titulo nombra otra carta: {partes}"
    if cn_ok:
        return "coincide", f"card_name de Hareruya: {card_name}"
    tokens = _tokens_nombre(RE_MARCAS_TITULO.sub(" ", titulo))
    m = _tramo_nombre(tokens, claves) if tokens else None
    if m and card_name:
        return "incierto", f"el titulo contiene el nombre buscado pero card_name dice '{card_name}': revisar"
    if m and m[2]:
        return "coincide", "nombre + palabras de producto en el titulo (sin 《》)"
    if m:
        return "incierto", f"el nombre buscado aparece junto a palabras que no son de producto: {tokens[:m[0]] + tokens[m[1]:]}"
    if tokens or card_name:
        return "ajeno", f"el titulo ('{titulo}') no contiene el nombre buscado como palabras completas"
    return "incierto", "titulo y card_name vacios"


def nombre_en(fila):
    grupos = RE_CARAS.findall(fila.get("product_name_en") or "")
    return " // ".join(g.strip() for g in grupos) if grupos else (fila.get("card_name") or "")


def partir_set(codigo_norm):
    """'cmm-bf' -> ('cmm', 'bf'): se conservan el codigo completo, la base y el sufijo."""
    m = re.fullmatch(r"(.+)-([a-z0-9]+)", codigo_norm or "")
    return (m.group(1), m.group(2)) if m else (codigo_norm, None)


def anio4(d):
    return d if len(d) == 4 else ("19" + d if int(d) >= 90 else "20" + d)


RE_FNM = re.compile(r"(?<![a-z0-9])fnm(?:[_\s-]*((?:19|20)?\d{2}))?(?![a-z0-9])")


def es_fnm(c):
    """Impresion de Friday Night Magic: nombre del set (o codigo fNN / pfnm)."""
    return "friday night magic" in norm(c.get("set_name")) or bool(re.fullmatch(r"f\d\d|pfnm", c.get("set_code") or ""))


def tratamientos_del_texto(texto_norm):
    res = []
    for canon, (rx, _) in TRATAMIENTOS_FOIL.items():
        if re.search(rx, texto_norm) and canon not in res:
            res.append(canon)
    return res


def parsear_set(raw):
    """Texto entre [ ] -> set normalizado + indicador implicito. 'Judge Foil' y 'J19' no se tratan como set de Scryfall."""
    s = norm(re.sub(r"[\u30fb\u00b7\uff65]", "_", str(raw or "")))     # DD3・JvC -> dd3_jvc (codigo de Scryfall)
    r = {"set_normalizado": s or None, "indicador": None, "pista_j": None, "anio_fnm": None}
    if not s or not s.isascii():
        r["set_normalizado"] = None
    elif "judge" in s:
        r.update(set_normalizado=None, indicador="judge")
    elif re.fullmatch(r"fnm(?:[_\s-]*(?:19|20)?\d{2})?", s):     # [FNM], [FNM18], [FNM 2018]: se compara con el NOMBRE del set
        r.update(set_normalizado=None, indicador="fnm")
        m = re.search(r"(\d{2,4})$", s)
        r["anio_fnm"] = anio4(m.group(1)) if m else None
    elif re.fullmatch(r"wc[_\s-]*(?:19|20)?\d{2}", s):     # World Championship Decks: WC04 / WC2004 -> wc04 (el numero es el anio)
        r["set_normalizado"] = "wc" + s[-2:]
    elif re.fullmatch(r"j\d{2}", s):
        r["pista_j"] = s     # J## es solo una pista: puede ser Judge o Jumpstart; se valida contra Scryfall
    else:
        m = re.fullmatch(r"p\s*stamped[_\s-]*([a-z0-9]+)", s)
        if m:
            r.update(set_normalizado=m.group(1), indicador="promo_stamped")
    return r


def prerelease_mal_escrito(t):
    """'Prereleace', 'Pre-release', 'Pre release'...: errores de ortografia de Hareruya cuentan como prerelease."""
    if re.search(r"(?<![a-z])pre[\s\-]release", t):
        return True
    return any(difflib.SequenceMatcher(None, w, "prerelease").ratio() >= 0.85 for w in re.findall(r"[a-z]{7,}", t))


RE_DD_JP = re.compile(r"(?<![A-Za-z0-9])(DD[A-Z])(?![A-Za-z0-9])")   # DDS = Duel Decks (codigo de Scryfall) en el nombre japones


def detectar_indicadores(texto_en, texto_jp, info_set, sufijo):
    t = norm(sin_nombre(texto_en) + " " + sin_nombre(texto_jp))
    raw = sin_nombre(texto_en) + sin_nombre(texto_jp)
    ind = set()
    if "prerelease" in t or sufijo == "pre" or JP_PRERELEASE in raw or prerelease_mal_escrito(t):
        ind.add("prerelease")
    if "promo stamped" in t or "p stamped" in t or info_set["indicador"] == "promo_stamped":
        ind.add("promo_stamped")
    if "judge" in t or JP_JUDGE in raw or info_set["indicador"] == "judge":
        ind.add("judge")
    if "serial number" in t or "serialized" in t:
        ind.add("serializada")
    if RE_FNM.search(t) or "friday night magic" in t or info_set["indicador"] == "fnm":
        ind.add("fnm")
    return ind


def extraer_finish(texto_en, foil_flg):
    """Acabado segun product_name_en (marca) y segun el campo foil_flg, por separado.
    Devuelve (marca, flag, compatibles, fuente, conflicto). 'compatibles' = acabados que explican el producto."""
    t = norm(texto_en)
    if "etched" in t:
        marca = "etched"
    elif re.search(r"non[- ]?foil", t):
        marca = "nonfoil"
    elif "foil" in t:
        marca = "foil"
    else:
        marca = None
    flag = {"1": "foil", "0": "nonfoil"}.get(str(foil_flg)) if foil_flg is not None else None
    conflicto = False
    if marca == "etched":
        comp, fuente = {"etched"}, "texto"
        conflicto = flag == "nonfoil"
    elif marca == "foil":
        comp, fuente = {"foil"}, "texto"
        conflicto = flag == "nonfoil"
    elif marca == "nonfoil":
        comp, fuente = {"nonfoil"}, "texto"
        conflicto = flag == "foil"
    elif flag == "nonfoil":
        comp, fuente = {"nonfoil"}, "foil_flg=0 (sin marca de acabado en el texto)"
    elif flag == "foil":
        comp, fuente = {"foil", "etched"}, "foil_flg=1 (sin marca: no distingue foil de etched)"
    else:
        comp, fuente = {"nonfoil", "foil", "etched"}, "sin dato"
    return marca, flag, comp, fuente, conflicto


def producto_hareruya(reg):
    f, d = reg["fila"], reg["doc"]
    en, jp = f.get("product_name_en") or "", f.get("product_name") or ""
    info = parsear_set(f.get("set_code"))
    set_norm = info["set_normalizado"]
    base, sufijo = partir_set(set_norm)
    ind = detectar_indicadores(en, jp, info, sufijo)
    marca, flag, comp, fuente, conflicto = extraer_finish(en, d.get("foil_flg"))
    caras, grupos = caras_producto(f)
    grupos_norm = [{norm(x) for x in g.split("/") if norm(x)} for g in grupos]
    equiv = SET_EQUIV.get(base)
    if not equiv:
        t_nom = norm(sin_nombre(en) + " " + sin_nombre(jp))
        equiv = next((v for k, v in SET_EQUIV_POR_NOMBRE.items()
                      if re.search(r"(?<![a-z0-9])" + re.escape(k) + r"(?![a-z0-9])", t_nom)), None)
    if not equiv and set_norm:     # Duel Decks: el nombre en ingles solo trae el codigo de Hareruya (MvM); el japones trae DDS
        mdd = RE_DD_JP.search(sin_nombre(jp))
        if mdd and mdd.group(1).lower() != set_norm:
            equiv = mdd.group(1).lower()
    set_texto = regla_set_texto(set_norm)
    if not equiv and set_norm:
        me = RE_SET_EDICION.fullmatch(set_norm)
        if me:
            n_ed = int(me.group(1))
            cod_ed = "10e" if n_ed == 10 else f"{n_ed}ed"
            if cod_ed != set_norm:
                equiv = cod_ed
    t_promo = re.sub(r"promo[\s\-]*(stamped|pack)", " ", t_sin_corto := norm(sin_nombre(en) + " " + sin_nombre(jp)))
    promo_texto = bool(re.search(r"(?<![a-z0-9])promos?(?![a-z0-9])", t_promo)) and "promo_stamped" not in ind
    set_alt = []
    if set_norm and re.search(r"[_\s]", set_norm):
        ult = re.split(r"[_\s]+", set_norm)[-1]
        if 2 <= len(ult) <= 5 and ult != set_norm:
            set_alt.append(ult)
    ref_origen = list(dict.fromkeys(re.findall(r"[\(\uff08]\s*([A-Za-z0-9]{3}-\d{1,4}[a-z]?)\s*[\)\uff09]", en + " " + jp)))
    tokens, fuente_num, crudos = [], [], []
    for campo, texto in (("product_name_en", en), ("product_name", jp)):
        for patron in RE_NUMERO:
            for m in re.finditer(patron, texto, flags=re.I):
                tok = norm_num(m.group(1))
                if tok and tok not in tokens:
                    tokens.append(tok)
                    crudos.append(m.group(1))
                    fuente_num.append(f"{campo}:{tok}")
    if equiv == "plst" or set_norm == "plst":     # The List: el numero es 'SET-123' (p. ej. ICE-64)
        for m in re.finditer(r"(?<![a-z0-9])([a-z0-9]{3})-(\d{1,4})(?![a-z0-9])", norm(sin_nombre(en) + " " + sin_nombre(jp))):
            tok = f"{m.group(1)}-{m.group(2)}"
            if tok not in tokens:
                tokens.append(tok); crudos.append(tok); fuente_num.append(f"product_name:{tok}")
    for k, v in d.items():  # campos estructurados que parezcan numero de coleccion (si existieran)
        if RE_CLAVE_NUMERO.search(str(k)) and v not in (None, ""):
            tok = norm_num(v)
            if tok and tok not in tokens:
                tokens.append(tok)
                crudos.append(str(v))
                fuente_num.append(f"{k}:{tok}")
    t_sin = norm(sin_nombre(en) + " " + sin_nombre(jp))
    marcas = RE_MARCAS.findall(en)
    variantes = variantes_del_texto(re.sub(r"\[[^\]]*\]", " ", t_sin))   # fuera de los [ ]: el set no es una marca
    canon_suf = SUFIJO_A_VARIANTE.get(sufijo)
    if canon_suf and not any(canon_suf in fam and fam & set(variantes) for fam in FAMILIAS_VARIANTE):
        variantes.setdefault(canon_suf, "sufijo:" + sufijo)    # [BRO-Retro] sin marca explicita -> retro
    trat = tratamientos_del_texto(re.sub(r"\[[^\]]*\]", " ", t_sin))
    var_norm = list(variantes) + (["secret lair"] if "secret lair" in t_sin else [])
    if len(grupos) > 1:
        var_norm.append("dos_nombres")
    versiones = list(dict.fromkeys(versiones_del_texto(t_sin)))
    anios = anios_del_producto([en, jp] + [v for k, v in d.items() if isinstance(v, str) and k not in ("product_name", "product_name_en")])
    if "fnm" in ind:     # anio de la edicion FNM: [FNM18] o 'FNM 2018' en el texto
        mf = RE_FNM.search(t_sin)
        a = info["anio_fnm"] or (anio4(mf.group(1)) if mf and mf.group(1) else None)
        if a and a not in anios:
            anios.append(a)
    idioma_raw = None if f.get("language") is None else str(f["language"])
    cond_raw = None if f.get("condition") is None else str(f["condition"])
    return {
        "source_id": reg["source_id"], "search_term": reg["search_term"],
        "card_name_original": f.get("card_name"), "product_name_en_original": en,
        "product_name_original": jp, "nombre_normalizado": " | ".join(caras),
        "clase_producto": d.get("product_class"),
        "set_original": f.get("set_code"), "set_normalizado": set_norm, "set_base": base, "sufijo_set": sufijo,
        "numero_original": ", ".join(crudos) or None, "numero_normalizado": ", ".join(tokens) or None,
        "numero_tokens": tokens, "numero_fuente": fuente_num,
        "variante_texto": ", ".join(marcas) or None, "variante_normalizada": ", ".join(var_norm) or None,
        "indicador_prerelease": "prerelease" in ind,
        "indicador_promo_stamped": "promo_stamped" in ind,
        "indicador_judge": "judge" in ind,
        "indicador_serializada": "serializada" in ind,
        "referencia_origen": ", ".join(ref_origen) or None, "indicador_fnm": "fnm" in ind, "tratamiento_foil": ", ".join(trat) or None,
        "tipo_version": ", ".join(versiones) or None, "anio_producto": ", ".join(anios) or None,
        "finish_marca_texto": marca, "foil_flg_original": d.get("foil_flg"), "finish_flag": flag,
        "finish_compatibles": sorted(comp), "finish_fuente": fuente, "finish_conflicto": conflicto,
        "idioma_raw": idioma_raw, "idioma": IDIOMA_HARERUYA.get(idioma_raw),
        "condicion_raw": cond_raw, "condicion": CONDICION_HARERUYA.get(cond_raw),
        "precio_jpy": f.get("price_jpy"), "moneda": MONEDA, "stock": f.get("stock"),
        "precio_usd_estimado": None, "hareruya_bajo_2usd": None,
        "id_producto": f.get("product"), "capturado_utc": f.get("captured_at"),
        "_comp": comp, "_nombre_en": nombre_en(f), "_caras": caras, "_fila": f, "_n_grupos": len(grupos),
        "set_equivalencia": equiv, "equivalencia_aplicada": (f"{base or '(por nombre)'} -> {equiv}" if equiv else None),
        "_grupos": grupos_norm, "_ind": ind, "_variantes": list(variantes), "_tratamientos": trat, "_set_alt": set_alt, "_set_texto": set_texto, "_xvx": bool(RE_SET_XVX.fullmatch(set_norm or "")),
        "_ftv": (re.split(r"[:_\s]+", set_norm, maxsplit=1)[1:] or [None])[0] if re.match(r"ftv\b", set_norm or "") else None,
        "_sld_cmd": bool(re.search(r"sld\s*commander\s*deck", en, re.I)),
        "_xvx_ok": set(), "_wc": iniciales_jugador(en) if re.fullmatch(r"wc\d\d", set_norm or "") else None, "_wc_ok": None, "_promo_texto": promo_texto, "_ref_origen": [x.lower() for x in ref_origen], "_versiones": versiones, "_anios": anios, "_pista_j": info["pista_j"], "_sin_nombre": t_sin,
    }


def variantes_del_texto(texto_norm):
    """Variantes canonicas presentes (palabra completa) y la palabra original: {'retro': 'retrof'}."""
    res = {}
    for palabra, canon in ALIAS_VARIANTE.items():
        if re.search(r"(?<![a-z0-9])" + re.escape(palabra) + r"(?![a-z0-9])", texto_norm):
            res.setdefault(canon, palabra)
    return res


RE_ANIO = [re.compile(r"(?<!\d)((?:19|20)\d{2})\s*ver\b\.?", re.I), re.compile(r"\bver\.?\s*((?:19|20)\d{2})(?!\d)", re.I)]
RE_ANIO_EDICION = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")


def anios_del_producto(textos):
    """Anios indicados como '2000Ver.', 'Ver.2000' (nombre EN/JP y demas campos de texto de Hareruya), sin duplicados."""
    res = []
    for t in textos:
        for rx in RE_ANIO:
            for m in rx.finditer(norm(sin_nombre(t))):
                if m.group(1) not in res:
                    res.append(m.group(1))
    return res


def versiones_del_texto(texto_norm):
    return [v for k, v in VERSIONES.items() if k in texto_norm]


def palabra_no_carta(p):
    # El texto japones suele traer el nombre de la edicion ('Duel Decks', 'Secret Lair ... Deck'): si el producto ya trae numero
    # de coleccion, solo el nombre EN puede marcarlo como sellado.
    jp = "" if p.get("numero_tokens") else sin_nombre(p["product_name_original"])
    txt = RE_VERSION.sub(" ", sin_nombre(p["product_name_en_original"]) + " " + jp)
    txt = re.sub(r"(duel|commander)\s*decks?(\s*anthology)?", " ", txt, flags=re.I)
    txt = re.sub(r"booster\s*fun|box\s*(promos?|toppers?)", " ", txt, flags=re.I)     # 'Booster Fun' es una variante, no un sobre     # 'Duel Decks' es una edicion, no un mazo sellado
    m = RE_NO_CARTA.search(txt)
    return m.group(1).casefold() if m else None


def motivo_no_comparable(p):
    es_jp_art = "japanese art" in p["_variantes"]
    if p["precio_jpy"] is None:
        return "sin_precio"
    if p["stock"] is None:
        return "stock_desconocido"
    if p["stock"] <= 0:
        return "sin_stock"
    if p["idioma"] != "en" and not (es_jp_art and p["idioma"] == "ja"):
        return f"idioma_{p['idioma'] or 'desconocido'}"
    if p["condicion"] != "NM":
        return f"condicion_{p['condicion'] or 'desconocida'}"
    return None


# ---------------------------------------------------------------- evidencia por candidata
def regla_set_texto(set_norm):
    for rx, codigos, nombres in SET_TEXTO:
        if set_norm and re.fullmatch(rx, set_norm):
            return codigos, nombres
    return None


def iniciales_jugador(texto_en):
    """World Championship Decks: Hareruya pone el jugador despues del set ('...[WC02] Carlos Romao'). Scryfall numera cada
    carta con las iniciales del jugador ('cr92', 'cr92sb'). -> iniciales posibles ({'cr'}) o None si no viene el jugador."""
    m = re.search(r"\]\s*([^\[\]]+?)\s*$", texto_en or "")
    if not m:
        return None
    palabras = [w for w in re.findall(r"[^\W\d_]+", clave_texto(m.group(1)))]
    if not palabras:
        return None
    return {"".join(w[0] for w in palabras), palabras[0][0] + palabras[-1][0]}


def clave_texto(t):
    """Minusculas sin acentos (Romao = Romão)."""
    t = unicodedata.normalize("NFKD", str(t or ""))
    return "".join(ch for ch in t if not unicodedata.combining(ch)).casefold()


def prefijo_jugador(numero):
    m = re.match(r"([a-z]+)\d", (numero or "").casefold())
    return m.group(1) if m else None


def es_duel_deck(c):
    return (c.get("extra") or {}).get("set_type") == "duel_deck" or "duel decks" in norm(c.get("set_name"))


def iniciales_duel_deck(c):
    """'Duel Decks: Venser vs. Koth' -> 'vvk'."""
    nombre = norm(c.get("set_name")).split(":", 1)[-1]
    partes = [x.strip() for x in re.split(r"\s+vs\.?\s+", nombre) if x.strip()]
    return "v".join(x[0] for x in partes) if len(partes) == 2 else None


def es_set_promo(c):
    return "promo" in norm(c.get("set_name"))


def nivel_set(p, cand):
    """exacto / base / base_p / contradice / ausente. El sufijo NO demuestra nada: solo permite buscar candidatas."""
    codigo = cand["set_code"]
    if p.get("_sld_cmd"):        # SLD Commander Deck: la carta normal (set-numero de origen) o un set Secret Lair / Commander
        if any(r.split("-")[0] == codigo for r in p["_ref_origen"]):
            return "exacto"
        sn = norm(cand.get("set_name"))
        return "equiv" if ("secret lair" in sn or "commander" in sn) else "contradice"
    vari = SET_EQUIV_VARIOS.get(p["set_base"] or "")
    if vari and codigo in vari:
        return "exacto" if codigo == p["set_base"] else "equiv"
    if p.get("_ftv") is not None or (p["_set_texto"] and p["_set_texto"][1] == ["from the vault"]):
        sn = norm(cand.get("set_name"))
        if "from the vault" in sn or re.fullmatch(r"v0\d|v1\d|drb", codigo):
            return "exacto" if p.get("_ftv") and p["_ftv"] in sn else "equiv"
        return "contradice"
    if p["_pista_j"] and "judge" in p["_ind"]:        # Judge explicito: el codigo J## no se asume, solo suma si coincide
        return "exacto" if codigo == p["_pista_j"] else "ausente"
    if "fnm" in p["_ind"]:                             # FNM: se compara con el nombre del set de Scryfall (Friday Night Magic)
        return "exacto" if (p["set_normalizado"] == codigo or es_fnm(cand)) else "contradice"
    if not p["set_normalizado"] and not p["set_equivalencia"]:
        return "ausente"
    if p["set_normalizado"] == codigo:
        if p.get("_wc_ok") is not None and cand["cand_id"] not in p["_wc_ok"]:
            return "contradice"                         # WC: carta de otro jugador de ese mundial
        return "exacto"
    if p["_set_texto"]:
        codigos, nombres = p["_set_texto"]
        sn = norm(cand.get("set_name"))
        es_promo_set = "promo" in nombres and (cand.get("extra") or {}).get("set_type") == "promo"
        if (codigos and re.fullmatch(codigos, codigo)) or any(n in sn for n in nombres) or es_promo_set:
            return "equiv"
    if p["_xvx"] and cand["cand_id"] in p["_xvx_ok"]:
        return "equiv"
    if p["sufijo_set"] and codigo == f"{p['set_base']}_{p['sufijo_set']}":
        return "exacto"
    if p["set_equivalencia"] and p["set_equivalencia"] == codigo:
        return "equiv"
    if p["sufijo_set"] and p["set_base"] == codigo:
        return "base"
    if codigo in p["_set_alt"]:                      # 'Foo_BRO': lo que va antes del codigo se descarta y se busca el codigo
        return "equiv"
    for v in p["_variantes"]:
        if codigo in SET_ALIAS.get((p["set_base"], v), []):
            return "alias"
    if (p["sufijo_set"] == "pre" or {"prerelease", "promo_stamped"} & p["_ind"]) and "p" + (p["set_base"] or "") == codigo:
        return "base_p"
    if p["_promo_texto"] and es_set_promo(cand):       # nada coincide, pero el producto dice Promo: se busca 'Promo' en el nombre del set
        return "promo_nombre"
    return "contradice"


def partir_numero(n):
    m = re.fullmatch(r"(\d+)(.*)", n or "")
    return (m.group(1), m.group(2)) if m else (None, n or "")


def senal_numero(tokens_validos, cand, p):
    """coincide / parcial / contradice / ausente. Con indicador, el numero esperado lleva sufijo: prerelease 173 -> 173s,
    promo stamped 173 -> 173p. s y p NO se confunden: una impresion 173s contradice a un producto promo stamped."""
    if not tokens_validos:
        return "ausente"
    n = norm_num(cand["collector_number"])
    if n and "-" in n:     # The List: 'isd-123'. No se sabe cual numero usa Hareruya: sin veto, solo suma si coincide la parte local
        local = n.rsplit("-", 1)[1]
        if n in tokens_validos:
            return "coincide"
        return "parcial" if any(norm_num(t) == norm_num(local) for t in tokens_validos) else "ausente"
    tokens_validos = [t for t in tokens_validos if "-" not in t]     # 'SET-123' solo se compara con numeros del mismo formato
    if not tokens_validos:
        return "ausente"
    dc, sc = partir_numero(n)
    esperados = {SUFIJO_ESPERADO[i] for i in p["_ind"] if i in SUFIJO_ESPERADO}
    orden = {"contradice": 0, "parcial": 1, "coincide": 2}
    mejor = "contradice"
    for t in tokens_validos:
        dt, st = partir_numero(t)
        if t == n:
            r = "coincide"
        elif dt is None or dt != dc:
            r = "contradice"
        else:
            derivado = {st} if st else esperados
            if sc in derivado:
                r = "coincide"
            elif sc in SUFIJOS_ESPECIALES or st in SUFIJOS_ESPECIALES:
                r = "contradice"
            else:
                r = "parcial"
        if orden[r] > orden[mejor]:
            mejor = r
    return mejor



def senal_nombre(p, c):
    """Producto con un solo nombre: coincide (ya validado). Con dos: 'ambos' solo si cada nombre apunta a esta impresion."""
    if p["_n_grupos"] <= 1:
        return "coincide", "un solo nombre"
    if c["nombres"] is None:
        return "desconocido", "sin nombres alternativos de Scryfall (falta el bulk)"
    n = set(c["nombres"])
    hit = [bool(g & n) for g in p["_grupos"]]
    det = f"nombres del producto {[sorted(g) for g in p['_grupos']]} vs impresion {sorted(n)}"
    return ("ambos" if all(hit) else "parcial"), det


def nombres_compatibles(p, c):
    """Relaja 'dos nombres' solo si Scryfall no contradice: sin flavor_name conocido, o un nombre casi igual (puntuacion/mayusculas)."""
    ex = c.get("extra")
    fl = norm((ex or {}).get("flavor_name")) if ex else ""
    if not fl:
        return True
    pendientes = [g for g in p["_grupos"] if not (g & set(c["nombres"] or []))]
    return all(any(difflib.SequenceMatcher(None, x, n).ratio() >= 0.85 for x in g for n in c["nombres"]) for g in pendientes)


def tipo_cand(c):
    """Tipos promo de una impresion de Scryfall: prerelease / promo_stamped / judge (vacio = impresion normal)."""
    n = (c["collector_number"] or "").casefold()
    t = set()
    if re.fullmatch(r"\d+s", n):
        t.add("prerelease")
    if re.fullmatch(r"\d+p", n):
        t.add("promo_stamped")
    if re.fullmatch(r"\d+z", n):
        t.add("serializada")
    ex = c["extra"]
    if ex:
        pt = set(ex["promo_types"])
        if "prerelease" in pt:
            t.add("prerelease")
        if pt & {"promopack", "stamped"}:
            t.add("promo_stamped")
        if "serialized" in pt:
            t.add("serializada")
        if "judgegift" in pt or "judge" in norm(c.get("set_name")):
            t.add("judge")
    if es_fnm(c):
        t.add("fnm")
    return t


def senal_anio(p, c):
    """Solo para productos Judge con anio explicito. Sin anio en el producto o en el nombre de la edicion: ausente."""
    if not ({"judge", "fnm"} & p["_ind"]) or not p["_anios"]:
        return "ausente", "sin anio que comparar"
    m = RE_ANIO_EDICION.search(c.get("set_name") or "")
    if not m:
        return "ausente", f"el nombre de la edicion {c.get('set_name')!r} no trae anio"
    ok = m.group(1) in p["_anios"]
    return ("coincide" if ok else "contradice"), f"anio del producto {p['_anios']} vs edicion {c.get('set_name')!r} ({m.group(1)})"


def senal_indicador(p, c):
    ind, tipos = p["_ind"], tipo_cand(c)
    if not ind:
        return ("contradice", f"la impresion es promo {sorted(tipos)} y el producto no trae indicador") if tipos \
            else ("ausente", "sin indicador en el producto")
    if c["extra"] is None and not tipos:
        return "ausente", "sin campos de variante para comprobar el indicador"
    if ind & tipos:
        return "coincide", f"indicador {sorted(ind)} = tipo de la impresion {sorted(tipos)}"
    if "promo_stamped" in ind and not tipos and p["set_base"] and c["set_code"] == "p" + p["set_base"]:
        return "coincide", f"set promo '{c['set_code']}' = version con sello de {p['set_base']}"
    return "contradice", f"indicador {sorted(ind)} vs tipo de la impresion {sorted(tipos) or 'normal'}"


def senal_acabado(p, cand, etched_keys=frozenset()):
    comp = p["_comp"]
    if comp == {"nonfoil", "foil", "etched"}:
        return "ausente"
    if cand["finish"] not in comp:
        return "contradice"
    if len(comp) == 1:
        return "coincide"
    # foil_flg=1 sin marca en el texto (foil o etched): si esa impresion NO existe en etched, el unico acabado posible es foil
    if cand["finish"] == "foil" and (cand["set_code"], norm_num(cand["collector_number"])) not in etched_keys:
        return "coincide"
    return "compatible"


def senal_variante(p, cand):
    """-> (senal, detalle, canones que contradicen). Los canones permiten suavizar marcas solo visuales."""
    extra = cand["extra"]
    if extra is None:
        return "ausente", "sin campos de variante de Scryfall", []
    res, det = [], []
    for canon in p["_variantes"]:
        regla = REGLAS_VARIANTE.get(canon)
        ok = regla(extra, cand) if regla else None
        det.append(f"'{canon}' en el producto -> {'no se puede saber' if ok is None else ok}")
        if ok is not None:
            res.append((canon, ok))
    for canon, regla in REGLAS_INVERSAS.items():
        if canon not in p["_variantes"] and regla(extra, cand):
            res.append((canon, False))
            det.append(f"la impresion es '{canon}' y el producto no lo indica")
    for v in p["_versiones"]:
        regla = REGLAS_VERSION.get(v)
        ok = regla(extra, cand) if regla else None
        det.append(f"version '{v}' -> {'sin regla/dato en Scryfall' if ok is None else ok}")
        if ok is not None:
            res.append(("version:" + v, ok))
    contra = [k for k, ok in res if ok is False]
    if contra:
        return "contradice", "; ".join(det), contra
    if any(ok is True for _, ok in res):
        return "coincide", "; ".join(det), []
    return "ausente", "; ".join(det), []


def senal_tratamiento(p, cand):
    """Surge/Galaxy/... foil: el producto lo declara y Scryfall lo marca en promo_types (o a la inversa)."""
    extra = cand["extra"]
    if extra is None:
        return "ausente", "sin campos de variante de Scryfall"
    pt = set(extra["promo_types"])
    res, det = [], []
    for canon in p["_tratamientos"]:
        tags = TRATAMIENTOS_FOIL[canon][1]
        if not tags:
            det.append(f"'{canon}' sin campo conocido en Scryfall")
            continue
        ok = bool(set(tags) & pt)
        res.append(ok)
        det.append(f"'{canon}' en el producto -> {'promo_types lo trae' if ok else 'promo_types NO lo trae ' + str(sorted(pt))}")
    declarados = {t for c in p["_tratamientos"] for t in TRATAMIENTOS_FOIL[c][1]}
    de_mas = (pt & TAGS_TRATAMIENTO) - declarados
    if de_mas:
        res.append(False)
        det.append(f"la impresion trae tratamiento {sorted(de_mas)} y el producto no lo indica")
    if any(x is True for x in res) and not de_mas:
        return "coincide", "; ".join(det)
    if any(x is False for x in res):
        # el producto declara el tratamiento y la impresion no trae NINGUN tratamiento marcado: puede ser dato ausente en Scryfall
        if declarados and not (pt & TAGS_TRATAMIENTO):
            return "suave", "; ".join(det) + " (Scryfall no marca tratamientos en esta impresion: no se descarta)"
        return "contradice", "; ".join(det)
    return "ausente", "; ".join(det) or "sin tratamiento"


ORDEN_PASOS = ["numero", "indicador", "anio", "set", "acabado", "tratamiento", "variante"]   # orden de validacion


def evaluar(p, cands):
    """Evalua UNA POR UNA todas las candidatas. Devuelve la lista de evaluaciones (con puntaje y vetos)."""
    if p["_xvx"]:     # VvK: duel decks de esta carta; si las iniciales del nombre del set coinciden, solo esas
        dd = [c for c in cands if es_duel_deck(c)]
        p["_xvx_ok"] = {c["cand_id"] for c in ([c for c in dd if iniciales_duel_deck(c) == p["set_normalizado"]] or dd)}
    if p["_wc"]:      # WC: de ese anio, solo las del jugador que dice Hareruya (si alguna coincide)
        del_anio = [c for c in cands if c["set_code"] == p["set_normalizado"]]
        suyas = {c["cand_id"] for c in del_anio if prefijo_jugador(c["collector_number"]) in p["_wc"]}
        p["_wc_ok"] = suyas or None
    por_set = [c for c in cands if nivel_set(p, c) not in ("contradice",)]
    numeros_set = {norm_num(c["collector_number"]) for c in por_set} or {norm_num(c["collector_number"]) for c in cands}
    numeros_set |= {partir_numero(x)[0] for x in numeros_set}
    numeros_set |= {norm_num(x.rsplit("-", 1)[1]) for x in numeros_set if x and "-" in x}   # The List: 'isd-123' -> 123      # 173s / 173p tambien se alcanzan con '173'
    validos = [t for t in p["numero_tokens"] if t in numeros_set]   # un token solo cuenta si es un numero real de ese set
    p["numero_validado"] = validos
    p["numero_no_validado"] = [t for t in p["numero_tokens"] if t not in numeros_set]
    codigo_a_nombre = {c["set_code"]: norm(c["set_name"]) for c in cands if "-" not in (c["collector_number"] or "")}
    palabras = set(re.findall(r"[a-z0-9]{2,5}", p["_sin_nombre"])) - {"the", "list", "foil", "etched", "plst", "non"}
    evals = []
    etched_keys = {(c["set_code"], norm_num(c["collector_number"])) for c in cands if c["finish"] == "etched"}
    for c in cands:
        s_set, s_num, s_fin = nivel_set(p, c), senal_numero(validos, c, p), senal_acabado(p, c, etched_keys)
        cn = c["collector_number"] or ""
        if "-" in cn and s_num != "coincide" and not validos and (p["set_equivalencia"] == "plst" or p["set_normalizado"] == "plst"):
            # The List con varias impresiones: 'ICE-64' viene de ICE. Si el producto nombra ese set (codigo o nombre), esa es la impresion
            origen = norm(cn).split("-")[0]
            nombre_origen = codigo_a_nombre.get(origen)
            if origen in palabras or (nombre_origen and re.search(r"(?<![a-z0-9])" + re.escape(nombre_origen) + r"(?![a-z0-9])", p["_sin_nombre"])):
                s_num = "coincide"
        if p.get("_sld_cmd") and any(r == f"{c['set_code']}-{norm_num(c['collector_number'])}" for r in p["_ref_origen"]):
            s_num = "coincide"                     # (WOC-28): la carta normal de ese set y numero
        if p["_ref_origen"] and norm_num(c["collector_number"]) in p["_ref_origen"]:
            # numero literal tipo 'M11-221' que Scryfall trae tal cual: identifica la impresion aunque el set se escriba distinto
            s_num = "coincide"
            if s_set == "contradice" and not por_set:     # solo si ninguna impresion encaja con el set escrito
                s_set = "equiv"
        s_var, det_var, contra_var = senal_variante(p, c)
        s_tra, det_tra = senal_tratamiento(p, c)
        s_ind, det_ind = senal_indicador(p, c)
        s_nom, det_nom = senal_nombre(p, c)
        s_anio, det_anio = senal_anio(p, c)
        if s_num == "ausente" and not validos and p["numero_no_validado"] and s_set in ("equiv", "alias") \
                and "-" not in (c["collector_number"] or ""):
            s_num = "contradice"   # una equivalencia de set no puede ocultar un numero que no existe en esa edicion
        base_ok = s_num == "coincide" and s_set in ("exacto", "alias", "equiv", "base", "base_p", "promo_nombre")
        if s_var == "contradice" and base_ok and set(contra_var) <= VISUALES_SUAVES:
            s_var = "suave"          # numero + set + nombre coinciden: una marca visual (Borderless...) no descarta
        if s_tra == "contradice" and base_ok:
            s_tra = "suave"
        pts = {"numero": PESOS["numero"].get(s_num, 0), "set": PESOS["set"].get(s_set, 0),
               "acabado": PESOS["acabado"].get(s_fin, 0), "nombre": PESOS["nombre"]["ambos" if s_nom == "ambos" else "coincide"],
               "variante": PESOS["variante"].get(s_var, 0), "indicador": PESOS["indicador"].get(s_ind, 0),
               "tratamiento": PESOS["tratamiento"].get(s_tra, 0),
               "anio": PESOS["anio"].get(s_anio, 0)}
        senales = {"numero": s_num, "indicador": s_ind, "set": s_set, "acabado": s_fin, "variante": s_var, "tratamiento": s_tra, "nombre": s_nom, "anio": s_anio}
        vetos = [k for k in ORDEN_PASOS if senales[k] == "contradice"]
        evid = int(s_num == "coincide") + int(s_set in ("exacto", "alias", "equiv", "base", "base_p", "promo_nombre")) + int(s_fin == "coincide") + int(s_nom == "ambos") + int(s_anio == "coincide")
        evals.append({
            "source_id": p["source_id"], "cand": c, "cand_id": c["cand_id"], "senales": senales, "puntos": pts,
            "_esp": especiales_no_declarados(p, c), "score": sum(pts.values()), "vetos": vetos, "viable": not vetos, "detalle_variante": det_var, "detalle_tratamiento": det_tra,
            "detalle_indicador": det_ind, "detalle_nombre": det_nom, "detalle_anio": det_anio, "paso_fallido": vetos[0] if vetos else "", "evidencias": evid,
            "coincidencias": [k for k, v in senales.items() if v in ("coincide", "exacto", "alias", "equiv", "base", "base_p", "promo_nombre", "ambos")],
            "contradicciones": vetos, "ausentes": [k for k, v in senales.items() if v in ("ausente", "desconocido")]})
    # Version normal: si el producto no declara Showcase/Borderless/... y existe una impresion normal igual de buena,
    # las especiales pierden puntos (no se descartan).
    for e in evals:
        if e["_esp"] and e["senales"]["numero"] != "coincide" and not e["vetos"]:
            hay_normal = any(o is not e and not o["_esp"] and not o["vetos"] and o["cand"]["finish"] == e["cand"]["finish"]
                             and o["senales"]["set"] not in ("contradice", "ausente") for o in evals)
            if hay_normal:
                e["puntos"]["especial"] = PESOS["especial"]["no_declarado"]
                e["score"] += PESOS["especial"]["no_declarado"]
                e["detalle_variante"] += f"; impresion especial {sorted(e['_esp'])} que el producto no declara (hay una normal)"
    return evals


def especiales_no_declarados(p, c):
    ex = c["extra"]
    if not ex:
        return set()
    s = set()
    if "showcase" in ex["frame_effects"]:
        s.add("showcase")
    if ex["border_color"] == "borderless":
        s.add("borderless")
    if "extendedart" in ex["frame_effects"]:
        s.add("extended art")
    if ex["full_art"]:
        s.add("full art")
    if "boosterfun" in ex["promo_types"]:
        s.add("booster fun")
    if "alternate frame" in p["_variantes"]:
        s -= {"showcase", "borderless", "booster fun"}     # 'Alternate Frame' ya declara el marco especial
    return s - set(p["_variantes"])


def texto_cands(evals, n=4):
    return " | ".join(f"{e['cand_id']} {e['cand']['set_code']}#{e['cand']['collector_number']} {e['cand']['finish']}"
                      f"{'' if e['cand']['en_ref'] else ' [<$2]'} ({e['score']})" for e in evals[:n])


def decidir(p, evals, universo_completo):
    """confirmado / ambiguo / sin_coincidencia, con la candidata elegida, el puntaje y el motivo."""
    d = {"estado": None, "motivo": None, "confianza": None, "cand_id": None, "score": None, "segundo_score": None,
         "margen": None, "coincidencias": [], "contradicciones": [], "ausentes": [], "candidatos": None,
         "candidatos_ref_ids": [], "alertas": [], "detalle": None, "paso_fallido": "", "evidencias": None,
         "relacion_busqueda": None}
    if p["clase_producto"] is not None and str(p["clase_producto"]) in CLASES_NO_CARTA:
        return {**d, "estado": "no_es_carta", "motivo": "clase_no_carta", "detalle": str(p["clase_producto"])}
    emblemas = [c for c in p["_caras"] if re.search(r"(?<![a-z])emblem(?![a-z])", c) and c not in EMBLEMAS_REALES]
    if emblemas:
        return {**d, "estado": "no_es_carta", "motivo": "emblema_de_otra_carta", "detalle": p["_nombre_en"]}
    rel, det_nombre = clasificar_nombre(p["_fila"], p["_nombres_validos"])
    d["relacion_busqueda"] = {"coincide": "propio", "ajeno": "ajeno", "incierto": "incierto"}[rel]
    if rel == "ajeno":       # producto ajeno a ESTA busqueda: se conserva con todos sus datos, solo se clasifica
        return {**d, "estado": "no_es_carta", "motivo": "nombre_distinto", "detalle": f"{p['_nombre_en']} | {det_nombre}"}
    if rel == "incierto":    # no se puede interpretar con seguridad: se conserva como candidato para revision
        return {**d, "estado": "ambiguo", "motivo": "nombre_no_interpretable", "detalle": det_nombre}
    palabra = palabra_no_carta(p)
    if palabra:
        return {**d, "estado": "no_es_carta", "motivo": "producto_no_carta", "detalle": f"palabra: {palabra}"}

    ordenadas = sorted(evals, key=lambda e: -e["score"])
    viables = [e for e in ordenadas if e["viable"]]
    if p["finish_conflicto"]:
        d["alertas"].append("acabado: el texto y foil_flg se contradicen")
    if p["sufijo_set"] and p["sufijo_set"] not in HINTS_SUFIJO:
        d["alertas"].append(f"sufijo '{p['sufijo_set']}' sin interpretar")
    if p["numero_no_validado"]:
        d["alertas"].append(f"numeros del texto que no corresponden a ninguna impresion: {p['numero_no_validado']}")
    if len(p["_ind"]) > 1:
        return {**d, "estado": "ambiguo", "motivo": "indicadores_en_conflicto", "detalle": str(sorted(p["_ind"]))}
    if not universo_completo:
        d["alertas"].append("sin control del bulk: puede existir otra version de < $2 que compita")

    if not viables and p["_pista_j"] and "judge" not in p["_ind"] and any(
            e["cand"]["set_code"] == p["_pista_j"] and "judge" in tipo_cand(e["cand"]) for e in evals):
        return {**d, "estado": "ambiguo", "motivo": "judge_vs_jumpstart", "candidatos": texto_cands(ordenadas),
                "detalle": f"J## ({p['_pista_j']}) sin la palabra Judge; solo hay impresiones Judge con ese codigo"}
    if not viables:  # ninguna candidata sobrevive a las contradicciones
        con_set = [e for e in ordenadas if e["senales"]["set"] != "contradice"]
        if not con_set and p["_set_texto"] and "promo" in p["_set_texto"][1]:
            return {**d, "estado": "sin_coincidencia", "motivo": "promo_sin_parecida", "candidatos": texto_cands(ordenadas),
                    "detalle": f"{p['set_original']!r}: ninguna promo de Scryfall de esta carta se parece"}
        if not con_set:           # ninguna impresion encaja con el set (o con el indicador FNM/Judge): no hay candidata que explicar
            return {**d, "estado": "sin_coincidencia", "motivo": "set_sin_candidato", "candidatos": texto_cands(ordenadas),
                    "detalle": f"set del producto: {p['set_original']!r}; ninguna impresion de Scryfall encaja; acabados compatibles: {p['finish_compatibles']}"}
        if p["set_normalizado"] and not con_set:
            motivo = "set_sin_candidato"
        elif not p["set_normalizado"] and not p["_ind"] and not p["set_equivalencia"]:
            motivo = "sin_set"
        else:
            # el motivo lo da la candidata que MAS se parecia (mayor puntaje bruto), no el conjunto de todas
            vetos = con_set[0]["vetos"]
            motivo = ("numero_no_corresponde" if "numero" in vetos
                      else "anio_judge_no_corresponde" if "anio" in vetos
                      else ("judge_sin_candidata" if "judge" in p["_ind"] else "serializada_sin_candidata" if "serializada" in p["_ind"]
                                              else "promo_no_corresponde") if "indicador" in vetos
                      else "acabado_no_disponible" if "acabado" in vetos else "tratamiento_contradice" if "tratamiento" in vetos else "variante_contradice")
            d["paso_fallido"] = con_set[0]["paso_fallido"]
        return {**d, "estado": "sin_coincidencia", "motivo": motivo, "candidatos": texto_cands(con_set or ordenadas),
                "detalle": f"set del producto: {p['set_original']!r}; acabados compatibles: {p['finish_compatibles']}"}

    top = viables[0]
    d.update(cand_id=top["cand_id"], score=top["score"], coincidencias=top["coincidencias"],
             contradicciones=top["contradicciones"], ausentes=top["ausentes"],
             candidatos=texto_cands(viables), candidatos_ref_ids=[e["cand"]["ref_id"] for e in viables if e["cand"]["ref_id"]])
    if len(viables) > 1:
        d["segundo_score"], d["margen"] = viables[1]["score"], top["score"] - viables[1]["score"]
    s = top["senales"]
    d["evidencias"] = top["evidencias"]
    if p["_n_grupos"] > 1 and s["nombre"] != "ambos":
        # numero + set + acabado ya identifican la impresion: el 2o nombre (a menudo flavor_name que Scryfall no trae) no la discrimina
        # Doble cara / dos nombres (Strixhaven y similares): basta con que UNO de los nombres sea esta carta si set y acabado la
        # identifican (con o sin numero) y nada en Scryfall contradice al otro nombre.
        if s["numero"] in ("coincide", "ausente") and s["set"] in ("exacto", "alias", "equiv", "base") and s["acabado"] == "coincide" \
                and s["nombre"] in ("parcial", "desconocido") and nombres_compatibles(p, top["cand"]):
            d["alertas"].append("dos nombres: solo uno verificado contra Scryfall; confirmado por numero + set + acabado (" + top["detalle_nombre"] + ")")
        else:
            return {**d, "estado": "ambiguo", "motivo": "nombres_no_resueltos", "cand_id": None,
                    "detalle": top["detalle_nombre"]}
    if s["variante"] == "suave":
        d["alertas"].append("marca visual del producto no confirmada por Scryfall (" + top["detalle_variante"] + "); aceptada porque numero y set coinciden")
    if s["tratamiento"] == "suave":
        d["alertas"].append("tratamiento foil no coincide con promo_types (" + top["detalle_tratamiento"] + "); aceptado porque numero y set coinciden")
    if p.get("referencia_origen"):
        d["alertas"].append(f"referencia de origen {p['referencia_origen']} conservada; no se usa como numero de coleccion")
    if s["set"] == "promo_nombre":
        d["alertas"].append(f"set tomado por la palabra Promo en el nombre del set: {top['cand']['set_name']!r}")
    if p["_ref_origen"] and norm_num(top["cand"]["collector_number"]) in p["_ref_origen"]:
        d["alertas"].append(f"numero literal {top['cand']['collector_number']!r} coincide con la referencia del producto")
    if top["cand"]["set_code"] in p["_set_alt"]:
        d["alertas"].append(f"set tomado de la ultima parte de {p['set_original']!r}: {top['cand']['set_code']}")
    if p["set_equivalencia"] and top["cand"]["set_code"] == p["set_equivalencia"]:
        d["alertas"].append(f"equivalencia de set aplicada: {p['equivalencia_aplicada']} (no es confirmacion por si sola)")
    if p["_pista_j"] and "judge" not in p["_ind"] and any(
            e["cand"]["set_code"] == p["_pista_j"] and "judge" in tipo_cand(e["cand"]) for e in evals):
        d["alertas"].append(f"el codigo {p['_pista_j']} tambien tiene una impresion Judge; se eligio por el nombre de la edicion")
    exigir = bool(p["_ind"]) or p["_n_grupos"] > 1 or top["cand"]["set_code"] == "sld"
    # Indicador explicito (Judge Foil, prerelease...) + acabado identificado + UNICA candidata viable en TODO el universo
    # de Scryfall (el resto se descarto por contradiccion): el indicador cuenta como evidencia de identidad.
    unica = bool(p["_ind"]) and universo_completo and len(viables) == 1 and s["indicador"] == "coincide" \
        and s["acabado"] == "coincide"
    if unica and top["evidencias"] < MIN_EVIDENCIAS_PROMO:
        d["alertas"].append("confirmado por indicador explicito + unica candidata compatible (las demas se descartaron)")
    evid_total = top["evidencias"] + (1 if unica else 0)
    if exigir and evid_total < MIN_EVIDENCIAS_PROMO:
        return {**d, "estado": "ambiguo", "motivo": "identidad_promo_insuficiente", "cand_id": None,
                "detalle": f"evidencias independientes: {top['evidencias']} (minimo {MIN_EVIDENCIAS_PROMO}); "
                           f"mejor candidata: {top['cand']['set_code']}#{top['cand']['collector_number']} {top['cand']['finish']}"}
    if p["_versiones"] and not any(v in REGLAS_VERSION for v in p["_versiones"]):
        d["alertas"].append(f"tipo de version {p['tipo_version']} conservado; Scryfall no tiene regla para distinguirlo (REGLAS_VERSION)")
    if s["set"] in ("ausente", "contradice") and not unica:
        return {**d, "estado": "ambiguo", "motivo": "sin_set", "cand_id": None}
    if s["acabado"] != "coincide":
        return {**d, "estado": "ambiguo", "motivo": "evidencia_minima_incompleta",
                "detalle": "el acabado no esta identificado sin ambiguedad (" + p["finish_fuente"] + ")", "cand_id": None}
    if len(viables) > 1 and d["margen"] < MARGEN_MIN:
        return {**d, "estado": "ambiguo", "motivo": "varias_candidatas_similares", "cand_id": None}
    if top["score"] < UMBRAL_CONFIRMAR and not unica:
        return {**d, "estado": "ambiguo", "motivo": "puntuacion_insuficiente", "cand_id": None}
    if not top["cand"]["en_ref"]:
        return {**d, "estado": "sin_coincidencia", "motivo": "fuera_del_universo_2usd",
                "detalle": f"mejor candidata: {top['cand']['set_code']}#{top['cand']['collector_number']} "
                           f"{top['cand']['finish']} (${top['cand']['precio_scryfall_usd']})"}
    d["confianza"] = "alta" if s["numero"] == "coincide" else "media"
    d["evidencias"] = evid_total
    if s["numero"] == "ausente":
        d["alertas"].append("confirmado sin numero de coleccion (unica candidata compatible)")
    elif s["numero"] == "parcial":
        d["alertas"].append(f"el numero coincide solo en los digitos ({p['numero_validado']} vs {top['cand']['collector_number']})")
    return {**d, "estado": "confirmado", "motivo": "ok"}





# ---------------------------------------------------------------- version de las reglas
VERSION_REGLAS_BASE = "2026.10-r1"      # subir a mano cuando cambie la LOGICA (no solo las tablas)

def _canon(x):
    """Representacion estable (independiente del orden de sets/dicts y de la semilla de hash de Python)."""
    if isinstance(x, dict):
        return "{" + ",".join(sorted(f"{_canon(k)}:{_canon(v)}" for k, v in x.items())) + "}"
    if isinstance(x, (set, frozenset)):
        return "{" + ",".join(sorted(_canon(v) for v in x)) + "}"
    if isinstance(x, (list, tuple)):
        return "[" + ",".join(_canon(v) for v in x) + "]"
    if hasattr(x, "pattern"):
        return f"re:{x.pattern}"
    return repr(x)


def version_reglas(hvs):
    """Version manual + huella de las tablas de reglas del comparador: cualquier cambio de pesos, alias o umbrales cambia la huella."""
    partes = []
    for nombre in ("PESOS", "SET_ALIAS", "SET_EQUIV", "SET_EQUIV_POR_NOMBRE", "SET_TEXTO", "ALIAS_VARIANTE", "SUFIJO_A_VARIANTE",
                   "TRATAMIENTOS_FOIL", "EMBLEMAS_REALES", "UMBRAL_CONFIRMAR", "MARGEN_MIN", "MIN_EVIDENCIAS_PROMO", "IDIOMA_HARERUYA",
                   "CONDICION_HARERUYA", "DESCARTAR_IRRELEVANTES"):
        partes.append(f"{nombre}={_canon(getattr(hvs, nombre, None))}")
    h = hashlib.sha1("\n".join(partes).encode("utf-8")).hexdigest()[:8]
    return f"{VERSION_REGLAS_BASE}+{h}"



VERSION_REGLAS = version_reglas(sys.modules[__name__])     # cambia si cambian las tablas o umbrales: obliga a emparejar otra vez



# ====================================================================================================
# C. MOTOR SIN RED: universo de impresiones y emparejado de productos ya descargados
# ====================================================================================================
CARPETA_TIENDA = CARPETA_DATA / "tiendas" / "hareruya"   # todo lo de Hareruya
CARPETA_SECO = CARPETA_TIENDA / "descargas"             # precios tal como los publica Hareruya (lo que baja el scraper)
CARPETA_CACHE = CARPETA_TIENDA / "emparejado"           # emparejados e indice del bulk (se pueden borrar: se rehacen)
RUTA_PROGRESO = CARPETA_TIENDA / "progreso.json"        # avance de la descarga (lo muestra Stack)
PATRON_SECO = "hareruya_seco_*.jsonl.gz"
VERSION_SECO = 1
VERSION_EMPAREJADO = 5       # sube si cambia el formato del emparejado (obliga a emparejar otra vez)
MP_CONTEXT = "spawn"         # igual en Windows, Linux y Mac
RE_NOMBRE_BULK = re.compile(r'"name"\s*:\s*"((?:[^"\\]|\\.)*)"')
RE_PREFIJO = re.compile(r"^\d+-")


RUTA_HISTORIAL = CARPETA_DATA / "precios" / "historial.sqlite"     # lo genera scryfall.py (catalogo, search_list)
CARPETA_SCRYFALL = CARPETA_DATA / "precios" / "scryfall"           # el default-cards mas reciente


def dia_catalogo():
    """Fecha del catalogo (el dia mas reciente del historial) o None si todavia no hay historial."""
    if not RUTA_HISTORIAL.exists():
        return None
    try:
        con = abrir_catalogo(RUTA_HISTORIAL)
        try:
            r = con.execute("SELECT MAX(fecha) FROM dias").fetchone()
        finally:
            con.close()
    except sqlite3.Error:
        return None
    return r[0] if r else None


def leer_search_list(ruta_catalogo=None):
    """[{name, buscar_como}] de la search_list del historial (solo lectura), sin repetidos."""
    con = abrir_catalogo(Path(ruta_catalogo or RUTA_HISTORIAL))
    try:
        filas = con.execute("SELECT name, buscar_como FROM search_list ORDER BY name").fetchall()
    finally:
        con.close()
    vistos, cartas = set(), []
    for name, como in filas:
        if (name or "").strip() and (como or "").strip() and name.casefold() not in vistos:
            vistos.add(name.casefold())
            cartas.append({"name": name, "buscar_como": como})
    return cartas


def bulk_del_dia(_=None):
    """El default-cards mas reciente (scryfall.py guarda solo uno, en data/precios/scryfall/)."""
    if not CARPETA_SCRYFALL.is_dir():
        return None
    cand = [p for p in CARPETA_SCRYFALL.iterdir() if p.is_file() and "cards" in p.name.lower() and not p.name.startswith("_parcial_")
            and p.name.lower().endswith((".jsonl.gz", ".jsonl"))]
    return max(cand, key=lambda p: p.stat().st_mtime) if cand else None


def construir_indice(ruta_bulk, claves=None):
    """UNA lectura del bulk -> {nombre normalizado: [impresiones]} (mismo criterio que el stress test). Optimizado: el nombre se
    lee de la linea con una expresion regular y solo se decodifica el JSON de las cartas que se van a consultar."""
    abrir = gzip.open if ruta_bulk.name.lower().endswith(".gz") else open
    indice = {}
    with abrir(ruta_bulk, "rt", encoding="utf-8") as f:
        for linea in f:
            if claves is not None:
                m = RE_NOMBRE_BULK.search(linea)
                if m:
                    try:
                        nombre = json.loads(f'"{m.group(1)}"').strip()
                    except ValueError:
                        nombre = None
                    if nombre is not None and not ({norm(nombre)} | {norm(x) for x in nombre.split(" // ")}) & claves:
                        continue
            try:
                c = json.loads(linea.strip().rstrip(","))
            except ValueError:
                continue
            if not isinstance(c, dict):
                continue
            nombre = (c.get("name") or "").strip()
            ks = {norm(nombre)} | {norm(x) for x in nombre.split(" // ")}
            if claves is not None and not (ks & claves):
                continue
            imp = impresion_de_carta(c)
            if imp is None:
                continue
            for k in ks:
                indice.setdefault(k, []).append(imp)
    return indice


def indice_del_dia(_=None, avisar=print):
    """Indice del bulk mas reciente para las cartas de la search_list, guardado en disco: se lee el bulk UNA vez por dia
    (lo comparten la descarga, el emparejado y cada proceso). None si no hay default-cards."""
    import pickle
    bulk = bulk_del_dia()
    if bulk is None:
        return None
    st = bulk.stat()
    ruta = CARPETA_CACHE / f"indice_{dia_catalogo()}_{st.st_size}_{int(st.st_mtime)}.pkl"
    if ruta.exists():
        try:
            with open(ruta, "rb") as f:
                return pickle.load(f)
        except Exception:                            # noqa: BLE001  (archivo cortado: se rehace)
            ruta.unlink(missing_ok=True)
    claves = set()
    for c in leer_search_list():
        claves |= {norm(c["name"]), norm(c["buscar_como"])}
    avisar(f"Leyendo {bulk.name} una vez para {len(claves):,} nombres (solo la primera vez de este dia) ...")
    indice = construir_indice(bulk, claves)
    CARPETA_CACHE.mkdir(parents=True, exist_ok=True)
    temporal = ruta.with_name(ruta.name + ".part")
    with open(temporal, "wb") as f:
        pickle.dump(indice, f, protocol=pickle.HIGHEST_PROTOCOL)
    temporal.replace(ruta)
    for viejo in sorted(CARPETA_CACHE.glob("indice_*.pkl"), key=lambda p: p.stat().st_mtime, reverse=True)[2:]:
        viejo.unlink(missing_ok=True)
    return indice


def universo_de(indice, carta):
    vistos, imps = set(), []
    for k in {norm(carta["name"]), norm(carta["buscar_como"])}:
        for imp in indice.get(k, []):
            if id(imp) not in vistos:
                vistos.add(id(imp))
                imps.append(imp)
    return imps


def preparar(con, indice, carta):
    """Lo que se necesita de Scryfall para una carta: (cands, nombres_validos, universo_ok). cands = [] si ya no esta en el catalogo."""
    refs = cargar_referencia(con, carta["name"])
    universo = universo_de(indice, carta) if indice is not None else None
    cands = construir_candidatos(refs, universo) if refs else []
    return cands, nombres_validos_carta(carta, cands), universo is not None


def emparejar_registros(idx, registros, cands, nombres, universo_ok):
    """Productos de Hareruya de UNA carta -> [(producto, decision)]. Mismas reglas y mismo orden que el motor del stress test
    (separar ajenos, extraer, evaluar, decidir); se omite solo lo que era para informes (evaluaciones, trazabilidad, ajenos)."""
    registros = [r for r in registros if not es_basura(r.get("fila") or {})]   # oversized, stickers chibi, fundas, Ultra Pro
    conservar, descartar = separar_irrelevantes(registros, nombres)
    if descartar and (DESCARTAR_IRRELEVANTES or not conservar):
        registros = conservar               # los ajenos no entran a las decisiones
    salida = []
    for reg in registros:
        reg = {**reg, "source_id": f"{idx:02d}-{RE_PREFIJO.sub('', str(reg['source_id']), count=1)}"}
        try:
            p = producto_hareruya(reg)
            p["_nombres_validos"] = nombres
            # con DESCARTAR_IRRELEVANTES todo lo que queda ya paso clasificar_nombre y no es ajeno: no se repite
            ajeno = not DESCARTAR_IRRELEVANTES and clasificar_nombre(p["_fila"], nombres)[0] == "ajeno"
            d = decidir(p, [] if ajeno else evaluar(p, cands), universo_ok)
            p["no_comparable"] = motivo_no_comparable(p) if d["estado"] == "confirmado" else None
        except Exception as e:                       # noqa: BLE001  (un producto raro no tumba la carta)
            p = {"source_id": reg["source_id"], "search_term": reg.get("search_term"), "_fila": reg.get("fila") or {}}
            d = {"estado": "error", "motivo": f"error_de_procesamiento: {type(e).__name__}: {e}"[:300], "cand_id": None}
        if d["estado"] == "no_es_carta":
            continue                                 # otra carta, emblema o producto sellado: no se guarda
        salida.append((p, d))
    return salida


# ---------------------------------------------------------------- archivos secos y emparejados
def archivos_secos():
    """Precios secos de Hareruya, del mas reciente al mas antiguo."""
    if not CARPETA_DATA.is_dir():
        return []
    return sorted(CARPETA_SECO.glob(PATRON_SECO), key=lambda p: p.stat().st_mtime, reverse=True) if CARPETA_SECO.is_dir() else []


def leer_seco(ruta):
    """Una entrada por carta buscada: {i, name, buscar_como, estado, detalle, capturado, registros: [...]}."""
    with gzip.open(ruta, "rt", encoding="utf-8") as f:
        for linea in f:
            if linea.strip():
                yield json.loads(linea)


def meta_seco(ruta):
    try:
        return json.loads(Path(str(ruta).replace(".jsonl.gz", ".meta.json")).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def dia_de_seco(ruta_seco, meta=None):
    """Dia de una captura = el dia (hora de tu computadora) en que EMPEZO, que es el que lleva el nombre del archivo
    (hareruya_seco_AAAAmmdd_HHMMSS). Antes se usaba el 'fin' del .meta.json, que esta en hora UTC: una descarga que
    empezaba el 18 en la noche terminaba el 19 UTC y le ganaba el dia a la del 19 (las dos quedaban como el mismo dia y
    la segunda borraba a la primera del historial)."""
    m = re.search(r"_(\d{4})(\d{2})(\d{2})_\d{6}", Path(str(ruta_seco or "")).name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    ini = (meta if meta is not None else meta_seco(ruta_seco)).get("inicio") or ""
    try:                                               # 'inicio' viene en UTC: se pasa a la hora local
        return datetime.fromisoformat(ini.replace("Z", "+00:00")).astimezone().date().isoformat()
    except ValueError:
        pass
    fin = ((meta if meta is not None else meta_seco(ruta_seco)).get("fin") or "")[:10]
    return fin if PATRON_FECHA.match(fin) else ""


def dia_de_emparejado(meta):
    """El dia de la captura de un emparejado, con la regla de dia_de_seco (los emparejados viejos guardaban el dia UTC)."""
    seco = meta.get("seco") or ""
    d = dia_de_seco(seco, {}) if seco else ""
    return d if PATRON_FECHA.match(d) else (meta.get("seco_dia") or "")


def ruta_cache(ruta_seco, carpeta_catalogo):
    return CARPETA_CACHE / f"emparejado_{Path(ruta_seco).name.replace('.jsonl.gz', '')}__{Path(carpeta_catalogo).name}.sqlite"


def firma(ruta):
    st = Path(ruta).stat()
    return f"{st.st_size}:{int(st.st_mtime)}"


def cache_valido(ruta_seco, carpeta_catalogo):
    """Ruta del emparejado si ya existe, esta completo y es de ESTE archivo seco (no de uno reescrito al reanudar)."""
    ruta = ruta_cache(ruta_seco, carpeta_catalogo)
    if not ruta.exists():
        return None
    try:
        con = abrir_catalogo(ruta)
        try:
            m = dict(con.execute("SELECT key, value FROM meta"))
        finally:
            con.close()
    except sqlite3.Error:
        return None
    ok = (m.get("completo") == "1" and m.get("firma_seco") == firma(ruta_seco) and m.get("version") == str(VERSION_EMPAREJADO)
          and m.get("reglas") == VERSION_REGLAS)
    return ruta if ok else None


# ====================================================================================================
# D. EMPAREJADO (lo hace Stack en segundo plano; tambien: python hareruya.py emparejar)
# ====================================================================================================
# Mismas columnas que las corridas del stress test (hareruya_productos + decisiones): Stack las lee igual.
COLS_PROD = ["source_id", "nombre_buscado", "search_term", "id_producto", "product_name_en_original", "product_name_original",
             "set_original", "idioma", "idioma_raw", "condicion", "condicion_raw", "precio_jpy", "stock", "capturado_utc"]
COLS_DEC = ["source_id", "nombre_buscado", "estado", "motivo", "confianza", "scryfall_id", "acabado_scryfall", "set_scryfall",
            "numero_scryfall", "precio_scryfall_usd", "score", "segundo_score", "no_comparable"]


def _filas(nombre, pares, cands_por_id):
    prods, decs = [], []
    for p, d in pares:
        f = p.get("_fila") or {}
        c = cands_por_id.get(d.get("cand_id")) if d.get("cand_id") else None
        prods.append([p["source_id"], nombre, p.get("search_term"), p.get("id_producto", f.get("product")),
                      p.get("product_name_en_original", f.get("product_name_en")), p.get("product_name_original", f.get("product_name")),
                      p.get("set_original", f.get("set_code")), p.get("idioma"), p.get("idioma_raw", f.get("language")),
                      p.get("condicion"), p.get("condicion_raw", f.get("condition")), p.get("precio_jpy", f.get("price_jpy")),
                      p.get("stock", f.get("stock")), p.get("capturado_utc", f.get("captured_at"))])
        decs.append([p["source_id"], nombre, d["estado"], d["motivo"], d.get("confianza"),
                     c["id"] if c else None, c["finish"] if c else None, c["set_code"] if c else None,
                     c["collector_number"] if c else None, c["precio_scryfall_usd"] if c else None,
                     d.get("score"), d.get("segundo_score"), p.get("no_comparable")])
    return prods, decs


def _crear_tablas(con):
    con.executescript(
        f"CREATE TABLE IF NOT EXISTS hareruya_productos ({', '.join(COLS_PROD)});"
        f"CREATE TABLE IF NOT EXISTS decisiones ({', '.join(COLS_DEC)});"
        "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);")


def _trabajador_emparejar(w, n, ruta_seco, ruta_catalogo, ruta_parcial, prog, stop):
    """Proceso w (0..n-1): empareja las cartas k con k % n == w y las escribe en su SQLite parcial."""
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)       # el Ctrl+C lo maneja el proceso principal
    except ValueError:
        pass
    try:
        indice = indice_del_dia(avisar=lambda *_: None)
        cat = abrir_catalogo(Path(ruta_catalogo))
        out = sqlite3.connect(ruta_parcial)
        _crear_tablas(out)
        hechas = 0
        with contextlib.redirect_stdout(io.StringIO()):
            for k, e in enumerate(leer_seco(ruta_seco)):
                if k % n != w:
                    continue
                if stop.value:
                    break
                registros = e.get("registros") or []
                if registros:
                    carta = {"name": e["name"], "buscar_como": e.get("buscar_como") or e["name"]}
                    cands, nombres, universo_ok = preparar(cat, indice, carta)
                    if cands:                               # si ya no esta en el catalogo >= $2 de este dia, no hay con que comparar
                        pares = emparejar_registros(e.get("i", k), registros, cands, nombres, universo_ok)
                        prods, decs = _filas(carta["name"], pares, {c["cand_id"]: c for c in cands})
                        out.executemany(f"INSERT INTO hareruya_productos VALUES ({','.join('?' * len(COLS_PROD))})", prods)
                        out.executemany(f"INSERT INTO decisiones VALUES ({','.join('?' * len(COLS_DEC))})", decs)
                        prog[2 * w + 1] += len(prods)
                hechas += 1
                prog[2 * w] = hechas
                if hechas % 200 == 0:
                    out.commit()
        out.commit()
        out.close()
        cat.close()
    except BaseException as e:                              # noqa: BLE001
        Path(str(ruta_parcial) + ".error").write_text(f"{type(e).__name__}: {e}\n{traceback.format_exc()}", encoding="utf-8")
        raise


def emparejar(ruta_seco=None, carpeta_catalogo=None, workers=None, progreso=None, parar=None):
    """Empareja un archivo seco contra un catalogo y guarda el resultado. progreso(hechas, total, productos) ~2 veces por
    segundo; parar() -> True lo detiene (InterruptedError). Devuelve la ruta del emparejado."""
    if ruta_seco is None:
        secos = archivos_secos()
        if not secos:
            raise FileNotFoundError("no hay precios secos de Hareruya (python hareruya.py descargar)")
        ruta_seco = secos[0]
    if carpeta_catalogo is None:                  # = fecha del catalogo (el dia mas reciente del historial)
        carpeta_catalogo = dia_catalogo()
        if not carpeta_catalogo:
            raise FileNotFoundError(f"no hay historial de precios ({RUTA_HISTORIAL}); corre scryfall.py")
    ruta_seco, carpeta_catalogo = Path(ruta_seco), Path(carpeta_catalogo)
    total = sum(1 for _ in leer_seco(ruta_seco))
    workers = max(1, min(workers or max(1, min(4, (os.cpu_count() or 2) - 1)), total or 1))
    t0 = time.time()
    indice_del_dia()                          # se arma UNA vez aqui; los procesos solo lo leen
    destino = ruta_cache(ruta_seco, carpeta_catalogo)
    destino.parent.mkdir(parents=True, exist_ok=True)
    parciales = [destino.with_name(destino.stem + f".w{w}.part.sqlite") for w in range(workers)]
    for p in parciales:
        p.unlink(missing_ok=True)
        Path(str(p) + ".error").unlink(missing_ok=True)
    ctx = multiprocessing.get_context(MP_CONTEXT)
    prog = ctx.Array("q", 2 * workers, lock=False)
    stop = ctx.Value("i", 0, lock=False)
    procs = [ctx.Process(target=_trabajador_emparejar, daemon=True,
                         args=(w, workers, str(ruta_seco), str(RUTA_HISTORIAL), str(parciales[w]), prog, stop))
             for w in range(workers)]
    for p in procs:
        p.start()
    try:
        while any(p.is_alive() for p in procs):
            if parar and parar():
                stop.value = 1
            if progreso:
                progreso(sum(prog[2 * w] for w in range(workers)), total, sum(prog[2 * w + 1] for w in range(workers)))
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop.value = 1
        for p in procs:
            p.join()
        raise
    errores = [Path(str(p) + ".error") for p in parciales if Path(str(p) + ".error").exists()]
    if errores:
        raise RuntimeError("fallo un proceso del emparejado: " + errores[0].read_text(encoding="utf-8").splitlines()[0])
    if stop.value:
        for p in parciales:
            p.unlink(missing_ok=True)
        raise InterruptedError("emparejado detenido")
    if any(p.exitcode for p in procs):
        raise RuntimeError(f"un proceso del emparejado termino con codigo {[p.exitcode for p in procs]}")
    temporal = destino.with_name(destino.name + ".part")      # si algo falla, no queda un archivo a medias con el nombre bueno
    temporal.unlink(missing_ok=True)
    out = sqlite3.connect(temporal)
    try:
        _crear_tablas(out)
        for p in parciales:
            out.execute("ATTACH DATABASE ? AS w", (str(p),))
            out.execute("INSERT INTO hareruya_productos SELECT * FROM w.hareruya_productos")
            out.execute("INSERT INTO decisiones SELECT * FROM w.decisiones")
            out.commit()
            out.execute("DETACH DATABASE w")
        out.execute("CREATE INDEX IF NOT EXISTS idx_dec_sid ON decisiones(source_id)")
        out.execute("CREATE INDEX IF NOT EXISTS idx_prod_sid ON hareruya_productos(source_id)")
        n_prod = out.execute("SELECT COUNT(*) FROM hareruya_productos").fetchone()[0]
        n_conf = out.execute("SELECT COUNT(*) FROM decisiones WHERE estado = 'confirmado'").fetchone()[0]
        ms = meta_seco(ruta_seco)
        meta = {"version": VERSION_EMPAREJADO, "reglas": VERSION_REGLAS, "completo": 1, "seco": str(ruta_seco),
                "firma_seco": firma(ruta_seco), "seco_dia": dia_de_seco(ruta_seco, ms), "seco_completo": int(bool(ms.get("completo"))),
                "capturado_desde": ms.get("inicio"), "capturado_hasta": ms.get("fin"), "catalogo_dia": carpeta_catalogo.name,
                "cartas": total, "productos": n_prod, "confirmados": n_conf, "workers": workers,
                "segundos": round(time.time() - t0, 1), "emparejado_utc": ahora_utc()}
        out.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)", [(k, str(v)) for k, v in meta.items()])
        out.commit()
    finally:
        out.close()
    temporal.replace(destino)
    for p in parciales:
        p.unlink(missing_ok=True)
    actualizar_historial()                         # antes de borrar emparejados viejos: todo lo confirmado queda en el historial
    for viejo in sorted(CARPETA_CACHE.glob("emparejado_*.sqlite"), key=lambda p: p.stat().st_mtime, reverse=True)[2:]:
        viejo.unlink(missing_ok=True)             # se quedan los 2 mas recientes (cada uno pesa cientos de MB y se puede rehacer)
    if progreso:
        progreso(total, total, n_prod)
    return destino


# ====================================================================================================
# F. HISTORIAL DE PRECIOS DE HARERUYA (compacto): lo que Stack grafica dia con dia
# ====================================================================================================
# Despues de cada emparejado se guardan SOLO los precios confirmados, agrupados por version de Scryfall (id + acabado),
# idioma y condicion: el mas barato del dia, el stock total y cuantas ofertas. Asi el historial ocupa poco y los archivos
# secos grandes (la descarga completa) se pueden borrar: se conservan los de los 2 dias mas recientes, por si hay que
# volver a emparejar con reglas nuevas.
RUTA_HIST_HAR = CARPETA_TIENDA / "historial_hareruya.sqlite"
SECOS_A_CONSERVAR = 2
DDL_HIST_HAR = """
CREATE TABLE IF NOT EXISTS dias (dia TEXT PRIMARY KEY, seco TEXT, firma_seco TEXT, emparejado TEXT, reglas TEXT,
    completo INTEGER, productos INTEGER, confirmados INTEGER, versiones INTEGER, registrado_utc TEXT);
CREATE TABLE IF NOT EXISTS precios (dia TEXT NOT NULL, scryfall_id TEXT NOT NULL, acabado TEXT NOT NULL, idioma TEXT NOT NULL,
    condicion TEXT NOT NULL, jpy INTEGER, stock INTEGER, ofertas INTEGER,
    PRIMARY KEY (scryfall_id, acabado, idioma, condicion, dia)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_hh_dia ON precios(dia);
"""


def registrar_historial(ruta_emparejado):
    """Pasa los precios confirmados de un emparejado al historial (reemplaza ese dia). Devuelve (dia, versiones)."""
    ruta_emparejado = Path(ruta_emparejado)
    src = sqlite3.connect(f"{ruta_emparejado.resolve().as_uri()}?mode=ro", uri=True)
    try:
        meta = dict(src.execute("SELECT key, value FROM meta"))
        dia = dia_de_emparejado(meta)
        if not PATRON_FECHA.match(dia):
            raise ValueError(f"{ruta_emparejado.name}: no dice de que dia es la captura")
        # mismo criterio que Stack: un producto repetido (cartas de dos nombres salen en dos busquedas) cuenta una vez
        filas = src.execute(
            "SELECT scryfall_id, acabado, idioma, condicion, MIN(precio_jpy), SUM(CASE WHEN stock > 0 THEN stock ELSE 0 END), COUNT(*) "
            "FROM (SELECT DISTINCT d.scryfall_id AS scryfall_id, d.acabado_scryfall AS acabado, "
            "             COALESCE(p.idioma, 'cod ' || p.idioma_raw, '?') AS idioma, COALESCE(p.condicion, 'cod ' || p.condicion_raw, '?') AS condicion, "
            "             p.precio_jpy AS precio_jpy, p.stock AS stock, COALESCE(p.id_producto, p.source_id) AS prod "
            "      FROM decisiones d JOIN hareruya_productos p ON p.source_id = d.source_id "
            "      WHERE d.estado = 'confirmado' AND d.scryfall_id IS NOT NULL AND p.precio_jpy IS NOT NULL) "
            "GROUP BY scryfall_id, acabado, idioma, condicion").fetchall()
    finally:
        src.close()
    RUTA_HIST_HAR.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(RUTA_HIST_HAR, timeout=30)
    try:
        con.executescript(DDL_HIST_HAR)
        ya = con.execute("SELECT versiones FROM dias WHERE dia = ?", (dia,)).fetchone()
        nuevas = len({(f[0], f[1]) for f in filas})
        if not nuevas or (ya and ya[0] and nuevas < 0.5 * ya[0]):
            # una descarga a medias o un emparejado fallido no borra lo bueno que ya habia de ese dia
            raise ValueError(f"{ruta_emparejado.name}: {nuevas:,} versiones confirmadas"
                             + (f" contra {ya[0]:,} que ya hay del {dia}; se conserva lo anterior" if ya else ""))
        con.execute("DELETE FROM precios WHERE dia = ?", (dia,))
        con.executemany("INSERT INTO precios VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [(dia, *f) for f in filas])
        con.execute("INSERT OR REPLACE INTO dias VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (dia, Path(meta.get("seco") or "").name, meta.get("firma_seco"), ruta_emparejado.name, meta.get("reglas"),
                     int(meta.get("seco_completo") or 0), int(meta.get("productos") or 0), int(meta.get("confirmados") or 0),
                     len({(f[0], f[1]) for f in filas}), ahora_utc()))
        con.commit()
    finally:
        con.close()
    return dia, len({(f[0], f[1]) for f in filas}), ya is not None


def dias_registrados():
    if not RUTA_HIST_HAR.exists():
        return {}
    con = sqlite3.connect(f"{RUTA_HIST_HAR.resolve().as_uri()}?mode=ro", uri=True)
    try:
        return {d: (s, fi) for d, s, fi in con.execute("SELECT dia, seco, firma_seco FROM dias")}
    except sqlite3.Error:
        return {}
    finally:
        con.close()


def registrar_pendientes(avisar=print):
    """Los emparejados que hay en disco y todavia no estan en el historial (o que son mas nuevos que lo registrado)."""
    if not CARPETA_CACHE.is_dir():
        return []
    reg, hechos = dias_registrados(), []
    for ruta in sorted(CARPETA_CACHE.glob("emparejado_*.sqlite"), key=lambda p: p.stat().st_mtime):
        try:
            con = sqlite3.connect(f"{ruta.resolve().as_uri()}?mode=ro", uri=True)
            try:
                m = dict(con.execute("SELECT key, value FROM meta"))
            finally:
                con.close()
        except sqlite3.Error:
            continue
        dia = dia_de_emparejado(m)
        if not dia or m.get("completo") != "1":
            continue
        if dia in reg and reg[dia][1] == m.get("firma_seco") and reg[dia][0] == Path(m.get("seco") or "").name:
            continue
        try:
            d, n, _ = registrar_historial(ruta)
            hechos.append((d, n))
            reg[d] = (Path(m.get("seco") or "").name, m.get("firma_seco"))
            avisar(f"Historial de Hareruya: {d} -> {n:,} versiones ({ruta.name})")
        except (sqlite3.Error, ValueError) as e:
            avisar(f"AVISO: no se pudo pasar {ruta.name} al historial ({e})")
    return hechos


def limpiar_secos(avisar=print):
    """Borra las descargas completas (secos) que ya estan en el historial, menos las de los 2 dias mas recientes."""
    reg = dias_registrados()
    secos = archivos_secos()
    dias_vistos, borrados, liberado = [], 0, 0
    for ruta in secos:                                 # del mas reciente al mas viejo
        dia = dia_de_seco(ruta)
        if dia not in dias_vistos:
            dias_vistos.append(dia)
        if dia in dias_vistos[:SECOS_A_CONSERVAR]:     # los 2 dias mas recientes se quedan
            continue
        if dia not in reg:                             # todavia no esta en el historial: no se toca
            continue
        if (CARPETA_SECO / (ruta.name.replace(".jsonl.gz", ".plan.json"))).exists():
            continue                                   # descarga a medias
        try:
            liberado += ruta.stat().st_size
            ruta.unlink()
            Path(str(ruta).replace(".jsonl.gz", ".meta.json")).unlink(missing_ok=True)
            borrados += 1
        except OSError:
            pass
    if borrados:
        avisar(f"Hareruya: {borrados} descarga(s) vieja(s) ya guardadas en el historial borradas ({liberado / 1e6:,.0f} MB liberados).")
    return borrados, liberado


def actualizar_historial(avisar=print):
    """Lo que se hace despues de cada emparejado: historial + limpieza. Nunca truena el emparejado."""
    try:
        registrar_pendientes(avisar)
        limpiar_secos(avisar)
    except Exception as e:                            # noqa: BLE001
        avisar(f"AVISO: historial de Hareruya: {type(e).__name__}: {e}")



# ====================================================================================================
# E. DESCARGA (sin interfaz: 16 procesos; el avance lo ve Stack en data/tiendas/hareruya/progreso.json)
# ====================================================================================================
N_WORKERS = 16               # procesos de descarga a la vez (cada uno con su pausa)
MAX_ERRORES_SEGUIDOS = 3     # si el scraper falla asi de seguido (red caida, bloqueo) ese proceso se detiene: no se insiste


def ahora_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def instalar_espera_conexion(stop):
    """Si una solicitud falla por conexion y la red esta caida, el proceso se pausa y repite la MISMA solicitud cuando vuelve.
    Si la red esta bien, el error sigue su curso normal (lo maneja el scraper)."""
    sondeo = "https://" + URL_BUSQUEDA.split("//", 1)[-1].split("/", 1)[0] + "/"

    def hay_conexion():
        try:
            requests.head(sondeo, timeout=(1.5, 1.5), allow_redirects=False)
            return True
        except requests.RequestException:
            return False
    original = requests.Session.get

    def get(self, *a, **k):
        while True:
            try:
                return original(self, *a, **k)
            except (requests.ConnectionError, requests.Timeout):
                if stop.value or hay_conexion():
                    raise
                while not stop.value and not hay_conexion():
                    time.sleep(1.0)
                if stop.value:
                    raise
    requests.Session.get = get


def descargar_carta(con, indice, carta):
    """Busca UNA carta en Hareruya (sin emparejar). -> entrada del archivo de precios."""
    entrada = {"name": carta["name"], "buscar_como": carta["buscar_como"], "estado": None, "detalle": None,
               "capturado": ahora_utc(), "registros": []}
    _, nombres, _ = preparar(con, indice, carta)    # nombres validos (flavor names, caras): para cortar busquedas ajenas
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        registros = consultar_hareruya_carta(carta, nombres)
    avisos = [l.strip() for l in buf.getvalue().splitlines() if "[Error]" in l]
    if not registros:
        entrada.update(estado="error" if avisos else "sin_resultados",
                       detalle=" | ".join(avisos)[:500] if avisos else "Hareruya no tiene esta carta")
    else:
        entrada.update(estado="con_resultados", registros=registros,
                       detalle=f"{len(registros)} productos" + (f" (avisos: {' | '.join(avisos)[:300]})" if avisos else ""))
    return entrada


def _trabajador_descarga(w, trabajo, cfg, stop, prog):
    """Proceso w (1..N). trabajo = [(i, carta), ...] solo suyo. Cada carta terminada se anexa a su parcial.
    prog[3*(w-1)] = cartas hechas, +1 = productos, +2 = cartas con error."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)      # el Ctrl+C lo maneja el proceso principal (termina la carta en curso)
    indice = indice_del_dia(avisar=lambda *_: None)
    con = abrir_catalogo(RUTA_HISTORIAL)
    instalar_espera_conexion(stop)
    time.sleep(cfg["pausa"] * (w - 1) / cfg["workers"])     # escalonados: las solicitudes no salen en rafaga
    seguidos = 0
    with open(Path(cfg["parciales"]) / f"w{w}.jsonl", "a", encoding="utf-8") as out:
        for k, (i, carta) in enumerate(trabajo):
            if stop.value:
                break
            try:
                entrada = descargar_carta(con, indice, carta)
            except Exception as e:                   # noqa: BLE001  (una carta rara no tumba el proceso)
                entrada = {"name": carta["name"], "buscar_como": carta["buscar_como"], "estado": "error", "capturado": ahora_utc(),
                           "detalle": f"{type(e).__name__}: {e}"[:500], "registros": []}
            entrada["i"] = i
            out.write(json.dumps(entrada, ensure_ascii=False, default=str) + "\n")
            out.flush()
            base = 3 * (w - 1)
            prog[base] += 1
            prog[base + 1] += len(entrada["registros"])
            prog[base + 2] += entrada["estado"] == "error"
            seguidos = seguidos + 1 if entrada["estado"] == "error" else 0
            if seguidos >= cfg["max_errores_seguidos"]:
                break                                 # posible bloqueo o red caida: este proceso no insiste
            if k < len(trabajo) - 1:
                fin = time.monotonic() + cfg["pausa"]
                while not stop.value and time.monotonic() < fin:
                    time.sleep(0.1)
    con.close()


def leer_parciales(carpeta):
    hechas = {}
    for p in sorted(Path(carpeta).glob("w*.jsonl")):
        with open(p, encoding="utf-8") as f:
            for linea in f:
                try:
                    e = json.loads(linea)
                except ValueError:
                    continue                          # ultima linea cortada si el proceso murio escribiendo
                hechas[e["i"]] = e                    # la ultima vez gana (un reintento reemplaza al error)
    return hechas


def consolidar(base, plan, hechas):
    """Escribe el .jsonl.gz (atomico) y el .meta.json con lo que haya."""
    ruta = base.with_name(base.name + ".jsonl.gz")
    temporal = ruta.with_name(ruta.name + ".part")
    n_prod = 0
    with gzip.open(temporal, "wt", encoding="utf-8") as f:
        for i in sorted(hechas):
            n_prod += len(hechas[i]["registros"])
            f.write(json.dumps(hechas[i], ensure_ascii=False, default=str) + "\n")
    temporal.replace(ruta)
    estados = [e["estado"] for e in hechas.values()]
    capt = [e["capturado"] for e in hechas.values() if e.get("capturado")]
    meta = {"version": VERSION_SECO, "tienda": TIENDA, "moneda": MONEDA, "catalogo_dia": plan["catalogo_dia"],
            "inicio": min(capt) if capt else None, "fin": max(capt) if capt else None,
            "cartas_plan": len(plan["cartas"]), "procesadas": len(hechas),
            "con_resultados": estados.count("con_resultados"), "sin_resultados": estados.count("sin_resultados"),
            "errores": estados.count("error"), "pendientes": len(plan["cartas"]) - len(hechas), "productos": n_prod,
            "completo": len(hechas) == len(plan["cartas"])}
    base.with_name(base.name + ".meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    return ruta, meta


def ultima_incompleta():
    """La descarga que quedo a medias (se retoma sola), o None."""
    for plan in sorted(CARPETA_SECO.glob("hareruya_seco_*.plan.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        base = plan.with_name(plan.name.replace(".plan.json", ""))
        if base.with_name(base.name + ".parciales").is_dir():
            return base
    return None


def escribir_progreso(datos):
    """data/tiendas/hareruya/progreso.json: lo lee Stack para mostrar el avance (escritura atomica)."""
    try:
        CARPETA_TIENDA.mkdir(parents=True, exist_ok=True)
        temporal = RUTA_PROGRESO.with_name(RUTA_PROGRESO.name + ".part")
        temporal.write_text(json.dumps({"tienda": TIENDA, "actualizado": ahora_utc(), "pid": os.getpid(), **datos},
                                       ensure_ascii=False), encoding="utf-8")
        temporal.replace(RUTA_PROGRESO)
    except OSError:
        pass


def descargar(n=None, workers=N_WORKERS, pausa=PAUSA_SEGUNDOS, nueva=False):
    """Descarga los precios de Hareruya de toda la search_list (o n al azar para probar). Si hay una descarga a medias,
    la retoma (y reintenta las cartas con error). Para pararla: crear DETENER.txt en data/tiendas/hareruya/descargas/
    (Stack lo hace con su boton) o Ctrl+C: termina la carta en curso y guarda."""
    base = None if nueva or n else ultima_incompleta()
    if base is not None:
        plan = json.loads(base.with_name(base.name + ".plan.json").read_text(encoding="utf-8"))
    else:
        dia = dia_catalogo()
        if not dia:
            escribir_progreso({"estado": "error", "detalle": "no hay historial de precios (corre scryfall.py)"})
            sys.exit(f"No hay historial de precios ({RUTA_HISTORIAL}). Corre antes scryfall.py.")
        cartas = leer_search_list()
        if n:
            cartas = sorted(random.sample(cartas, min(n, len(cartas))), key=lambda c: c["name"])
        CARPETA_SECO.mkdir(parents=True, exist_ok=True)
        base = CARPETA_SECO / f"hareruya_seco_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        plan = {"catalogo_dia": dia, "creado": ahora_utc(), "cartas": cartas}
        base.with_name(base.name + ".plan.json").write_text(json.dumps(plan, ensure_ascii=False), encoding="utf-8")
    parciales = base.with_name(base.name + ".parciales")
    parciales.mkdir(exist_ok=True)
    detener = CARPETA_SECO / "DETENER.txt"
    detener.unlink(missing_ok=True)
    hechas = leer_parciales(parciales)
    pendientes = [(i, c) for i, c in enumerate(plan["cartas"]) if i not in hechas or hechas[i]["estado"] == "error"]
    total = len(plan["cartas"])
    previas = total - len(pendientes)
    inicio = ahora_utc()
    print(f"Hareruya: {total:,} nombres, {len(pendientes):,} por buscar con {workers} procesos. El avance se ve en Stack.", flush=True)
    escribir_progreso({"estado": "descargando", "hechas": previas, "total": total, "productos": 0, "errores": 0,
                       "inicio": inicio, "archivo": base.name, "procesos": workers})
    detenida = False
    if pendientes:
        indice_del_dia()                              # se arma UNA vez aqui; los procesos solo lo leen
        workers = max(1, min(workers, len(pendientes)))
        cfg = {"parciales": str(parciales), "pausa": pausa, "workers": workers, "max_errores_seguidos": MAX_ERRORES_SEGUIDOS}
        ctx = multiprocessing.get_context(MP_CONTEXT)
        stop = ctx.Value("i", 0, lock=False)
        prog = ctx.Array("q", 3 * workers, lock=False)
        procs = [ctx.Process(target=_trabajador_descarga, args=(w, pendientes[w - 1::workers], cfg, stop, prog), daemon=True)
                 for w in range(1, workers + 1)]
        for p in procs:
            p.start()

        def parar(*_):
            stop.value = 1
        signal.signal(signal.SIGINT, parar)
        t0 = time.time()
        try:
            while any(p.is_alive() for p in procs):
                if detener.exists():
                    parar()
                hechas_ = sum(prog[3 * k] for k in range(workers))
                ritmo = (time.time() - t0) / hechas_ if hechas_ else None
                escribir_progreso({"estado": "deteniendo" if stop.value else "descargando", "hechas": previas + hechas_, "total": total,
                                   "productos": sum(prog[3 * k + 1] for k in range(workers)),
                                   "errores": sum(prog[3 * k + 2] for k in range(workers)), "inicio": inicio, "archivo": base.name,
                                   "procesos": sum(p.is_alive() for p in procs),
                                   "faltan_min": round((len(pendientes) - hechas_) * ritmo / 60) if ritmo else None})
                time.sleep(2)
        finally:
            for p in procs:
                p.join()
        detenida = bool(stop.value)
        hechas = leer_parciales(parciales)
    ruta, meta = consolidar(base, plan, hechas)
    completa = meta["completo"] and not meta["errores"]
    if completa:
        shutil.rmtree(parciales, ignore_errors=True)
        base.with_name(base.name + ".plan.json").unlink(missing_ok=True)
    detener.unlink(missing_ok=True)
    escribir_progreso({"estado": "terminado" if completa else "detenido" if detenida else "incompleto",
                       "hechas": meta["procesadas"], "total": meta["cartas_plan"], "productos": meta["productos"],
                       "errores": meta["errores"], "inicio": inicio, "fin": ahora_utc(), "archivo": ruta.name})
    print(f"{meta['procesadas']:,}/{meta['cartas_plan']:,} cartas | {meta['productos']:,} productos | con error {meta['errores']:,}"
          + ("" if completa else " | quedo a medias: la siguiente vez se retoma sola"), flush=True)
    return ruta


# ----------------------------------------------------------------------------------------------------
# Hareruya bloquea a los servidores de GitHub: la descarga se hace en tu PC y se sube a la nube (release 'hareruya-pc'
# de Guayaba7/stack-datos). El workflow diario la toma de ahi en lugar de descargar. Necesita GitHub CLI (gh) con sesion.
REPO_NUBE, TAG_PC = "Guayaba7/stack-datos", "hareruya-pc"


def subir_a_nube(repo=REPO_NUBE, tag=TAG_PC):
    """Descarga Hareruya (retoma si quedo a medias) y sube el resultado a la nube para que el workflow lo use."""
    import subprocess
    gh = shutil.which("gh")
    if not gh:
        sys.exit("Falta GitHub CLI (https://cli.github.com) con sesion:  gh auth login")
    corre = lambda *a: subprocess.run([gh, *a], capture_output=True, text=True, encoding="utf-8", errors="replace")
    ruta = descargar()                              # termina la que haya quedado a medias...
    if dia_de_seco(ruta) != datetime.now().date().isoformat():
        ruta = descargar(nueva=True)                # ...y si era de otro dia, baja la de hoy
    meta = meta_seco(ruta)
    if not meta.get("completo") or not int(meta.get("productos") or 0):
        sys.exit(f"La descarga no quedo completa ({meta.get('procesadas')}/{meta.get('cartas_plan')} cartas): no se sube.")
    archivos = [ruta, ruta.with_name(ruta.name.replace(".jsonl.gz", ".meta.json"))]
    if corre("release", "view", tag, "--repo", repo).returncode != 0:   # pre-release: nunca es el 'latest' de los datos
        c = corre("release", "create", tag, "--repo", repo, "--prerelease", "--latest=false", "--title", "Hareruya (desde la PC)",
                  "--notes", "Descarga de Hareruya hecha en la PC (Hareruya bloquea a GitHub). La usa el workflow diario.")
        if c.returncode != 0:
            sys.exit("No se pudo crear el release: " + c.stderr.strip())
    for a in json.loads(corre("release", "view", tag, "--repo", repo, "--json", "assets").stdout or "{}").get("assets", []):
        if a["name"] not in {x.name for x in archivos}:            # solo queda la de hoy
            corre("release", "delete-asset", tag, a["name"], "--repo", repo, "--yes")
    u = corre("release", "upload", tag, *map(str, archivos), "--repo", repo, "--clobber")
    if u.returncode != 0:
        sys.exit("No se pudo subir: " + u.stderr.strip())
    print(f"Subido a la nube ({repo}, {tag}): {ruta.name} | {int(meta['productos']):,} productos", flush=True)


# ====================================================================================================
# F. LINEA DE COMANDOS
# ====================================================================================================
def main(argv=None):
    ap = argparse.ArgumentParser(prog="hareruya.py", description="Precios de Hareruya para Stack.")
    ap.add_argument("accion", nargs="?", default="descargar", choices=["descargar", "emparejar", "detener", "historial", "nube"],
                    help="descargar (por defecto; retoma la que haya quedado a medias), emparejar (Stack lo hace solo), detener, "
                         "o historial (pasa al historial los emparejados que falten; con --todo empareja tambien las descargas viejas)")
    ap.add_argument("--n", type=int, help="solo N nombres al azar (prueba rapida)")
    ap.add_argument("--workers", type=int, default=N_WORKERS, help=f"procesos en paralelo (por defecto {N_WORKERS})")
    ap.add_argument("--nueva", action="store_true", help="empezar una descarga nueva aunque haya una a medias")
    ap.add_argument("--todo", action="store_true", help="historial: emparejar tambien las descargas que no tengan emparejado")
    a = ap.parse_args(argv)
    if a.accion == "detener":
        CARPETA_SECO.mkdir(parents=True, exist_ok=True)
        (CARPETA_SECO / "DETENER.txt").write_text("detener", encoding="utf-8")
        print("Se pidio detener la descarga: termina la carta en curso y guarda.")
    elif a.accion == "nube":                        # en tu PC: descargar y subir a la nube (ver subir_a_nube)
        subir_a_nube()
    elif a.accion == "emparejar":
        ruta = emparejar()
        print(f"Emparejado -> {ruta}")
    elif a.accion == "historial":
        if a.todo:                                  # descargas viejas sin emparejado: se emparejan con el catalogo de hoy
            reg = dias_registrados()
            for seco in reversed(archivos_secos()):
                if dia_de_seco(seco) in reg:
                    continue
                print(f"Emparejando {seco.name} ...")
                try:
                    registrar_historial(emparejar(seco, workers=max(1, a.workers)))
                except Exception as e:              # noqa: BLE001
                    print(f"  AVISO: {type(e).__name__}: {e}")
        registrar_pendientes()
        limpiar_secos()
        reg = dias_registrados()
        print(f"Historial de Hareruya: {len(reg)} dia(s) -> {RUTA_HIST_HAR}" + (f" ({min(reg)} a {max(reg)})" if reg else ""))
    else:
        descargar(a.n, max(1, a.workers), nueva=a.nueva)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
