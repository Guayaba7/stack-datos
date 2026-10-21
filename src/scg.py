"""
STARCITYGAMES para Stack (antes scg_tienda.py + scg_prueba.py): el precio NM de cada version del catalogo de $2 o mas (Ctrl+T -> STARCITYGAMES -> Ins).

Como funciona:
  1. Lee el catalogo del dia (data/precios/historial.sqlite, el que genera scryfall.py) y saca los NOMBRES distintos.
  2. Busca cada nombre en la busqueda de StarCityGames (HawkSearch, la misma que usa su pagina), con 16 actores a la vez
     (16 conexiones, cada una con su pausa). Lo que regresa SCG se guarda: si la descarga se detiene o se cierra la
     computadora, al iniciarla otra vez sigue donde se quedo.
  3. Una segunda busqueda (nombre + set) para las versiones de cartas con muchisimas impresiones (basicas, staples).
  4. Empareja cada version con el producto EXACTO de SCG (mismo nombre, set / promo, numero, acabado e idioma; condicion
     NM), y guarda el precio del dia en el historial.

Archivos (data/tiendas/starcitygames/):
  scg.sqlite      busquedas (lo que regreso SCG por nombre), precios (el emparejado que usa Stack), historial por dia
  progreso.json   avance (lo muestra Stack)            registro.txt  lo que imprime la descarga
  descargas/DETENER.txt   si existe, la descarga termina lo que tiene en curso y se detiene (Stack: Supr en Ctrl+T)

Uso:   python src/scg.py               (descargar; retoma si hay una a medias)
       python src/scg.py emparejar     (vuelve a emparejar lo ya descargado, sin conectarse; segundos)
       python src/scg.py probar        (revisa las reglas con casos reales ya verificados; no descarga nada)
       python src/scg.py detener

OJO: no es una API publica documentada; puede cambiar o bloquear. No bajes las pausas.
"""
import json
import os
import queue
import re
import sqlite3
import sys
import threading
import time
import unicodedata
import zlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

try:                                                 # consola de Windows: que los acentos no truenen
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

TIENDA = "starcitygames"            # carpeta data/tiendas/starcitygames; el programa es src/scg.py
N_ACTORES = 16                       # conexiones a la vez; cada una espera PAUSA_SEGUNDOS entre peticiones

URL_API = "https://ajax.starcitygames.com/hawksearch/searchapi/api/v2/search"
URL_SITIO = "https://starcitygames.com"
RESULTADOS_POR_PAGINA = "96"
VARIANTES_POR_PRODUCTO = 32
MAX_PAGINAS = 8                     # por carta (96 x 8 = 768 productos: de sobra para una carta)
PAUSA_SEGUNDOS = 1.2
REINTENTOS = 4                      # con espera creciente (3, 6, 12, 24 s): un tropiezo de la red ya no deja la carta en 'error'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:157.0) Gecko/20100101 Firefox/157.0",
    "Content-Type": "application/json",
    "Origin": "https://starcitygames.com",
    "Referer": "https://starcitygames.com/",
}

RAIZ = Path(os.environ.get("STACK_RAIZ") or Path(__file__).resolve().parent.parent)
RUTA_HISTORIAL = RAIZ / "data" / "precios" / "historial.sqlite"
CARPETA_SALIDA = RAIZ / "data" / "tiendas" / "starcitygames"

# Para cada dato: posibles nombres del campo en la respuesta (en orden). Los de la variante tienen prioridad.
CANDIDATOS = {
    "name": ["card_name", "product_name", "title", "name", "itemname"],
    "set": ["set_name", "set", "edition", "expansion"],
    "set_code": ["set_code", "setcode", "set_abbreviation"],
    "collector_number": ["collector_number", "card_number", "collectornumber", "number"],
    "finish": ["finish", "foil", "is_foil", "printing", "treatment"],
    "language": ["language", "lang"],
    "condition": ["condition", "card_condition"],
    "price": ["price", "sale_price", "saleprice", "retail_price"],
    "stock": ["qty", "quantity", "stock", "inventory", "inventory_quantity", "qty_available"],
    "sku": ["sku", "variant_sku", "upc"],
    "url": ["url_detail", "url", "link", "custom_url", "customurl"],
}
# SKU de SCG (cuando viene): SGL-MTG-<SET>-<NUMERO>-<IDIOMA><ACABADO>...  p. ej. SGL-MTG-THS-107-ENN / ...-ENF
#   promos y listas traen un tipo y el set de origen con guion bajo:
#     SGL-MTG-PRM-PP_ECL_165-ENN   (PP = promo pack de ECL #165)     SGL-MTG-PRM-PRE_BFZ_006-ENF  (PRE = prerelease)
#     SGL-MTG-PRM-CD_Q06_009-ENN   (challenger deck)                  SGL-MTG-PWSB-JUD_072-ENN     (Mystery Booster / The List)
#     SGL-MTG-PRM-SECRET_SLC_005-ENN (Secret Lair)                    SGL-MTG-WAR2-1-JAN           (SCG: variante de WAR)
# el idioma+acabado es el ULTIMO tramo (ENN, ENF, JAN, puede seguir un digito); el '-PRE_' de PRM-PRE_BFZ no lo es
RE_SKU = re.compile(r"SGL-MTG-(\S+)-([A-Z]{2})([NFE])\d*(?![A-Za-z0-9_])", re.I)
# sufijo del numero en Scryfall -> tipo de promo en el SKU de SCG
TIPO_SUFIJO = {"p": {"PP"}, "s": {"PRE", "LNCH"}}         # 's' = prerelease (o launch party, p. ej. Battlebond)
TIPOS_LISTA = {"PWSB", "PLST", "LIST"}                 # The List: Scryfall PLST con numero 'BBD-87'
ALIAS_SET = {"H1R": "MH1", "H2R": "MH2", "MP2": "MPS3"}   # Scryfall -> codigo que usa SCG (timeshifts, invocations)
# tipos de SCG que son OTRA version del set que nombran (no la normal): PRM-PP_ECL = promo pack de ECL, PWSB-FUT = The List
TIPOS_VARIANTE = {"PP", "PRE", "LNCH", "BAB", "ALT", "FNM", "ARENA", "LNY", "LYLGS", "JUDGE"} | TIPOS_LISTA
# los UNICOS tipos de SCG que pueden ser la version 'normal' del set que nombran (Secret Lair, challenger decks); cualquier
# otro tipo con el mismo set (GMDY_FRF = Game Day de FRF, PP_FRF, PWSB_FRF...) es otra version, no la normal
TIPOS_MISMO_SET = {"SECRET", "CD"}
# sets de Scryfall que SCG guarda como tipo + set de origen: PSS1 (BFZ Standard Series) = PRM-ALT_BFZ_241
# promos de evento: el set de Scryfall lleva el anio y SCG lo pone como tipo + anio (PRM-ARENA_2004_006)
EVENTOS = [(re.compile(r"^PAL(\d\d)$"), "ARENA"), (re.compile(r"^F(\d\d)$"), "FNM"), (re.compile(r"^PL(\d\d)$"), "LNY"),
           (re.compile(r"^PLG(\d\d)$"), "LYLGS")]                 # PLG24 = Love Your LGS 2024 = PRM-LYLGS_2024


def anio_de(q):
    m = re.search(r"\b(?:19|20)\d\d\b", str(q.get("set_nombre") or ""))
    return m.group(0) if m else None


# promos de evento / tienda: SCG los guarda como TIPO_ANIO_NUM o TIPO_SET_NUM (PRM-FNM_2009_012, PRM-SDCC18_M19_106...)
EVENTO_ANIO = [(r"PAL(\d\d)", {"ARENA"}), (r"F(\d\d)", {"FNM"}), (r"PL(\d\d)", {"LNY"}), (r"PLG(\d\d)", {"LYLGS"}),
               (r"PF(\d\d)", {"FEST"}), (r"H(\d\d)", {"HSCON"}), (r"PW(\d\d)", {"PLAY", "WPNP"})]
#   set de Scryfall -> tipo de SCG (el anio, si el nombre del set trae uno, debe coincidir)
EVENTO_TIPO = {"PCMP": "CHMP", "SCH": "SCHP", "PRCQ": "PRMR", "DCI": "WPNG", "LMAR": "MLF", "FNM": "FNM", "PSS5": "SSD"}
#   set de Scryfall -> (tipo, set) fijos en el SKU de SCG; True = el numero debe coincidir
EVENTO_SET = {"PTG": ("PRM", "PONY", True), "HHO": ("PRM", "HLDY", False), "PRW2": ("PRM", "RVWK", True), "PRWK": ("PRM", "RVWK", True),
              "PSS1": ("ALT", "BFZ", True), "BLC": ("BAB", "BLB", True)}


def promo_mapa(c, q):
    """Si el producto de SCG es la promo / mazo especial que corresponde al set de Scryfall: (motivo, numero que debe
    tener en SCG o None si SCG lo numera de otra forma). Si no: None."""
    s, sk, tipo = q["set"].upper(), (c.get("sku_set") or "").upper(), (c.get("sku_tipo") or "").upper()
    numq = str(q["numero"])
    if not tipo:
        return None
    m = re.fullmatch(r"WC(\d\d)", s)                   # mazos de World Championship: Scryfall 'tvdl324' = SCG 01TVDL_MMQ_324
    if m:
        n = re.fullmatch(r"([a-z]+)(\d+[a-z]?)(sb)?", numq.lower())
        if n and tipo == f"{m.group(1)}{n.group(1).upper()}":
            return f"mazo de World Championship ({tipo}_{sk})", n.group(2)
        return None
    for rx, tipos in EVENTO_ANIO:
        m = re.fullmatch(rx, s)
        if m and tipo in tipos:
            yy = int(m.group(1))
            anio = str(1900 + yy if yy >= 90 else 2000 + yy)
            if tipo in ("PLAY", "WPNP") or sk.startswith(anio):     # PLAY_MSH lleva el set; WPNP a veces el anio anterior
                return f"promo de evento ({tipo}_{sk})", numq
    m = re.fullmatch(r"PS(\d\d)", s)                   # San Diego Comic-Con: PS18 = SDCC18_M19
    if m and tipo == f"SDCC{m.group(1)}":
        return f"promo de Comic-Con ({tipo}_{sk})", numq
    if s in EVENTO_TIPO and tipo == EVENTO_TIPO[s] and (not anio_de(q) or sk.startswith(anio_de(q))):
        return f"promo de evento ({tipo}_{sk})", numq
    if s == "PJSC" and tipo == "JSC":                    # Japan Standard Cup: '2025-2' = JSC_2025 #002
        a, _, n = numq.partition("-")
        if n and sk.startswith(a):
            return f"Japan Standard Cup ({tipo}_{sk})", n
    if s in EVENTO_SET and (tipo, sk) == EVENTO_SET[s][:2]:
        return f"promo ({tipo}_{sk})", numq if EVENTO_SET[s][2] else None
    return None


def evento(set_scryfall):
    """('ARENA', '2004') para PAL04, ('FNM', '2009') para F09, ('LNY', '2021') para PL21; si no, None."""
    for rx, tipo in EVENTOS:
        m = rx.match(set_scryfall.upper())
        if m:
            yy = int(m.group(1))
            return tipo, str(1900 + yy if yy >= 90 else 2000 + yy)
    return None
CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
IDIOMA_SCG = {"english": "en", "japanese": "ja", "en": "en", "ja": "ja", "jp": "ja"}
ACABADO_SKU = {"N": "nonfoil", "F": "foil", "E": "etched"}
# palabras de SCG que no son parte del nombre del set
RUIDO_SET = re.compile(r"\b(foil|non-?foil|etched|borderless|extended art|showcase|promo pack|the list)\b", re.I)


# ---------------------------------------------------------------- utilidades
def escalar(valor):
    if isinstance(valor, list):
        valor = [v for v in valor if v not in (None, "")]
        if not valor:
            return None
        return escalar(valor[0]) if len(valor) == 1 else ", ".join(str(escalar(v)) for v in valor)
    if isinstance(valor, dict):
        return str(valor)
    if isinstance(valor, str):
        return CONTROL.sub("", valor).strip() or None
    return valor


def es_lista_de_dicts(v):
    return isinstance(v, list) and len(v) > 0 and all(isinstance(x, dict) for x in v)


def a_decimal(valor):
    if valor is None:
        return None
    texto = re.sub(r"[^\d.\-]", "", str(valor))
    try:
        return float(texto)
    except ValueError:
        return None


def a_entero(valor):
    n = a_decimal(valor)
    return int(n) if n is not None else None


def norm(t):
    """minusculas, sin acentos ni signos: 'Jace, the Mind Sculptor' == 'jace the mind sculptor'."""
    t = unicodedata.normalize("NFKD", str(t or "")).encode("ascii", "ignore").decode().lower()
    t = t.replace("&", " and ")
    return re.sub(r"[^a-z0-9/]+", " ", t).strip()


def parse_sku(sku):
    """(set, numero, idioma, acabado, tipo) del SKU de SCG, o None."""
    m = RE_SKU.search(str(sku or ""))
    if not m:
        return None
    medio, idioma, acabado = m.group(1).upper(), m.group(2).lower(), ACABADO_SKU[m.group(3).upper()]
    tipo = None
    if "_" in medio:
        grupo, _, resto = medio.partition("-")
        partes = (resto or grupo).split("_")
        if resto and len(partes) == 2:                 # PWSB-JUD_072
            tipo, set_, num = grupo, partes[0], partes[1]
        elif len(partes) >= 3:                         # PRM-PP_ECL_165
            tipo, set_, num = partes[0], partes[1], "_".join(partes[2:])
        else:
            return None
    else:
        set_, _, num = medio.partition("-")
        if not num:
            return None
    return set_, num, idioma, acabado, tipo


def aplicar_sku(d):
    """Llena sku_set / sku_num / sku_idioma / sku_acabado / sku_tipo e idioma y acabado ."""
    p = parse_sku(d.get("sku"))
    d["sku_set"], d["sku_num"], d["sku_idioma"], d["sku_acabado"], d["sku_tipo"] = p if p else (None,) * 5
    idioma = norm(d.get("language"))
    d["idioma"] = d["sku_idioma"] or IDIOMA_SCG.get(idioma) or (idioma or None)
    if d.get("finish") is None and d["sku_acabado"]:
        d["finish"] = d["sku_acabado"]
    return d


def norm_num(n):
    """'107' == '0107' == '107a'? no: solo se quitan ceros a la izquierda y espacios."""
    n = str(n or "").strip().lower().lstrip("#").replace("\u2605", "").replace("\u2020", "")   # 245\u2605 (solo foil) == 245
    return re.sub(r"^0+(?=\d)", "", n)


def normalizar_finish(valor):
    if valor is None:
        return None
    s = str(valor).strip().casefold()
    if "etched" in s:
        return "etched"
    if s in ("1", "true", "yes", "y", "foil") or ("foil" in s and "non" not in s):
        return "foil"
    if s in ("0", "false", "no", "n", "nonfoil", "non-foil", "non foil", "regular", "normal") or "non" in s:
        return "nonfoil"
    return str(valor)


def es_nm(cond):
    c = norm(cond)
    return c in ("near mint", "nm", "nm m", "near mint mint") or c.startswith("near mint")



# ---------------------------------------------------------------- respuesta de SCG -> filas
def filas_de_resultado(res):
    doc = res.get("Document", res)
    if isinstance(doc, list) and doc and isinstance(doc[0], dict):
        doc = doc[0]
    if not isinstance(doc, dict):
        doc = {}
    padre = {k: escalar(v) for k, v in doc.items() if not es_lista_de_dicts(v)}
    for clave in ("DocId", "Score"):
        if clave in res:
            padre[clave] = res[clave]
    variantes = []
    for origen in (doc, {k: v for k, v in res.items() if k != "Document"}):
        variantes = next((v for v in origen.values() if es_lista_de_dicts(v)), [])
        if variantes:
            break
    if not variantes:
        return [padre]
    filas = []
    for var in variantes:
        fila = dict(padre)
        fila.update({f"variant.{k}": escalar(v) for k, v in var.items()})
        filas.append(fila)
    return filas


def buscar_valor(fila, candidatos):
    minusculas = {k.lower(): k for k in fila}
    for c in candidatos:
        for clave in (f"variant.{c}", c):
            real = minusculas.get(clave)
            if real is not None and fila[real] not in (None, ""):
                return fila[real], real
    return None, None


def fila_limpia(fila, campos_usados):
    d = {}
    for col, cands in CANDIDATOS.items():
        v, campo = buscar_valor(fila, cands)
        if campo:
            campos_usados.setdefault(col, campo)
        d[col] = v
    d["price"] = a_decimal(d["price"])
    d["stock"] = a_entero(d["stock"])
    d["finish"] = normalizar_finish(d["finish"])
    d["doc"] = fila.get("DocId")                      # el producto (sus variantes = condiciones comparten DocId)
    if isinstance(d["url"], str) and d["url"].startswith("/"):
        d["url"] = URL_SITIO + d["url"]
    # el SKU (si viene) dice set, numero, idioma y acabado sin ambiguedad
    return aplicar_sku(d)


def esperar(segundos):
    """Pausa que se corta si piden detener (Stack: Supr): asi un reintento largo no hace esperar a nadie."""
    fin = time.time() + segundos
    while time.time() < fin:
        if (CARPETA_SALIDA / "descargas" / "DETENER.txt").exists():
            return False
        time.sleep(min(0.5, max(0.0, fin - time.time())))
    return True


def pedir(sesion, cuerpo):
    for intento in range(REINTENTOS + 1):
        try:
            r = sesion.post(URL_API, json=cuerpo, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504) and intento < REINTENTOS:
                if not esperar(5 * 2 ** intento):
                    return None, "detenido"
                continue
            r.raise_for_status()
            return r.json(), None
        except requests.RequestException as e:
            if intento < REINTENTOS:
                if not esperar(3 * 2 ** intento):
                    return None, "detenido"
                continue
            return None, f"HTTP: {type(e).__name__}: {str(e)[:120]}"
        except ValueError:
            return None, "la respuesta no es JSON"
    return None, "sin respuesta"


def buscar_nombre(sesion, nombre, campos_usados=None, crudos_muestra=None):
    """(filas, error, info): todas las filas (producto x variante) que SCG regresa para el nombre.
    info = {"paginas": leidas, "cortado": True si quedaron paginas sin leer (MAX_PAGINAS)}."""
    limpias, vistos, info = [], set(), {"paginas": 0, "cortado": False}
    for pagina in range(1, MAX_PAGINAS + 1):
        info["paginas"] = pagina
        cuerpo = {"Keyword": nombre, "Variant": {"MaxPerPage": VARIANTES_POR_PRODUCTO}, "MaxPerPage": RESULTADOS_POR_PAGINA}
        if pagina > 1:
            cuerpo["Page"] = pagina
        datos, err = pedir(sesion, cuerpo)
        if err:
            return limpias, err, info
        resultados = (datos or {}).get("Results") or []
        if not resultados:
            break
        nuevos = 0
        for res in resultados:
            clave = res.get("DocId") or id(res)
            if clave in vistos:
                continue
            vistos.add(clave); nuevos += 1
            for f in filas_de_resultado(res):
                if crudos_muestra is not None and len(crudos_muestra) < 300:
                    crudos_muestra.append({"busqueda": nombre, **f})
                limpias.append(fila_limpia(f, campos_usados if campos_usados is not None else {}))
        if not nuevos:
            break
        tot = ((datos or {}).get("Pagination") or {}).get("NofPages")
        if (tot is not None and pagina >= tot) or (tot is None and len(resultados) < int(RESULTADOS_POR_PAGINA)):
            break
        if pagina == MAX_PAGINAS:
            info["cortado"] = True
        time.sleep(PAUSA_SEGUNDOS)
    return limpias, None, info


# ---------------------------------------------------------------- emparejado (el producto EXACTO)
def nombre_coincide(scg, quiero):
    a, b = norm(scg), norm(quiero)
    if not a:
        return False
    if a == b:
        return True
    caras = [c.strip() for c in b.split("/") if c.strip()]   # 'Fire // Ice': SCG a veces solo pone la primera cara
    if len(caras) > 1 and (a == caras[0] or a.replace(" ", "") == b.replace(" ", "")):
        return True
    caras_scg = [c.strip() for c in a.split("/") if c.strip()]   # melds: SCG 'Hanweir Battlements // Hanweir, the Writhing Township'
    if len(caras_scg) > 1 and len(caras) == 1 and caras_scg[0] == b:
        return True
    a2 = re.sub(r"\s*\(.*?\)\s*|\s*\[.*?\]\s*", " ", str(scg or "")).replace("{", "").replace("}", "")   # 'Thoughtseize (Borderless)', '{Drake Token}'
    if norm(a2) == b:
        return True
    return re.sub(r"\s+token$", "", norm(a2)) == b                   # fichas: SCG las llama 'Beast Token'



def set_base(q):
    """Codigo 'de origen' de la version: PAKH -> AKH (promos), PLST + 'BBD-87' -> BBD, H1R -> MH1."""
    s = q["set"].upper()
    if s in TIPOS_LISTA and "-" in str(q["numero"]):
        return str(q["numero"]).split("-", 1)[0].upper()
    s = ALIAS_SET.get(s, s)
    return s[1:] if len(s) == 4 and s[0] == "P" else s


def set_fichas(q):
    """TCMA -> CMA: Scryfall pone las fichas en T+set; SCG las pone en el set con numero 'T18'."""
    s = q["set"].upper()
    return s[1:] if len(s) == 4 and s[0] == "T" else None


def sufijo_promo(q):
    m = re.search(r"\d([ps])$", str(q["numero"]).lower())
    return m.group(1) if m else None


def set_coincide(c, q):
    """(nivel, motivo): 2 = por codigo del SKU, 1 = por nombre del set o variante, 0 = no."""
    sk, tipo = (c.get("sku_set") or "").upper(), (c.get("sku_tipo") or "").upper()
    pm = promo_mapa(c, q)
    if pm:
        return 2, pm[0]
    if sk and sk == ALIAS_SET.get(q["set"].upper()) and not tipo:
        return 2, f"set por SKU ({sk} = {q['set'].upper()})"
    num_scg = str(c.get("sku_num") or c.get("collector_number") or "").upper()
    if set_fichas(q) and sk == set_fichas(q) and num_scg.startswith("T"):
        return 2, f"set por SKU (fichas de {sk})"
    if sk and sk == q["set"].upper():
        if tipo and tipo not in TIPOS_MISMO_SET and not sufijo_promo(q):   # PP_ECL / PWSB_FUT / GMDY_FRF: otra version
            nombre = {"PP": "promo pack", "PRE": "prerelease", "BAB": "buy-a-box", "GMDY": "game day"}.get(tipo, "The List" if tipo in TIPOS_LISTA else tipo)
            return 0, f"otro tipo (SCG: {nombre}; la version es la normal)"
        return 2, "set por SKU"
    if sk and tipo:                                       # promo / The List: el SKU trae el set de origen
        suf = sufijo_promo(q)
        if sk == set_base(q) and (q["set"].upper() != set_base(q) or tipo in TIPOS_LISTA):
            if suf and tipo not in TIPO_SUFIJO[suf]:
                return 0, f"otro tipo de promo (SCG: {tipo}, Scryfall: {'promo pack' if suf == 'p' else 'prerelease'})"
            if q["set"].upper() in TIPOS_LISTA and tipo not in TIPOS_LISTA:
                return 0, f"otro tipo (SCG: {tipo}; la version es de The List)"
            return 2, f"set por SKU de promo ({tipo}_{sk})"
    v = re.fullmatch(r"([A-Z0-9]{3})\d", sk)               # SCG: WAR2, MH13, DFT2... = variante del set base
    base = ALIAS_SET.get(q["set"].upper(), q["set"].upper())
    o = re.fullmatch(r"O([A-Z0-9]{3})", q["set"].upper())  # oversized: OC17 = SCG C172, OCM1 = CM12, OCMD = CMD2
    if v and not tipo and (v.group(1) == base or (o and v.group(1) == o.group(1))):
        return 1, f"set por variante de SCG ({sk})"
    s_scg = norm(RUIDO_SET.sub(" ", str(c.get("set") or "")))
    s_q = norm(q["set_nombre"])
    if s_scg and s_q and (s_scg == s_q or s_scg.replace(" ", "") == s_q.replace(" ", "")):
        return 1, "set por nombre"
    if c.get("set_code") and str(c["set_code"]).upper() == q["set"]:
        return 2, "set por codigo"
    return 0, f"otro set ({c.get('set') or c.get('sku_set') or '?'})"


def num_coincide(c, q):
    """None = SCG no dio numero; True / False."""
    num = c.get("sku_num") or c.get("collector_number")
    if not num:
        return None
    pm = promo_mapa(c, q)
    if pm:                                               # promos: el numero que corresponde (o SCG numera distinto: se acepta)
        return True
    a, b = norm_num(num), norm_num(q["numero"])
    if a == b:
        return True
    if q["set"].upper() in TIPOS_LISTA and "-" in b:     # The List: 'BBD-87' == 87
        return norm_num(b.split("-", 1)[1]) == a
    suf = sufijo_promo(q)                                # promo: '165p' == 165 / '401s' == 401A si el SKU dice que es promo
    if suf and (c.get("sku_tipo") or "").upper() in TIPO_SUFIJO[suf]:
        return re.sub(r"[a-z]$", "", a) == b[:-1]
    if ("//" in str(q["nombre"]) or "//" in str(c.get("name"))) and a == b + "a":   # doble cara: SCG numera el frente '11A'
        return True
    if q.get("idioma") == "ja" and re.fullmatch(r"\d+j", b) and a == b[:-1]:   # Scryfall '4J' = la japonesa del #4
        return True
    if set_fichas(q) and a.startswith("t") and norm_num(a[1:]) == b:   # fichas: 'T18' == 18
        return True
    return False


def num_exacto(c, q):
    """El numero coincide tal cual (sin quitar letras de SCG): desempata 401s contra 401A / 401B."""
    a, b = norm_num(c.get("sku_num") or c.get("collector_number")), norm_num(q["numero"])
    pm = promo_mapa(c, q)
    if pm:
        return pm[1] is not None and a == norm_num(pm[1])
    return bool(a) and (a == b or (sufijo_promo(q) is not None and a == b[:-1]))


def evaluar(c, q):
    """(aceptado, puntos, motivo) de un producto/variante de SCG contra la version buscada."""
    if not nombre_coincide(c.get("name"), q["nombre"]):
        return False, 0, f"otro nombre ({c.get('name')})"
    if c.get("finish") and c["finish"] != q["acabado"]:
        return False, 0, f"otro acabado ({c['finish']})"
    if c.get("idioma") and c["idioma"] != q["idioma"]:
        return False, 0, f"otro idioma ({c['idioma']})"
    nivel, mset = set_coincide(c, q)
    if not nivel:
        return False, 0, mset
    num = c.get("sku_num") or c.get("collector_number")
    if num_coincide(c, q) is False:
        return False, 0, f"otro numero ({num})"
    pm = promo_mapa(c, q)
    if pm and not num_exacto(c, q):                     # es la promo correcta pero SCG la numera distinto: probable, no exacta
        if re.fullmatch(r"\d+j", norm_num(q["numero"])) and q.get("idioma") != "ja":
            return False, 0, f"otro numero ({num})"
        return True, 12, f"{pm[0]} (SCG la numera #{num}; Scryfall #{q['numero']})"
    puntos = nivel * 10 + (5 if num else 0) + (2 if c.get("finish") else 0) + (1 if c.get("idioma") else 0) + (3 if num and num_exacto(c, q) else 0)
    return True, puntos, f"{mset}{' + numero' if num else ' (SCG no da numero)'}{'' if c.get('finish') else ' (sin acabado)'}"


def diagnostico(c, q):
    """Para un producto con el MISMO nombre: que datos si coinciden y cuales no. (aciertos, [fallas], texto corto)."""
    fallas, ok = [], 0
    nivel, _ = set_coincide(c, q)
    if nivel:
        ok += 1
    else:
        motivo = set_coincide(c, q)[1]
        fallas.append(motivo if motivo.startswith("otro tipo") else f"set (SCG: {c.get('sku_set') or c.get('set') or '?'})")
    num = c.get("sku_num") or c.get("collector_number")
    if num_coincide(c, q) is False:
        fallas.append(f"numero (SCG: {num})")
    elif num:
        ok += 1
    if c.get("finish") and c["finish"] != q["acabado"]:
        fallas.append(f"acabado (SCG: {c['finish']})")
    elif c.get("finish"):
        ok += 1
    if c.get("idioma") and c["idioma"] != q["idioma"]:
        fallas.append(f"idioma (SCG: {c['idioma']})")
    elif c.get("idioma"):
        ok += 1
    return ok, fallas


def version_txt(c):
    """'THS #107 foil en' de un producto de SCG (lo que SCG dice que es)."""
    return " ".join(str(x) for x in ((f"{c['sku_tipo']}_" if c.get("sku_tipo") else "") + str(c.get("sku_set") or c.get("set") or "?"), f"#{c.get('sku_num') or c.get('collector_number') or '?'}",
                                     c.get("finish") or "?", c.get("idioma") or "?"))


# familias de versiones que SCG normalmente NO vende: si no aparecen, se marcan aparte para no llenar 'revisar'
NO_VENDE = [("World Championship Decks", "mazos de World Championship (SCG no tiene ese mazo)"), ("Pro Tour Collector", "Pro Tour Collector Set"),
            ("Magic Premiere Shop", "basicas de Magic Premiere Shop (Japon)"), ("Japanese Promo Tokens", "fichas promo japonesas"),
            ("Summer Vacation", "promo de Summer Vacation (Japon)"), ("Japan Junior", "Japan Junior Tournament"),
            ("Vanguard", "Vanguard"), ("Unknown Event", "playtest / evento desconocido"),
            ("Planeswalker Championship", "Planeswalker Championship (Japon)"), ("Regional Promos", "promos regionales")]


def no_vende(q):
    nombre_set = str(q.get("set_nombre") or "")
    return next((txt for clave, txt in NO_VENDE if clave.lower() in nombre_set.lower()), None)


def emparejar(q, filas):
    """Estado para la version buscada + el producto elegido (NM) + todos los candidatos con su motivo."""
    if not filas:
        return "no_encontrada", None, [], "SCG no regreso nada para ese nombre"
    cand, mismo_nombre = [], 0
    for c in filas:
        ok, pts, motivo = evaluar(c, q)
        mismo_nombre += nombre_coincide(c.get("name"), q["nombre"])
        cand.append((ok, pts, motivo, c))
    if not mismo_nombre:
        return "no_encontrada", None, cand, "ningun producto con ese nombre (solo otras cartas)"
    buenos = [x for x in cand if x[0]]
    if not buenos:
        return "sin_coincidencia", None, cand, "la carta si esta, pero no esa impresion/acabado/idioma"
    mejor = max(x[1] for x in buenos)
    top = [x for x in buenos if x[1] == mejor]
    # mismo producto en varias condiciones: se toma NM; si hay varios productos distintos empatados -> ambiguo
    # un producto = una impresion (set, numero, idioma, acabado, tipo del SKU); SCG a veces repite la misma en dos fichas
    clave_prod = lambda c: ((c.get("sku_tipo") or "").upper(), (c.get("sku_set") or "").upper(), norm_num(c.get("sku_num")),
                            c.get("sku_idioma"), c.get("sku_acabado")) if c.get("sku_set") \
        else (norm(c.get("set")), norm_num(c.get("collector_number")), c.get("idioma"), c.get("finish"))
    productos = {clave_prod(x[3]) for x in top}
    nm = [x for x in top if es_nm(x[3].get("condition"))] or top
    elegido = min(nm, key=lambda x: (x[3].get("price") is None, x[3].get("price") or 0))[3]
    exacto = mejor >= 15                              # set + numero (por SKU o por campos)
    precios_nm = {x[3].get("price") for x in nm}
    if len(productos) > 1 and len(precios_nm) == 1 and None not in precios_nm:
        productos = {None}                            # dos fichas de la misma carta al mismo precio: da igual cual (no es ambiguo)
    if len(productos) > 1:
        return "ambiguo", elegido, cand, f"{len(productos)} productos de SCG encajan igual"
    if exacto:
        return "exacto", elegido, cand, top[0][2]
    return "probable", elegido, cand, top[0][2]




# ====================================================================================================
# DESCARGA Y EMPAREJADO PARA STACK
# ====================================================================================================
RUTA_DB = CARPETA_SALIDA / "scg.sqlite"
RUTA_PROGRESO = CARPETA_SALIDA / "progreso.json"
RUTA_DETENER = CARPETA_SALIDA / "descargas" / "DETENER.txt"
CAMPOS_GUARDADOS = ("name", "set", "set_code", "collector_number", "finish", "language", "condition", "price", "stock", "sku", "url", "doc")
OTROS_GUARDADOS = 12                 # de los productos de OTRAS cartas solo se guardan unos cuantos (para 'nombres que si regreso')
ESTADOS_OK = ("exacto", "probable")
DDL = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS busquedas (nombre TEXT PRIMARY KEY, ronda TEXT, filas BLOB, error TEXT, paginas INTEGER,
    cortado INTEGER, extra TEXT, capturado_utc TEXT);
CREATE TABLE IF NOT EXISTS precios (scryfall_id TEXT NOT NULL, acabado TEXT NOT NULL, estado TEXT, precio_usd REAL, stock INTEGER,
    condicion TEXT, url TEXT, sku TEXT, scg_nombre TEXT, scg_version TEXT, motivo TEXT, falla TEXT, nombre TEXT, set_code TEXT,
    numero TEXT, idioma TEXT, set_nombre TEXT, PRIMARY KEY (scryfall_id, acabado));
CREATE TABLE IF NOT EXISTS historial (dia TEXT NOT NULL, scryfall_id TEXT NOT NULL, acabado TEXT NOT NULL, precio_usd REAL,
    stock INTEGER, PRIMARY KEY (dia, scryfall_id, acabado));
CREATE TABLE IF NOT EXISTS dias (dia TEXT PRIMARY KEY, ronda TEXT, versiones INTEGER, con_precio INTEGER, registrado_utc TEXT);
"""


def ahora_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def abrir_db():
    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(RUTA_DB, timeout=30)
    con.executescript(DDL)
    return con


def meta_db(con):
    return dict(con.execute("SELECT key, value FROM meta"))


def poner_meta(con, **kv):
    con.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)", [(k, None if v is None else str(v)) for k, v in kv.items()])


def escribir_progreso(datos):
    """data/tiendas/starcitygames/progreso.json: lo lee Stack (escritura atomica)."""
    try:
        CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)
        tmp = RUTA_PROGRESO.with_name(RUTA_PROGRESO.name + ".part")
        tmp.write_text(json.dumps({"tienda": TIENDA, "actualizado": ahora_utc(), "pid": os.getpid(), "procesos": N_ACTORES, **datos},
                                  ensure_ascii=False), encoding="utf-8")
        tmp.replace(RUTA_PROGRESO)
    except OSError:
        pass


def catalogo():
    """Todas las versiones del catalogo de hoy (>= $2) como las busca el emparejado, y el dia del catalogo."""
    if not RUTA_HISTORIAL.exists():
        raise RuntimeError(f"No existe {RUTA_HISTORIAL}. Corre scryfall.py primero.")
    con = sqlite3.connect(f"{RUTA_HISTORIAL.resolve().as_uri()}?mode=ro", uri=True)
    try:
        filas = con.execute("SELECT id, finish, name, set_code, collector_number, price_usd, set_name, lang FROM cards").fetchall()
        try:
            dia = con.execute("SELECT MAX(fecha) FROM dias").fetchone()[0]
        except sqlite3.Error:
            dia = None
    finally:
        con.close()
    return [{"id": f[0], "acabado": f[1], "nombre": f[2], "set": (f[3] or "").upper(), "numero": f[4], "tcg_usd": f[5],
             "set_nombre": f[6], "idioma": f[7] or "en"} for f in filas], dia


def compactar(nombre, filas):
    """Solo lo necesario de cada producto; los de OTRAS cartas, unos cuantos. -> bytes (json + zlib)."""
    mismos = [f for f in filas if nombre_coincide(f.get("name"), nombre)]
    otros = [f for f in filas if not nombre_coincide(f.get("name"), nombre)][:OTROS_GUARDADOS]
    datos = [{k: f.get(k) for k in CAMPOS_GUARDADOS} for f in mismos + otros]
    return zlib.compress(json.dumps(datos, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8"), 6)


def descompactar(blob):
    if not blob:
        return []
    try:
        filas = json.loads(zlib.decompress(blob).decode("utf-8"))
    except (zlib.error, ValueError):
        return []
    for f in filas:
        f["finish"] = normalizar_finish(f.get("finish"))
        aplicar_sku(f)
    return filas


def pedir_detener():
    return RUTA_DETENER.exists()


def buscar_varios(palabras, al_llegar, n_actores=N_ACTORES, detener=pedir_detener, buscar=None):
    """Busca cada palabra con n_actores conexiones a la vez. al_llegar(palabra, filas, error, info) se llama en ESTE hilo
    (asi la base de datos solo se toca desde uno). Regresa True si termino, False si se pidio detener."""
    pendientes, llegadas = queue.Queue(), queue.Queue()
    for p in palabras:
        pendientes.put(p)
    alto = threading.Event()

    def actor():
        with requests.Session() as sesion:
            sesion.headers.update(HEADERS)
            while not alto.is_set():
                try:
                    p = pendientes.get_nowait()
                except queue.Empty:
                    return
                try:
                    filas, err, info = (buscar or buscar_nombre)(sesion, p)
                except Exception as e:                    # noqa: BLE001  una carta rara no tumba la descarga
                    filas, err, info = [], f"{type(e).__name__}: {e}", {"paginas": 0, "cortado": False}
                llegadas.put((p, filas, err, info))
                time.sleep(PAUSA_SEGUNDOS)

    hilos = [threading.Thread(target=actor, daemon=True) for _ in range(max(1, n_actores))]
    for h in hilos:
        h.start()
    faltan, ultimo_chequeo = len(palabras), 0.0
    while faltan:
        try:
            p, filas, err, info = llegadas.get(timeout=0.5)
            al_llegar(p, filas, err, info)
            faltan -= 1
        except queue.Empty:
            if not any(h.is_alive() for h in hilos) and llegadas.empty():
                break
        if time.time() - ultimo_chequeo > 2:
            ultimo_chequeo = time.time()
            if detener():
                alto.set()
    alto.set()
    for h in hilos:
        h.join(timeout=60)
    while not llegadas.empty():                           # lo que llego mientras se detenian
        al_llegar(*llegadas.get_nowait())
    return not detener() and faltan <= 0


def filas_por_nombre(con):
    out, extra = {}, {}
    for nombre, blob, error, cortado, ex in con.execute("SELECT nombre, filas, error, cortado, extra FROM busquedas"):
        out[nombre] = descompactar(blob)
        extra[nombre] = {"error": error, "cortado": bool(cortado), "extra": json.loads(ex) if ex else []}
    return out, extra


def resultado_version(q, filas, info):
    """(estado, elegido, motivo, falla, lo_mas_parecido) de una version con lo que hay guardado de su nombre."""
    if info and info.get("error") and not filas:
        return "error", None, info["error"], "error de la busqueda", None
    estado, elegido, cand, motivo = emparejar(q, filas)
    if estado in ("sin_coincidencia", "no_encontrada") and no_vende(q):
        estado, motivo = "no_vende_scg", f"{no_vende(q)}: SCG normalmente no la vende ({motivo})"
    mismos = [c for c in filas if nombre_coincide(c.get("name"), q["nombre"])]
    diag = [(diagnostico(c, q), c) for c in mismos]
    cercano = max(diag, key=lambda x: (x[0][0], -len(x[0][1]), es_nm(x[1].get("condition"))))[1] if diag else None
    falla = ("" if estado in ESTADOS_OK else "nombre (SCG no tiene una carta con ese nombre exacto)" if not mismos
             else ", ".join(max(diag, key=lambda x: (x[0][0], -len(x[0][1])))[0][1]) or ("varios productos empatados" if estado == "ambiguo" else ""))
    return estado, elegido, motivo, falla, cercano


def segunda_busqueda(versiones, filas, extra):
    """Palabras 'nombre + set' para las versiones que no salieron y cuya carta tiene muchas impresiones (o error)."""
    kws = {}
    for q in versiones:
        fs = filas.get(q["nombre"])
        if fs is None:
            continue
        info = extra.get(q["nombre"]) or {}
        if info.get("error"):
            kws.setdefault(q["nombre"], (q["nombre"], q["nombre"]))      # reintento de las que fallaron
            continue
        if emparejar(q, fs)[0] not in ("sin_coincidencia", "no_encontrada"):
            continue
        mismos = sum(1 for c in fs if nombre_coincide(c.get("name"), q["nombre"]))
        if info.get("cortado") or mismos >= 40 or not mismos:
            kw = f"{q['nombre']} {q.get('set_nombre') or q['set']}"
            if kw not in info.get("extra", []):
                kws[kw] = (kw, q["nombre"])
    return list(kws.values())


def emparejar_todo(con, versiones, ronda, avisar=print):
    """Rehace la tabla precios con lo guardado y registra el dia en el historial. -> resumen."""
    filas, extra = filas_por_nombre(con)
    cuenta, salida = {}, []
    for q in versiones:
        if q["nombre"] not in filas:
            continue
        estado, e, motivo, falla, cerca = resultado_version(q, filas[q["nombre"]], extra.get(q["nombre"]))
        e = e or {}
        cuenta[estado] = cuenta.get(estado, 0) + 1
        salida.append((q["id"], q["acabado"], estado, e.get("price"), e.get("stock"), e.get("condition"), e.get("url") or (cerca or {}).get("url"),
                       e.get("sku") or (cerca or {}).get("sku"), (e or cerca or {}).get("name"),
                       version_txt(e) if e else (version_txt(cerca) if cerca else None), motivo, falla,
                       q["nombre"], q["set"], q["numero"], q["idioma"], q.get("set_nombre")))
    con.execute("DELETE FROM precios")
    con.executemany("INSERT OR REPLACE INTO precios VALUES (" + ",".join("?" * 17) + ")", salida)
    dia = f"{ronda[:4]}-{ronda[4:6]}-{ronda[6:8]}" if ronda and re.match(r"\d{8}", ronda) else datetime.now().date().isoformat()
    con.execute("DELETE FROM historial WHERE dia = ?", (dia,))
    con.execute("INSERT INTO historial SELECT ?, scryfall_id, acabado, precio_usd, stock FROM precios "
                "WHERE estado IN ('exacto', 'probable') AND precio_usd IS NOT NULL", (dia,))
    con_precio = sum(1 for x in salida if x[2] in ESTADOS_OK and x[3] is not None)
    con.execute("INSERT OR REPLACE INTO dias VALUES (?, ?, ?, ?, ?)", (dia, ronda, len(salida), con_precio, ahora_utc()))
    total = len(salida)
    no_vende_n = cuenta.get("no_vende_scg", 0)
    aciertos = cuenta.get("exacto", 0) + cuenta.get("probable", 0)
    hit = round(aciertos / max(1, total - no_vende_n) * 100, 1)
    poner_meta(con, emparejado_utc=ahora_utc(), versiones=total, con_precio=con_precio, hit=hit, conteo=json.dumps(cuenta), dia=dia)
    con.commit()
    avisar(f"StarCityGames: {total:,} versiones · {aciertos:,} encontradas ({hit}% sin contar las que SCG no vende) · "
           + " · ".join(f"{k} {v:,}" for k, v in sorted(cuenta.items())))
    return {"versiones": total, "con_precio": con_precio, "hit": hit, "conteo": cuenta, "dia": dia}


def descargar(n_actores=N_ACTORES):
    RUTA_DETENER.unlink(missing_ok=True)
    try:
        versiones, dia_cat = catalogo()
    except Exception as e:                                # noqa: BLE001
        escribir_progreso({"estado": "error", "detalle": str(e)})
        print(f"ERROR: {e}")
        return 1
    nombres = sorted({v["nombre"] for v in versiones})
    con = abrir_db()
    m = meta_db(con)
    if m.get("ronda_estado") in ("descargando", "detenido") and m.get("ronda"):
        ronda = m["ronda"]                                 # se retoma la descarga a medias
        print(f"Retomando la descarga {ronda}.")
    else:
        ronda = datetime.now().strftime("%Y%m%d_%H%M%S")
        poner_meta(con, ronda=ronda, ronda_inicio_utc=ahora_utc(), catalogo_dia=dia_cat)
    poner_meta(con, ronda_estado="descargando")
    con.commit()
    m = meta_db(con)
    hechos = {r[0] for r in con.execute("SELECT nombre FROM busquedas WHERE ronda = ?", (ronda,))}
    pendientes = [n for n in nombres if n not in hechos]
    total, t0 = len(nombres), time.time()
    estado = {"hechas": len(hechos), "productos": 0, "errores": 0, "ultimo": 0.0}
    print(f"StarCityGames: {len(versiones):,} versiones, {total:,} nombres; faltan {len(pendientes):,}. {n_actores} actores.")

    def progreso(fase=None, **extra):
        hechas_ahora = estado["hechas"] - len(hechos)
        ritmo = hechas_ahora / max(1e-9, time.time() - t0)
        faltan_min = round((total - estado["hechas"]) / ritmo / 60) if ritmo > 0 and fase is None else None
        escribir_progreso({"estado": "descargando", "hechas": estado["hechas"], "total": total, "productos": estado["productos"],
                           "errores": estado["errores"], "faltan_min": faltan_min, "inicio": m.get("ronda_inicio_utc") or ahora_utc(),
                           "archivo": RUTA_DB.name, **({"fase": fase} if fase else {}), **extra})

    def guardar(nombre, filas, err, info, kw_extra=None):
        if kw_extra:                                       # segunda busqueda: se agrega a lo que ya habia de esa carta
            fila = con.execute("SELECT filas, extra FROM busquedas WHERE nombre = ?", (kw_extra,)).fetchone()
            previas = descompactar(fila[0]) if fila else []
            ya = {(c.get("doc"), c.get("sku"), c.get("condition")) for c in previas}
            nuevas = [c for c in filas if (c.get("doc"), c.get("sku"), c.get("condition")) not in ya]
            lista = (json.loads(fila[1]) if fila and fila[1] else []) + [nombre]
            if nombre == kw_extra:                         # reintento de una que fallo
                con.execute("UPDATE busquedas SET filas = ?, error = ?, cortado = ?, capturado_utc = ? WHERE nombre = ?",
                            (compactar(kw_extra, filas or previas), err if not filas else None, int(info.get("cortado", False)), ahora_utc(), kw_extra))
            else:
                con.execute("UPDATE busquedas SET filas = ?, extra = ? WHERE nombre = ?", (compactar(kw_extra, previas + nuevas), json.dumps(lista), kw_extra))
        else:
            con.execute("INSERT OR REPLACE INTO busquedas VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (nombre, ronda, compactar(nombre, filas), err, info.get("paginas", 0), int(info.get("cortado", False)), None, ahora_utc()))
            estado["hechas"] += 1
            estado["productos"] += sum(1 for c in filas if nombre_coincide(c.get("name"), nombre))
            estado["errores"] += bool(err)
        if time.time() - estado["ultimo"] > 2:
            con.commit()
            estado["ultimo"] = time.time()
            progreso()

    progreso()
    completo = buscar_varios(pendientes, lambda p, f, e, i: guardar(p, f, e, i), n_actores)
    con.commit()
    if not completo:
        poner_meta(con, ronda_estado="detenido")
        con.commit()
        escribir_progreso({"estado": "detenido", "hechas": estado["hechas"], "total": total, "productos": estado["productos"],
                           "errores": estado["errores"], "archivo": RUTA_DB.name})
        print("Detenido: la proxima vez se retoma donde se quedo.")
        RUTA_DETENER.unlink(missing_ok=True)
        return 0
    # segunda busqueda (nombre + set) y reintento de errores
    filas, extra = filas_por_nombre(con)
    kws = segunda_busqueda(versiones, filas, extra)
    if kws:
        print(f"Segunda busqueda para {len(kws):,} versiones (nombre + set, o reintento).")
        destino = dict(kws)
        hechas2 = {"n": 0}

        def guardar2(p, f, e, i):
            hechas2["n"] += 1
            guardar(p, f, e, i, kw_extra=destino.get(p, p))
            if hechas2["n"] % 10 == 0:
                progreso(fase=f"segunda busqueda {hechas2['n']}/{len(kws)}")
        buscar_varios([k for k, _ in kws], guardar2, n_actores)
        con.commit()
    escribir_progreso({"estado": "descargando", "hechas": total, "total": total, "productos": estado["productos"], "fase": "emparejando"})
    res = emparejar_todo(con, versiones, ronda)
    poner_meta(con, ronda_estado="terminado", ronda_fin_utc=ahora_utc())
    con.commit()
    con.close()
    escribir_progreso({"estado": "terminado", "hechas": total, "total": total, "productos": res["con_precio"], "errores": estado["errores"],
                       "archivo": RUTA_DB.name, "hit": res["hit"], "versiones": res["versiones"]})
    RUTA_DETENER.unlink(missing_ok=True)
    return 0


def solo_emparejar():
    versiones, _ = catalogo()
    con = abrir_db()
    try:
        m = meta_db(con)
        return emparejar_todo(con, versiones, m.get("ronda") or "")
    finally:
        con.close()


# ---------------------------------------------------------------- casos reales verificados (python scg.py probar)
# (descripcion, SKU de SCG, numero de SCG, nombre de SCG, version buscada: set, numero, acabado, idioma, debe_coincidir)
PRUEBAS = [
    ("normal", "SGL-MTG-THS-107-ENN", "107", None, ("THS", "107", "nonfoil", "en"), True),
    ("promo pack p", "SGL-MTG-PRM-PP_ECL_165-ENN", "165", None, ("PECL", "165p", "nonfoil", "en"), True),
    ("prerelease s", "SGL-MTG-PRM-PRE_BFZ_006S-ENF", "006s", None, ("PBFZ", "6s", "foil", "en"), True),
    ("prerelease s con letra", "SGL-MTG-PRM-PRE_LTR_401A-ENF", "401A", None, ("PLTR", "401s", "foil", "en"), True),
    ("prerelease s != promo pack", "SGL-MTG-PRM-PP_STX_086-ENF", "086", None, ("PSTX", "86s", "foil", "en"), False),
    ("launch party s", "SGL-MTG-PRM-LNCH_BBD_021-ENF", "021", None, ("PBBD", "21s", "foil", "en"), True),
    ("normal != promo pack", "SGL-MTG-PRM-PP_ECL_165-ENN", "165", None, ("ECL", "165", "nonfoil", "en"), False),
    ("challenger deck", "SGL-MTG-PRM-CD_Q06_009-ENN", "009", None, ("Q06", "9", "nonfoil", "en"), True),
    ("secret lair", "SGL-MTG-PRM-SECRET_SLC_005-ENN", "005", None, ("SLC", "5", "nonfoil", "en"), True),
    ("the list", "SGL-MTG-PWSB-BBD_087-ENN", "087", None, ("PLST", "BBD-87", "nonfoil", "en"), True),
    ("the list != original", "SGL-MTG-BBD-087-ENN", "087", None, ("PLST", "BBD-87", "nonfoil", "en"), False),
    ("normal != the list", "SGL-MTG-PWSB-FUT_153-ENN", "153", None, ("FUT", "153", "nonfoil", "en"), False),
    ("arena 2004", "SGL-MTG-PRM-ARENA_2004_006-ENF", "006", None, ("PAL04", "6", "foil", "en"), True),
    ("fnm 2009", "SGL-MTG-PRM-FNM_2009_012-ENF", "012", None, ("F09", "12", "foil", "en"), True),
    ("lunar new year", "SGL-MTG-PRM-LNY_2021_003-ENF", "003", None, ("PL21", "3", "foil", "en"), True),
    ("love your lgs", "SGL-MTG-PRM-LYLGS_2024_004-JAF", "004", None, ("PLG24", "4", "foil", "ja"), True),
    ("mazo WC 2001", "SGL-MTG-01TVDL_MMQ_324-ENN", "324", None, ("WC01", "tvdl324", "nonfoil", "en"), True),
    ("mazo WC de otro jugador", "SGL-MTG-00JK_MMQ_324-ENN", "324", None, ("WC01", "tvdl324", "nonfoil", "en"), False),
    ("love your lgs 2021B", "SGL-MTG-PRM-LYLGS_2021B_003-ENF", "003", None, ("PLG21", "3", "foil", "en"), True),
    ("champs", "SGL-MTG-PRM-CHMP_2008_011-ENN", "011", None, ("PCMP", "11", "nonfoil", "en"), True),
    ("japan standard cup", "SGL-MTG-PRM-JSC_2025_002-JAF", "002", None, ("PJSC", "2025-2", "foil", "ja"), True),
    ("store championship", "SGL-MTG-PRM-SCHP_2023_018-ENN", "018", None, ("SCH", "18", "nonfoil", "en"), True),
    ("regional qualifier", "SGL-MTG-PRM-PRMR_2022B_001-ENF", "001", None, ("PRCQ", "1", "foil", "en"), True),
    ("timeshift MH2", "SGL-MTG-MH22-007-ENF", "007", None, ("H2R", "7", "foil", "en"), True),
    ("marvel legends", "SGL-MTG-PRM-MLF_2025_003-ENF", "003", None, ("LMAR", "3", "foil", "en"), True),
    ("ravnica weekend", "SGL-MTG-PRM-RVWK_B02-ENF", "B02", None, ("PRW2", "B02", "foil", "en"), True),
    ("oversized", "SGL-MTG-C172-35-ENF", "35", None, ("OC17", "35", "foil", "en"), True),
    ("comic-con 2018", "SGL-MTG-PRM-SDCC18_M19_106-ENF", "106", None, ("PS18", "106", "foil", "en"), True),
    ("magicfest", "SGL-MTG-PRM-FEST_2025_002-ENF", "002", None, ("PF25", "2", "foil", "en"), True),
    ("ponies", "SGL-MTG-PRM-PONY_002-ENF", "002", None, ("PTG", "2", "foil", "en"), True),
    ("hascon", "SGL-MTG-PRM-HSCON_2017_003-ENF", "003", None, ("H17", "3", "foil", "en"), True),
    ("fnm 2000", "SGL-MTG-PRM-FNM_2000_010-ENF", "010", None, ("FNM", "10", "foil", "en"), True),
    ("invocations", "SGL-MTG-MPS3-44-ENF", "44", None, ("MP2", "44", "foil", "en"), True),
    ("meld 28A", "SGL-MTG-EMN-28a-ENN", "28A", "Gisela, the Broken Blade // Brisela, Voice of Nightmares", ("EMN", "28", "nonfoil", "en"), True),
    ("buy-a-box BLB en BLC", "SGL-MTG-PRM-BAB_BLB_356-ENN", "356", None, ("BLC", "356", "nonfoil", "en"), True),
    ("game day != normal", "SGL-MTG-PRM-GMDY_FRF_054-ENF", "054", None, ("FRF", "54", "foil", "en"), False),
    ("game day promo", "SGL-MTG-PRM-GMDY_FRF_054-ENF", "054", None, ("PFRF", "54", "foil", "en"), True),
    ("ficha T18", "SGL-MTG-CMA-T18-ENN", "T18", "{Drake Token}", ("TCMA", "18", "nonfoil", "en"), True),
    ("ficha de otro set", "SGL-MTG-AKH-T18-ENN", "T18", "{Drake Token}", ("TCMA", "18", "nonfoil", "en"), False),
    ("japonesa con J en el numero", "SGL-MTG-PRM-LYLGS_2024_004-JAF", "004", None, ("PLG24", "4J", "foil", "ja"), True),
    ("J solo vale en japones", "SGL-MTG-PRM-LYLGS_2024_004-ENF", "004", None, ("PLG24", "4J", "foil", "en"), False),
    ("standard series", "SGL-MTG-PRM-ALT_BFZ_241-ENN", "241", None, ("PSS1", "241", "nonfoil", "en"), True),
    ("normal != alt", "SGL-MTG-PRM-ALT_BFZ_241-ENN", "241", None, ("BFZ", "241", "nonfoil", "en"), False),
    ("estrella foil", "SGL-MTG-9ED-245-ENF", "245", None, ("9ED", "245\u2605", "foil", "en"), True),
    ("variante SCG (WAR2)", "SGL-MTG-WAR2-1-JAN", "1", None, ("WAR", "1\u2605", "nonfoil", "ja"), True),
    ("timeshift MH13", "SGL-MTG-MH13-036-ENE", "036", None, ("H1R", "36", "etched", "en"), True),
    ("doble cara 11A", "SGL-MTG-V17-11A-ENF", "11A", "Huntmaster of the Fells // Ravager of the Fells", ("V17", "11", "foil", "en"), True),
    ("meld con nombre largo", "SGL-MTG-PRM-PRE_EMN_204S-ENF", "204s", "Hanweir Battlements // Hanweir, the Writhing Township", ("PEMN", "204s", "foil", "en"), True),
    ("otra variante (46d != 46A)", "SGL-MTG-POR-46a-ENN", "46A", None, ("POR", "46d", "nonfoil", "en"), False),
    ("mazo WC != the list", "SGL-MTG-PWSB-JUD_072-ENN", "072", None, ("WC03", "pk72sb", "nonfoil", "en"), False),
    ("basica japonesa de tienda != 4ED", "SGL-MTG-4BB-378-JAN", "378", None, ("PMPS07", "5", "nonfoil", "ja"), False),
]


def probar():
    mal = 0
    for desc, sku, num, nombre_scg, (set_, numero, acabado, idioma), espera in PRUEBAS:
        nombre = (nombre_scg.split(" // ")[0] if "Huntmaster" not in nombre_scg else nombre_scg) if nombre_scg else "Carta"
        nombre = re.sub(r"^\{(.*) Token\}$", r"\1", nombre)
        c = aplicar_sku({"name": nombre_scg or nombre, "set": "?", "set_code": None, "collector_number": num, "finish": None,
                         "language": None, "sku": sku, "doc": "d", "condition": "Near Mint", "price": 1.0, "url": None})
        q = {"nombre": nombre, "set": set_, "numero": numero, "acabado": acabado, "idioma": idioma, "set_nombre": ""}
        ok, _, motivo = evaluar(c, q)
        bien = ok == espera
        mal += not bien
        print(f"  {'ok ' if bien else 'MAL'} {desc:<34} {set_} #{numero:<8} vs {sku:<34} -> {'coincide' if ok else 'no'} ({motivo})")
    print(f"\n{len(PRUEBAS) - mal} de {len(PRUEBAS)} bien." + (" REVISA LAS 'MAL'." if mal else ""))
    return mal



def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    accion = (argv[0] if argv else "descargar").lower()
    if accion == "detener":
        RUTA_DETENER.parent.mkdir(parents=True, exist_ok=True)
        RUTA_DETENER.write_text("detener", encoding="utf-8")
        print("Se pidio detener: termina lo que tiene en curso y guarda.")
        return 0
    if accion == "emparejar":
        solo_emparejar()
        return 0
    if accion == "probar":
        return 1 if probar() else 0
    return descargar()


if __name__ == "__main__":
    sys.exit(main())
