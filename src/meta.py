"""
META para Stack (0.9.0): los decks que se estan jugando en cada formato, para saber que cartas pide el meta y cuanto
cuesta armar cada deck.

De donde salen:
  MTGO (oficial)   mtgo.com/decklists: Challenges (top 32), Leagues (5-0 que escoge Wizards), Qualifiers. Diario.
  MTGTop8          torneos de papel de todo el mundo, con el nombre del arquetipo. Diario (pausa de 5 s: la pide su robots.txt).
  MTGGoldfish      % del meta por arquetipo y una lista tipica de cada uno. Cada semana.
  TopDeck.gg       torneos de papel con API (cEDH sobre todo). Necesita una llave gratis (secreto TOPDECK_KEY); sin
                   llave se salta. Pide poner su credito con un link (Stack lo muestra en META).
  EDHREC           Commander casual: comandantes mas jugados, sus cartas recomendadas y el deck promedio. Cada semana.

Formatos: standard, pioneer, modern, legacy, pauper, cedh y commander.

Archivo: data/meta/meta.sqlite (se publica en el release del dia como meta.sqlite.gz). Se van juntando los decks de los
ultimos DIAS_GUARDAR dias; lo que ya esta no se vuelve a bajar.

El arquetipo de los decks que no lo traen (MTGO, TopDeck) se pone comparando sus cartas con las listas de MTGGoldfish y
con los decks de MTGTop8 (el mas parecido, si se parece lo suficiente).

Uso:   python src/meta.py                 (todo lo que toque hoy)
       python src/meta.py --solo mtgo     (una fuente: mtgo, mtgtop8, goldfish, topdeck, edhrec)
       python src/meta.py --rapido        (pocos eventos por fuente; para probar)
       python src/meta.py clasificar      (vuelve a poner arquetipos sin conectarse)

OJO: salvo MTGO y TopDeck, son paginas web, no APIs: pueden cambiar o bloquear. Si una fuente falla, las demas siguen y
lo que ya habia se queda.
"""
import argparse
import html
import json
import math
import os
import re
import sqlite3
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, ValueError):
    pass

RAIZ = Path(__file__).resolve().parent.parent
CARPETA = RAIZ / "data" / "meta"
RUTA_DB = CARPETA / "meta.sqlite"
RUTA_ORACULO = RAIZ / "data" / "precios" / "oraculo.sqlite"

DIAS_META = 21           # ventana del meta (los % y las staples salen de aqui)
DIAS_GUARDAR = 60        # despues se borran (el archivo no crece sin fin)
DIAS_SEMANAL = 6         # MTGGoldfish y EDHREC: se vuelven a bajar si lo guardado tiene mas de esto
TOP_EDHREC = 60          # comandantes de EDHREC con sus recomendadas y su deck promedio
TOP_ARQUETIPOS_GF = 25   # listas de MTGGoldfish por formato
PARECIDO_MIN = 0.30      # para ponerle arquetipo a un deck (coseno de sus cartas)

NAVEGADOR = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:157.0) Gecko/20100101 Firefox/157.0"
SESION = requests.Session()
SESION.headers.update({"User-Agent": NAVEGADOR, "Accept-Language": "en-US,en;q=0.8"})

FORMATOS = {
    #            MTGO          MTGTop8  MTGGoldfish  TopDeck
    "standard": {"mtgo": "standard", "t8": "ST", "gf": "standard", "td": "Standard"},
    "pioneer": {"mtgo": "pioneer", "t8": "PI", "gf": "pioneer", "td": "Pioneer"},
    "modern": {"mtgo": "modern", "t8": "MO", "gf": "modern", "td": "Modern"},
    "legacy": {"mtgo": "legacy", "t8": "LE", "gf": "legacy", "td": "Legacy"},
    "pauper": {"mtgo": "pauper", "t8": "PAU", "gf": "pauper", "td": "Pauper"},
    "cedh": {"t8": "cEDH", "td": "EDH"},
    "commander": {},     # EDHREC
}
COMANDANTES = {"cedh", "commander"}
BASICAS = {"plains", "island", "swamp", "mountain", "forest", "wastes", "snow-covered plains", "snow-covered island",
           "snow-covered swamp", "snow-covered mountain", "snow-covered forest"}

DDL = """
CREATE TABLE IF NOT EXISTS dias (dia TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS meta (clave TEXT PRIMARY KEY, valor TEXT);
CREATE TABLE IF NOT EXISTS fuentes (fuente TEXT, formato TEXT, ultimo_ok TEXT, ultimo_error TEXT, error TEXT, n INTEGER,
  PRIMARY KEY (fuente, formato));
CREATE TABLE IF NOT EXISTS eventos (id TEXT PRIMARY KEY, fuente TEXT, formato TEXT, nombre TEXT, fecha TEXT, jugadores INTEGER,
  url TEXT);
CREATE TABLE IF NOT EXISTS decks (id TEXT PRIMARY KEY, evento TEXT, fuente TEXT, formato TEXT, fecha TEXT, jugador TEXT,
  puesto INTEGER, resultado TEXT, arquetipo TEXT, arq_fuente TEXT, parecido REAL, comandante TEXT, url TEXT);
CREATE INDEX IF NOT EXISTS decks_formato ON decks (formato, fecha);
CREATE TABLE IF NOT EXISTS cartas (deck TEXT, nombre TEXT, cantidad INTEGER, zona TEXT, PRIMARY KEY (deck, nombre, zona));
CREATE TABLE IF NOT EXISTS arquetipos (formato TEXT, nombre TEXT, fuente TEXT, pct REAL, n INTEGER, url TEXT, colores TEXT,
  claves TEXT, lista TEXT, dia TEXT, PRIMARY KEY (formato, nombre));
CREATE TABLE IF NOT EXISTS edhrec_comandantes (nombre TEXT PRIMARY KEY, slug TEXT, rango INTEGER, n_decks INTEGER,
  tendencia REAL, dia TEXT);
CREATE TABLE IF NOT EXISTS edhrec_recomendadas (comandante TEXT, carta TEXT, categoria TEXT, sinergia REAL, n_decks INTEGER,
  potencial INTEGER, PRIMARY KEY (comandante, carta));
CREATE TABLE IF NOT EXISTS edhrec_promedio (comandante TEXT, carta TEXT, cantidad INTEGER, tipo TEXT,
  PRIMARY KEY (comandante, carta));
"""


def hoy():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def hace(dias):
    return (datetime.now(timezone.utc) - timedelta(days=dias)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------- nombres de cartas (como los escribe Scryfall)
def clave_carta(nombre):
    """La misma llave que nube.py (oraculo): minusculas, sin acentos, '/' y '//' iguales."""
    t = unicodedata.normalize("NFKD", str(nombre or "")).encode("ascii", "ignore").decode().lower()
    t = re.sub(r"\s*/+\s*", " / ", t)
    t = re.sub(r"[^a-z0-9/ ]+", "", t)
    return re.sub(r"\s+", " ", t).strip()


class Nombres:
    """Pasa 'Wear/Tear', 'Delver of Secrets' o 'Jotun Grunt' al nombre de Scryfall (si hay oraculo)."""
    def __init__(self, ruta=RUTA_ORACULO):
        self.alias = {}
        if Path(ruta).is_file():
            con = sqlite3.connect(f"file:{ruta}?mode=ro", uri=True)
            try:
                self.alias = dict(con.execute("SELECT clave, nombre FROM alias"))
            finally:
                con.close()

    def __call__(self, nombre):
        nombre = html.unescape(str(nombre or "")).strip()
        return self.alias.get(clave_carta(nombre), nombre)


# ---------------------------------------------------------------- red
def pedir(url, metodo="GET", intentos=4, pausa=0.0, **kw):
    """GET/POST con reintentos. Regresa la respuesta (status 200) o lanza el ultimo error."""
    ultimo = None
    for i in range(intentos):
        try:
            r = SESION.request(metodo, url, timeout=40, **kw)
            if r.status_code == 200:
                if pausa:
                    time.sleep(pausa)
                return r
            if r.status_code in (403, 404):
                raise RuntimeError(f"HTTP {r.status_code} en {url}")
            ultimo = RuntimeError(f"HTTP {r.status_code} en {url}")
        except requests.RequestException as e:
            ultimo = e
        time.sleep(3 * (2 ** i))
    raise ultimo


def bloqueado(texto):
    return "Just a moment..." in texto[:3000] or "challenge-platform" in texto[:6000]


# ---------------------------------------------------------------- base de datos
def abrir():
    CARPETA.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(RUTA_DB)
    con.executescript(DDL)
    return con


def anotar_fuente(con, fuente, formato, ok, n=0, error=None):
    if ok:
        con.execute("INSERT INTO fuentes (fuente, formato, ultimo_ok, n) VALUES (?, ?, ?, ?) ON CONFLICT (fuente, formato) "
                    "DO UPDATE SET ultimo_ok = excluded.ultimo_ok, n = excluded.n, error = NULL",
                    (fuente, formato, datetime.now(timezone.utc).isoformat(timespec="seconds"), n))
    else:
        con.execute("INSERT INTO fuentes (fuente, formato, ultimo_error, error) VALUES (?, ?, ?, ?) ON CONFLICT (fuente, formato) "
                    "DO UPDATE SET ultimo_error = excluded.ultimo_error, error = excluded.error",
                    (fuente, formato, datetime.now(timezone.utc).isoformat(timespec="seconds"), str(error)[:300]))
    con.commit()


def ya_esta(con, deck_id):
    return con.execute("SELECT 1 FROM decks WHERE id = ?", (deck_id,)).fetchone() is not None


def guardar_deck(con, d, cartas):
    """d: dict con las columnas de decks; cartas: [(nombre, cantidad, zona)]. Sin cartas no se guarda."""
    juntas = defaultdict(int)
    for nombre, q, zona in cartas:
        if nombre and q > 0:
            juntas[(nombre, zona)] += int(q)
    if not juntas:
        return False
    con.execute("INSERT OR REPLACE INTO decks (id, evento, fuente, formato, fecha, jugador, puesto, resultado, arquetipo, "
                "arq_fuente, parecido, comandante, url) VALUES (:id, :evento, :fuente, :formato, :fecha, :jugador, :puesto, "
                ":resultado, :arquetipo, :arq_fuente, NULL, :comandante, :url)",
                {"evento": None, "jugador": None, "puesto": None, "resultado": None, "arquetipo": None, "arq_fuente": None,
                 "comandante": None, "url": None, **d})
    con.execute("DELETE FROM cartas WHERE deck = ?", (d["id"],))
    con.executemany("INSERT INTO cartas VALUES (?, ?, ?, ?)", [(d["id"], n, q, z) for (n, z), q in juntas.items()])
    return True


def leer_lista(texto, nombres):
    """Texto de deck ("4 Lightning Bolt", lineas en blanco o 'Sideboard' separan el side) -> [(nombre, cantidad, zona)]."""
    zona, out = "main", []
    for linea in str(texto or "").replace("\r", "").split("\n"):
        l = linea.strip()
        if not l:
            if out:
                zona = "side"
            continue
        b = l.lower().strip("~:# ")
        if b in ("sideboard", "side", "sb"):
            zona = "side"
            continue
        if b in ("commander", "commanders", "comandante"):
            zona = "cmd"
            continue
        if b in ("mainboard", "main", "deck", "companion"):
            zona = "main" if b != "companion" else "side"
            continue
        m = re.match(r"^(?:SB:\s*)?(\d+)\s*x?\s+(.+?)(?:\s+\([A-Za-z0-9]{2,6}\)\s*[\w-]*)?(?:\s+\*F\*)?$", l)
        if m:
            out.append((nombres(m.group(2)), int(m.group(1)), "side" if l.startswith("SB:") else zona))
    return out


# ---------------------------------------------------------------- MTGO (oficial)
RX_MTGO = re.compile(r'href="/decklist/([a-z0-9-]+?)-(\d{4}-\d{2}-\d{2})(\d+)"')


def mtgo(con, nombres, rapido=False):
    desde = hace(DIAS_META)
    meses = sorted({(datetime.now(timezone.utc) - timedelta(days=d)).strftime("%Y/%m") for d in (0, DIAS_META)})
    eventos = []
    for mes in meses:
        r = pedir(f"https://www.mtgo.com/decklists/{mes}", pausa=1)
        for slug, fecha, num in RX_MTGO.findall(r.text):
            fmt = next((f for f, c in FORMATOS.items() if c.get("mtgo") and slug.startswith(c["mtgo"] + "-")), None)
            if fmt and fecha >= desde:
                eventos.append((fmt, slug, fecha, num))
    eventos = sorted(set(eventos), key=lambda e: e[2], reverse=True)
    if rapido:
        eventos = eventos[:6]
    por_formato = Counter()
    for fmt, slug, fecha, num in eventos:
        ev_id = f"mtgo:{num}"
        if con.execute("SELECT 1 FROM eventos WHERE id = ?", (ev_id,)).fetchone():
            continue
        url = f"https://www.mtgo.com/decklist/{slug}-{fecha}{num}"
        try:
            r = pedir(url, pausa=1)
            m = re.search(r"window\.MTGO\.decklists\.data\s*=\s*(\{.*?\});\s*\n", r.text, re.S)
            if not m:
                continue
            data = json.loads(m.group(1))
        except Exception as e:  # noqa: BLE001
            print(f"  MTGO {slug}: {e}")
            continue
        puestos = {s.get("loginid"): int(s["rank"]) for s in (data.get("standings") or data.get("final_rank") or [])
                   if s.get("rank") and str(s["rank"]).isdigit()}
        jugadores = int((data.get("player_count") or {}).get("players") or 0) or None
        con.execute("INSERT OR REPLACE INTO eventos VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (ev_id, "mtgo", fmt, data.get("description") or slug, fecha, jugadores, url))
        es_liga = "league" in slug
        for dl in data.get("decklists") or []:
            cartas = [(nombres(c["card_attributes"]["card_name"]), int(c.get("qty") or 0), "side" if c.get("sideboard") == "true" else "main")
                      for c in (dl.get("main_deck") or []) + (dl.get("sideboard_deck") or [])]
            guardar_deck(con, {"id": f"mtgo:{dl.get('decktournamentid') or (num + ':' + str(dl.get('loginid')))}", "evento": ev_id,
                               "fuente": "mtgo", "formato": fmt, "fecha": fecha, "jugador": dl.get("player"),
                               "puesto": puestos.get(dl.get("loginid")), "resultado": "5-0" if es_liga else None, "url": url},
                         cartas)
            por_formato[fmt] += 1
        con.commit()
    for fmt in (f for f, c in FORMATOS.items() if c.get("mtgo")):
        anotar_fuente(con, "mtgo", fmt, True, por_formato[fmt])
    print(f"MTGO: {sum(por_formato.values())} decks nuevos {dict(por_formato)}")


# ---------------------------------------------------------------- MTGTop8 (papel, con arquetipo)
PAUSA_T8 = 5.0           # su robots.txt pide 5 s entre peticiones
DECKS_POR_EVENTO_T8 = 8
EVENTOS_POR_DIA_T8 = 8   # eventos nuevos por formato en cada corrida (lo demas llega los dias siguientes)


def _fecha_t8(txt):
    try:
        return datetime.strptime(txt, "%d/%m/%y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def mtgtop8(con, nombres, rapido=False):
    desde = hace(DIAS_META)
    total = 0
    for fmt, cod in FORMATOS.items():
        if not cod.get("t8"):
            continue
        n_fmt = 0
        try:
            r = pedir(f"https://www.mtgtop8.com/format?f={cod['t8']}", pausa=PAUSA_T8)
            txt = r.content.decode("latin-1")
            eventos = []
            for fila in re.findall(r"<tr class=hover_tr>(.*?)</tr>", txt, re.S):
                m = re.search(r"href=event\?e=(\d+)&f=" + re.escape(cod["t8"]) + r">([^<]+)</a>", fila)
                f = re.search(r"(\d\d/\d\d/\d\d)</td>", fila)
                if not m or not f:
                    continue
                if "online/mtgo.png" in fila or m.group(2).startswith("MTGO "):
                    continue                          # MTGO ya llega directo de la fuente oficial
                fecha = _fecha_t8(f.group(1))
                if fecha and fecha >= desde:
                    eventos.append((m.group(1), html.unescape(m.group(2)), fecha, fila.count("star.png")))
            eventos = [e for e in dict.fromkeys(eventos) if not con.execute("SELECT 1 FROM eventos WHERE id = ?", (f"t8:{e[0]}",)).fetchone()]
            eventos = eventos[: 2 if rapido else EVENTOS_POR_DIA_T8]
            for ev, nombre_ev, fecha, estrellas in eventos:
                ev_id = f"t8:{ev}"
                if con.execute("SELECT 1 FROM eventos WHERE id = ?", (ev_id,)).fetchone():
                    continue
                url_ev = f"https://www.mtgtop8.com/event?e={ev}&f={cod['t8']}"
                pag = pedir(url_ev, pausa=PAUSA_T8).content.decode("latin-1")
                jug = re.search(r"(\d+) players", pag)
                decks = []
                for d, arq in re.findall(r"href=\?e=" + ev + r"&d=(\d+)&f=" + re.escape(cod["t8"]) + r">([^<]+)</a>", pag):
                    arq = html.unescape(arq).strip()
                    if arq and arq != "→" and "&rarr;" not in arq and d not in [x[0] for x in decks]:
                        decks.append((d, arq))
                puestos = {d: i + 1 for i, (d, _) in enumerate(decks)}
                con.execute("INSERT OR REPLACE INTO eventos VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (ev_id, "mtgtop8", fmt, nombre_ev, fecha, int(jug.group(1)) if jug else None, url_ev))
                for d, arq in decks[:DECKS_POR_EVENTO_T8]:
                    deck_id = f"t8:{d}"
                    if ya_esta(con, deck_id):
                        continue
                    lista = pedir(f"https://www.mtgtop8.com/mtgo?d={d}", pausa=PAUSA_T8).content.decode("latin-1")
                    cartas = leer_lista(lista, nombres)
                    cmd = None
                    if fmt in COMANDANTES:            # en cEDH MTGTop8 pone el comandante en el sideboard
                        side = [c for c in cartas if c[2] == "side"]
                        if 1 <= len(side) <= 2:
                            cartas = [c for c in cartas if c[2] != "side"] + [(n, q, "cmd") for n, q, _ in side]
                        cmd = " / ".join(sorted(n for n, _, z in cartas if z == "cmd")) or arq
                    if guardar_deck(con, {"id": deck_id, "evento": ev_id, "fuente": "mtgtop8", "formato": fmt, "fecha": fecha,
                                          "puesto": puestos.get(d), "arq_fuente": arq, "comandante": cmd,
                                          "url": f"https://www.mtgtop8.com/event?e={ev}&d={d}&f={cod['t8']}"}, cartas):
                        n_fmt += 1
                con.commit()
            anotar_fuente(con, "mtgtop8", fmt, True, n_fmt)
        except Exception as e:  # noqa: BLE001
            anotar_fuente(con, "mtgtop8", fmt, False, error=e)
            print(f"  MTGTop8 {fmt}: {e}")
        total += n_fmt
        print(f"MTGTop8 {fmt}: {n_fmt} decks nuevos")
    return total


# ---------------------------------------------------------------- MTGGoldfish (% del meta y lista tipica)
def _semanal(con, clave):
    v = con.execute("SELECT valor FROM meta WHERE clave = ?", (clave,)).fetchone()
    return not v or v[0] < hace(DIAS_SEMANAL)


def goldfish(con, nombres, rapido=False, forzar=False):
    if not forzar and not _semanal(con, "goldfish_dia"):
        print("MTGGoldfish: ya se bajo esta semana")
        return
    algo = False
    for fmt, cod in FORMATOS.items():
        if not cod.get("gf"):
            continue
        try:
            r = pedir(f"https://www.mtggoldfish.com/metagame/{cod['gf']}/full", pausa=2)
            if bloqueado(r.text):
                raise RuntimeError("Cloudflare pidio verificacion (bloqueo)")
            arqs = []
            for m in re.finditer(r"<div class='archetype-tile' id='(\d+)'>(.*?)(?=<div class='archetype-tile' id=|$)", r.text, re.S):
                t = m.group(2)
                nom = re.search(r"deck-price-paper'>\s*<a href=\"(/archetype/[^\"#]+)#paper\">([^<]+)</a>", t)
                pct = re.search(r"archetype-tile-statistic-value'>\s*([\d.]+)%\s*<span class='archetype-tile-statistic-value-extra-data'>\s*\((\d+)\)", t)
                if not nom or not pct:
                    continue
                colores = "".join(dict.fromkeys(c.upper() for c in re.findall(r"ms ms-([wubrgc]) ms-cost", t)))
                claves = [html.unescape(x) for x in re.findall(r"<li>([^<]+)</li>", t)[:3]]
                arqs.append({"url": "https://www.mtggoldfish.com" + nom.group(1), "nombre": html.unescape(nom.group(2)).strip(),
                             "pct": float(pct.group(1)), "n": int(pct.group(2)), "colores": colores, "claves": claves})
            if not arqs:
                raise RuntimeError("la pagina del meta no trajo arquetipos (cambio?)")
            con.execute("DELETE FROM arquetipos WHERE formato = ? AND fuente = 'mtggoldfish'", (fmt,))
            for i, a in enumerate(arqs):
                lista = None
                if i < (3 if rapido else TOP_ARQUETIPOS_GF):
                    try:
                        p = pedir(a["url"], pausa=3).text
                        mm = re.search(r'id="deck_input_deck"[^>]*value="([^"]*)"', p)
                        if mm:
                            cartas = leer_lista(html.unescape(mm.group(1)), nombres)
                            lista = {"main": {n: q for n, q, z in cartas if z == "main"},
                                     "side": {n: q for n, q, z in cartas if z == "side"}}
                    except Exception as e:  # noqa: BLE001
                        print(f"  MTGGoldfish {a['nombre']}: {e}")
                con.execute("INSERT OR REPLACE INTO arquetipos VALUES (?, ?, 'mtggoldfish', ?, ?, ?, ?, ?, ?, ?)",
                            (fmt, a["nombre"], a["pct"], a["n"], a["url"], a["colores"], json.dumps(a["claves"], ensure_ascii=False),
                             json.dumps(lista, ensure_ascii=False) if lista else None, hoy()))
            con.commit()
            anotar_fuente(con, "mtggoldfish", fmt, True, len(arqs))
            algo = True
            print(f"MTGGoldfish {fmt}: {len(arqs)} arquetipos")
        except Exception as e:  # noqa: BLE001
            anotar_fuente(con, "mtggoldfish", fmt, False, error=e)
            print(f"  MTGGoldfish {fmt}: {e}")
    if algo:
        con.execute("INSERT OR REPLACE INTO meta VALUES ('goldfish_dia', ?)", (hoy(),))
        con.commit()


# ---------------------------------------------------------------- TopDeck.gg (API con llave)
def _lista_topdeck(texto, nombres):
    zona, out = "main", []
    for linea in str(texto or "").replace("\r", "").split("\n"):
        l = linea.strip()
        m = re.match(r"^~~(.+?)~~$", l)
        if m:
            t = m.group(1).lower()
            zona = "cmd" if "commander" in t else "side" if "side" in t else "main"
            continue
        m = re.match(r"^(\d+)\s+(.+)$", l)
        if m:
            out.append((nombres(m.group(2)), int(m.group(1)), zona))
    return out


def topdeck(con, nombres, rapido=False):
    llave = os.environ.get("TOPDECK_KEY", "").strip()
    if not llave:
        print("TopDeck: sin llave (secreto TOPDECK_KEY): se salta")
        for fmt, cod in FORMATOS.items():
            if cod.get("td"):
                anotar_fuente(con, "topdeck", fmt, False, error="sin llave")
        return
    for fmt, cod in FORMATOS.items():
        if not cod.get("td"):
            continue
        n = 0
        try:
            r = pedir("https://topdeck.gg/api/v2/tournaments", metodo="POST", intentos=3,
                      headers={"Authorization": llave, "Content-Type": "application/json"},
                      json={"game": "Magic: The Gathering", "format": cod["td"], "last": DIAS_META, "participantMin": 16,
                            "columns": ["name", "decklist", "wins", "draws", "losses"]})
            torneos = r.json()
            for t in torneos[: (2 if rapido else 200)]:
                fecha = datetime.fromtimestamp(int(t.get("startDate") or 0), timezone.utc).strftime("%Y-%m-%d")
                ev_id = f"td:{t.get('TID')}"
                url = f"https://topdeck.gg/bracket/{t.get('TID')}"
                con.execute("INSERT OR REPLACE INTO eventos VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (ev_id, "topdeck", fmt, t.get("tournamentName"), fecha, len(t.get("standings") or []), url))
                for i, s in enumerate((t.get("standings") or [])[:16]):
                    lista = s.get("decklist") or ""
                    if not lista or lista.startswith("http"):
                        continue
                    cartas = _lista_topdeck(lista, nombres)
                    cmd = " / ".join(sorted(nm for nm, _, z in cartas if z == "cmd")) or None
                    if guardar_deck(con, {"id": f"td:{t.get('TID')}:{i + 1}", "evento": ev_id, "fuente": "topdeck", "formato": fmt,
                                          "fecha": fecha, "jugador": s.get("name"), "puesto": i + 1,
                                          "resultado": f"{s.get('wins', 0)}-{s.get('losses', 0)}-{s.get('draws', 0)}",
                                          "comandante": cmd, "url": url}, cartas):
                        n += 1
            con.commit()
            anotar_fuente(con, "topdeck", fmt, True, n)
            print(f"TopDeck {fmt}: {n} decks")
        except Exception as e:  # noqa: BLE001
            anotar_fuente(con, "topdeck", fmt, False, error=e)
            print(f"  TopDeck {fmt}: {e}")
        time.sleep(13)                       # la API deja 5 peticiones de estas por minuto


# ---------------------------------------------------------------- EDHREC (Commander casual)
def edhrec(con, nombres, rapido=False, forzar=False):
    if not forzar and not _semanal(con, "edhrec_dia"):
        print("EDHREC: ya se bajo esta semana")
        return
    try:
        sem = pedir("https://json.edhrec.com/pages/commanders/week.json", pausa=1).json()
        lista = sem["container"]["json_dict"]["cardlists"][0]["cardviews"]
        top = lista[: (3 if rapido else TOP_EDHREC)]
        con.execute("DELETE FROM edhrec_comandantes")
        for i, c in enumerate(lista):
            con.execute("INSERT OR REPLACE INTO edhrec_comandantes VALUES (?, ?, ?, ?, ?, ?)",
                        (nombres(c["name"]), c.get("sanitized") or c.get("slug"), c.get("rank") or i + 1, c.get("num_decks"),
                         c.get("trend_zscore"), hoy()))
        for c in top:
            slug, nombre = c.get("sanitized") or c.get("slug"), nombres(c["name"])
            try:
                p = pedir(f"https://json.edhrec.com/pages/commanders/{slug}.json", pausa=1.5).json()
                con.execute("DELETE FROM edhrec_recomendadas WHERE comandante = ?", (nombre,))
                for cl in p["container"]["json_dict"]["cardlists"]:
                    for v in cl.get("cardviews") or []:
                        con.execute("INSERT OR REPLACE INTO edhrec_recomendadas VALUES (?, ?, ?, ?, ?, ?)",
                                    (nombre, nombres(v["name"]), cl.get("header"), v.get("synergy"), v.get("num_decks"),
                                     v.get("potential_decks")))
                prom = pedir(f"https://json.edhrec.com/pages/average-decks/{slug}.json", pausa=1.5).json()
                con.execute("DELETE FROM edhrec_promedio WHERE comandante = ?", (nombre,))
                for tipo, cartas in ((prom.get("deck") or {}).get("cards") or {}).items():
                    for nm, q in cartas:
                        con.execute("INSERT OR REPLACE INTO edhrec_promedio VALUES (?, ?, ?, ?)", (nombre, nombres(nm), q, tipo))
                con.commit()
            except Exception as e:  # noqa: BLE001
                print(f"  EDHREC {nombre}: {e}")
        con.execute("INSERT OR REPLACE INTO meta VALUES ('edhrec_dia', ?)", (hoy(),))
        con.commit()
        anotar_fuente(con, "edhrec", "commander", True, len(top))
        print(f"EDHREC: {len(lista)} comandantes de la semana · {len(top)} con recomendadas y deck promedio")
    except Exception as e:  # noqa: BLE001
        anotar_fuente(con, "edhrec", "commander", False, error=e)
        print(f"  EDHREC: {e}")


# ---------------------------------------------------------------- arquetipos
def _vector(cartas):
    """{nombre: copias} sin tierras basicas."""
    return {n: q for n, q in cartas.items() if n.lower() not in BASICAS}


def _coseno(a, b):
    comun = set(a) & set(b)
    if not comun:
        return 0.0
    num = sum(a[k] * b[k] for k in comun)
    return num / (math.sqrt(sum(v * v for v in a.values())) * math.sqrt(sum(v * v for v in b.values())))


def clasificar(con):
    """Pone 'arquetipo' a cada deck: el comandante en cEDH; en los demas, el arquetipo de MTGGoldfish mas parecido (o el
    de MTGTop8 si el deck es de alla y nada de MTGGoldfish se le parece)."""
    principal = defaultdict(dict)
    for d, n, q in con.execute("SELECT deck, nombre, cantidad FROM cartas WHERE zona = 'main'"):
        principal[d][n] = q
    decks = con.execute("SELECT id, formato, fuente, arq_fuente, comandante FROM decks").fetchall()
    refs = defaultdict(list)             # formato -> [(nombre, vector)]
    for fmt, nombre, lista in con.execute("SELECT formato, nombre, lista FROM arquetipos WHERE lista IS NOT NULL"):
        refs[fmt].append((nombre, _vector(json.loads(lista).get("main") or {})))
    # los decks de MTGTop8 (traen su arquetipo) tambien son referencia, con el nombre de MTGGoldfish si se parecen
    t8 = defaultdict(list)
    cambios = []
    for d, fmt, fuente, arq, cmd in decks:
        if fmt in COMANDANTES:
            cambios.append((cmd or arq or "?", None, d))
            continue
        v = _vector(principal.get(d, {}))
        mejor, sim = None, 0.0
        for nombre, rv in refs.get(fmt, []):
            s = _coseno(v, rv)
            if s > sim:
                mejor, sim = nombre, s
        if fuente == "mtgtop8" and arq:
            t8[fmt].append((mejor if sim >= 0.5 else arq, v))
        cambios.append((mejor if sim >= PARECIDO_MIN else None, sim, d))
    con.executemany("UPDATE decks SET arquetipo = ?, parecido = ? WHERE id = ?", cambios)
    # segunda vuelta: lo que no se parecio a ninguna lista de MTGGoldfish se compara con los decks de MTGTop8
    sin = con.execute("SELECT id, formato, arq_fuente FROM decks WHERE arquetipo IS NULL AND formato NOT IN ('cedh', 'commander')").fetchall()
    otra = []
    for d, fmt, arq in sin:
        v = _vector(principal.get(d, {}))
        mejor, sim = None, 0.0
        for nombre, rv in t8.get(fmt, []):
            s = _coseno(v, rv)
            if s > sim:
                mejor, sim = nombre, s
        otra.append((mejor if sim >= 0.45 else (arq or "Otros"), sim, d))
    con.executemany("UPDATE decks SET arquetipo = ?, parecido = ? WHERE id = ?", otra)
    con.commit()
    n = Counter(r[0] for r in con.execute("SELECT arquetipo FROM decks"))
    print(f"Arquetipos: {len(decks)} decks · {len(n)} arquetipos distintos · sin arquetipo: {n.get('Otros', 0)}")


def limpiar(con):
    viejos = [r[0] for r in con.execute("SELECT id FROM decks WHERE fecha < ?", (hace(DIAS_GUARDAR),))]
    for i in range(0, len(viejos), 500):
        parte = viejos[i:i + 500]
        q = ",".join("?" * len(parte))
        con.execute(f"DELETE FROM cartas WHERE deck IN ({q})", parte)
        con.execute(f"DELETE FROM decks WHERE id IN ({q})", parte)
    con.execute("DELETE FROM eventos WHERE fecha < ?", (hace(DIAS_GUARDAR),))
    con.execute("INSERT OR IGNORE INTO dias VALUES (?)", (hoy(),))
    con.execute("DELETE FROM dias WHERE dia < ?", (hace(DIAS_GUARDAR),))
    con.commit()
    if viejos:
        print(f"Se borraron {len(viejos)} decks de hace mas de {DIAS_GUARDAR} dias")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="meta.py")
    ap.add_argument("accion", nargs="?", default="todo", choices=["todo", "clasificar"])
    ap.add_argument("--solo", choices=["mtgo", "mtgtop8", "goldfish", "topdeck", "edhrec"])
    ap.add_argument("--rapido", action="store_true")
    ap.add_argument("--forzar", action="store_true", help="MTGGoldfish y EDHREC aunque ya se hayan bajado esta semana")
    a = ap.parse_args(argv)
    con = abrir()
    try:
        if a.accion == "todo":
            nombres = Nombres()
            print(f"Nombres de cartas: {len(nombres.alias):,} (oraculo)" if nombres.alias else "AVISO: sin oraculo; los nombres quedan como vienen")
            pasos = {"goldfish": lambda: goldfish(con, nombres, a.rapido, a.forzar),
                     "mtgo": lambda: mtgo(con, nombres, a.rapido),
                     "mtgtop8": lambda: mtgtop8(con, nombres, a.rapido),
                     "topdeck": lambda: topdeck(con, nombres, a.rapido),
                     "edhrec": lambda: edhrec(con, nombres, a.rapido, a.forzar)}
            for nombre, paso in pasos.items():
                if a.solo and nombre != a.solo:
                    continue
                try:
                    paso()
                except Exception as e:  # noqa: BLE001  (una fuente caida no tumba a las demas)
                    print(f"ERROR {nombre}: {type(e).__name__}: {e}")
                    for fmt in FORMATOS:
                        anotar_fuente(con, nombre, fmt, False, error=e)
        clasificar(con)
        limpiar(con)
        n = con.execute("SELECT formato, COUNT(*) FROM decks WHERE fecha >= ? GROUP BY formato", (hace(DIAS_META),)).fetchall()
        print("Decks en la ventana del meta:", dict(n))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
