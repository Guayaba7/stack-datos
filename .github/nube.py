"""
Datos de Stack en la nube (GitHub Releases).

Empaqueta lo que Stack necesita (historial de precios, Hareruya, StarCityGames, Card Kingdom) en archivos .gz con un
manifest.json, y los restaura. Lo usan el workflow de GitHub (.github/workflows/diario.yml) y el dueno en su PC
para la primera subida. Solo usa la biblioteca estandar de Python.

No es un programa de precios: es la herramienta del workflow (vive en .github/, junto a workflows/diario.yml).
La primera subida desde tu PC la hace Stack (CONFIGURACION > NUBE > Preparar primera subida), con el mismo formato.

Uso:
    python .github/nube.py publicar SALIDA --tag T --capa DIR [--capa DIR ...]
    python .github/nube.py restaurar DIR         (descomprime en data/ lo que haya en DIR con su manifest.json)
    python .github/nube.py preparar              (igual que el boton de Stack: deja en subir_a_github/ tus datos)
    python .github/nube.py web SALIDA [--bajar] [--huellas CACHE]
                                                 (archivos de la pagina web; con --huellas tambien el indice del escaner)
"""
import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

for _flujo in (sys.stdout, sys.stderr):              # Windows: que un acento nunca tumbe el programa
    try:
        _flujo.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError, OSError):
        pass

ESQUEMA = 1
RAIZ = Path(os.environ.get("STACK_RAIZ") or Path(__file__).resolve().parent.parent)
# asset en el release -> (ruta dentro de data/, consulta que da los dias que trae)
ARCHIVOS = {
    "historial.sqlite.gz": ("precios/historial.sqlite", "SELECT fecha FROM dias"),
    "hareruya_historial.sqlite.gz": ("tiendas/hareruya/historial_hareruya.sqlite", "SELECT dia FROM dias"),
    "scg.sqlite.gz": ("tiendas/starcitygames/scg.sqlite", "SELECT dia FROM dias"),
    "ck.sqlite.gz": ("tiendas/cardkingdom/ck.sqlite", "SELECT dia FROM dias"),
}
ASSET_EMPAREJADO = "hareruya_emparejado.sqlite.gz"     # el emparejado mas reciente (precios de Hareruya de hoy)
CARPETA_EMPAREJADO = "tiendas/hareruya/emparejado"
PATRON_DIA = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def ahora_utc():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def abrir_ro(ruta):
    return sqlite3.connect(f"{Path(ruta).resolve().as_uri()}?mode=ro", uri=True)


def dias_de(ruta, consulta):
    try:
        con = abrir_ro(ruta)
        try:
            return sorted({str(r[0]) for r in con.execute(consulta) if r[0]})
        finally:
            con.close()
    except sqlite3.Error:
        return []


def meta_de(ruta):
    try:
        con = abrir_ro(ruta)
        try:
            return dict(con.execute("SELECT key, value FROM meta"))
        finally:
            con.close()
    except sqlite3.Error:
        return {}


def dia_de_emparejado(meta):
    """Mismo criterio que hareruya.py: el dia en que EMPEZO la descarga (va en el nombre del seco)."""
    m = re.search(r"_(\d{4})(\d{2})(\d{2})_\d{6}", Path(str(meta.get("seco") or "")).name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    d = meta.get("seco_dia") or ""
    return d if PATRON_DIA.match(d) else ""


def productos_de(meta):
    try:
        return int(meta.get("productos") or 0)
    except (TypeError, ValueError):
        return 0


def emparejado_mas_reciente(carpetas):
    """(ruta, dia) del emparejado COMPLETO mas nuevo que haya en cualquiera de las carpetas. Uno con muy pocos productos
    (Hareruya bloqueo la descarga o se corto) no reemplaza al bueno anterior: se publica el anterior."""
    candidatos = []
    for c in carpetas:
        if not Path(c).is_dir():
            continue
        for ruta in Path(c).glob("emparejado_*.sqlite"):
            m = meta_de(ruta)
            dia = dia_de_emparejado(m)
            if m.get("completo") != "1" or not dia or productos_de(m) <= 0:
                continue
            candidatos.append(((dia, ruta.stat().st_mtime), ruta, dia, productos_de(m)))
    if not candidatos:
        return None, None
    tope = max(c[3] for c in candidatos)
    buenos = [c for c in candidatos if c[3] >= 0.3 * tope]          # menos del 30% del mejor: descarga a medias
    for c in sorted(candidatos, key=lambda c: c[0], reverse=True):
        if c not in buenos:
            print(f"  (se ignora {c[1].name}: solo {c[3]:,} productos contra {tope:,})")
            break
    _, ruta, dia, _ = max(buenos, key=lambda c: c[0])
    return ruta, dia


def copia_consistente(origen, destino):
    """Copia de un sqlite aunque este abierto o con WAL (API de respaldo de sqlite)."""
    src = abrir_ro(origen)
    try:
        dst = sqlite3.connect(destino)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def comprimir(origen, destino):
    """sqlite -> .gz; devuelve (bytes del sqlite, bytes del gz, sha256 del gz)."""
    with tempfile.TemporaryDirectory() as tmp:
        copia = Path(tmp) / "copia.sqlite"
        copia_consistente(origen, copia)
        crudo = copia.stat().st_size
        with open(copia, "rb") as fi, gzip.open(destino, "wb", compresslevel=6) as fo:
            shutil.copyfileobj(fi, fo, 1 << 20)
    h = hashlib.sha256()
    with open(destino, "rb") as f:
        for bloque in iter(lambda: f.read(1 << 20), b""):
            h.update(bloque)
    return crudo, Path(destino).stat().st_size, h.hexdigest()


def publicar(salida, tag, capas):
    """Junta lo mas nuevo de las capas (la ultima gana) y deja en SALIDA los .gz + manifest.json."""
    salida = Path(salida)
    salida.mkdir(parents=True, exist_ok=True)
    datas = [Path(c) / "data" for c in capas]
    man = {"esquema": ESQUEMA, "tag": tag, "generado_utc": ahora_utc(), "archivos": []}
    for asset, (ruta, consulta) in ARCHIVOS.items():
        # de todas las capas, la que trae MAS (dia mas nuevo, luego mas dias); en empate gana la ultima. Asi una tienda que
        # fallo (por ejemplo, un historial que quedo vacio) nunca tapa lo bueno que ya habia.
        opciones = [(dias_de(d / ruta, consulta), k, d / ruta) for k, d in enumerate(datas) if (d / ruta).is_file()]
        if not opciones:
            print(f"  (sin {ruta}: no se publica)")
            continue
        dias, _, origen = max(opciones, key=lambda o: ((o[0][-1] if o[0] else ""), len(o[0]), o[1]))
        crudo, comp, sha = comprimir(origen, salida / asset)
        man["archivos"].append({"asset": asset, "ruta": ruta, "dias": dias, "dia": dias[-1] if dias else None,
                                "bytes": crudo, "gz_bytes": comp, "sha256": sha})
        print(f"  {asset:32} {comp / 1e6:8.1f} MB  dias: {len(dias)} ({dias[-1] if dias else '-'})")
    ruta_emp, dia_emp = emparejado_mas_reciente([d / CARPETA_EMPAREJADO for d in datas])
    if ruta_emp is not None:
        crudo, comp, sha = comprimir(ruta_emp, salida / ASSET_EMPAREJADO)
        man["archivos"].append({"asset": ASSET_EMPAREJADO, "ruta": f"{CARPETA_EMPAREJADO}/{ruta_emp.name}", "dias": [dia_emp],
                                "dia": dia_emp, "bytes": crudo, "gz_bytes": comp, "sha256": sha})
        print(f"  {ASSET_EMPAREJADO:32} {comp / 1e6:8.1f} MB  dia: {dia_emp}")
    dias = [a["dia"] for a in man["archivos"] if a.get("dia")]
    man["dia"] = max(dias) if dias else None
    (salida / "manifest.json").write_text(json.dumps(man, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Listo: {len(man['archivos'])} archivos en {salida} (tag {tag}).")
    return man


def _instalar(gz, a):
    destino = RAIZ / "data" / a["ruta"]
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_name(destino.name + ".part")
    with gzip.open(gz, "rb") as fi, open(tmp, "wb") as fo:
        shutil.copyfileobj(fi, fo, 1 << 20)
    os.replace(tmp, destino)


def _de_releases_anteriores(faltan, ya_tag):
    """Si al ultimo release le falta algun archivo (una tienda que fallo), se toma del release anterior que si lo tenga.
    Usa GitHub CLI (en el workflow ya esta). Devuelve cuantos se recuperaron."""
    import subprocess
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not faltan or not repo or not shutil.which("gh"):
        return 0
    r = subprocess.run(["gh", "release", "list", "--repo", repo, "--limit", "30", "--json", "tagName,createdAt"],
                       capture_output=True, text=True)
    try:
        tags = [x["tagName"] for x in sorted(json.loads(r.stdout or "[]"), key=lambda x: x["createdAt"], reverse=True)
                if x["tagName"].startswith("datos-") and x["tagName"] != ya_tag]
    except ValueError:
        return 0
    n = 0
    for tag in tags:
        if not faltan:
            break
        with tempfile.TemporaryDirectory() as tmp:
            if subprocess.run(["gh", "release", "download", tag, "--repo", repo, "--dir", tmp, "--pattern", "manifest.json"],
                              capture_output=True).returncode != 0:
                continue
            man = json.loads((Path(tmp) / "manifest.json").read_text(encoding="utf-8"))
            for a in [x for x in man.get("archivos", []) if x["asset"] in faltan and (x.get("dias") or [])]:
                if subprocess.run(["gh", "release", "download", tag, "--repo", repo, "--dir", tmp, "--pattern", a["asset"]],
                                  capture_output=True).returncode != 0:
                    continue
                _instalar(Path(tmp) / a["asset"], a)
                faltan.discard(a["asset"])
                n += 1
                print(f"  {a['asset']} -> data/{a['ruta']} ({a.get('dia') or '-'}) [del release anterior {tag}]")
    for f in sorted(faltan):
        print(f"  (ningun release reciente tiene {f})")
    return n


def restaurar(carpeta):
    """Descomprime en data/ los archivos de un release ya descargado en CARPETA (con su manifest.json). Lo que le falte
    (o venga vacio) se toma del release anterior que lo tenga: una tienda que falla un dia no se pierde."""
    carpeta = Path(carpeta)
    ruta_man = carpeta / "manifest.json"
    man = json.loads(ruta_man.read_text(encoding="utf-8")) if ruta_man.is_file() else {}
    if not man:
        print(f"No hay manifest.json en {carpeta}: se buscan los datos en los releases anteriores.")
    n, tienen = 0, set()
    for a in man.get("archivos", []):
        gz = carpeta / a["asset"]
        if not gz.is_file():
            print(f"  falta {a['asset']}")
            continue
        if not (a.get("dias") or []):                 # vacio (por ejemplo, un historial que quedo sin dias): se busca otro
            print(f"  {a['asset']} viene vacio")
            continue
        _instalar(gz, a)
        tienen.add(a["asset"])
        n += 1
        print(f"  {a['asset']} -> data/{a['ruta']} ({a.get('dia') or '-'})")
    print(f"Restaurados {n} archivos del release {man.get('tag')}.")
    n += _de_releases_anteriores((set(ARCHIVOS) | {ASSET_EMPAREJADO}) - tienen, man.get("tag"))
    return n


# ------------------------------------------------------------------------------------------------------------------
# Pagina web (fase 1): archivos JSON ligeros que la pagina lee. Todo sale ya calculado; la pagina solo busca y muestra.
#   SALIDA/manifest.json            <- lo unico que cambia cada dia
#   SALIDA/AAAA-MM-DD/buscar.json   <- indice de busqueda (nombres + alias en espanol -> impresiones)
#   SALIDA/AAAA-MM-DD/p/xx.json     <- precios por impresion, 256 trozos (xx = 2 primeras letras del id de Scryfall)
#   SALIDA/AAAA-MM-DD/movers.json   <- suben / bajan / nuevas del dia
# ------------------------------------------------------------------------------------------------------------------
ESQUEMA_WEB = 1
DIAS_WEB = 2                                   # carpetas de dia que se quedan en SALIDA (hoy + respaldo)
MOVERS_MAX = 20
MOVERS_MIN_DIF_USD = 0.25                      # un cambio de centavos no es un "mover"
UMBRAL_WEB_USD = 2                             # el catalogo ya viene filtrado a >= $2 (solo se anota)
LETRA_ACABADO = {0: "n", 1: "f", 2: "e", "nonfoil": "n", "foil": "f", "etched": "e"}
REPO_DATOS = os.environ.get("GITHUB_REPOSITORY") or "Guayaba7/stack-datos"
URLS_CAMBIO = ("https://api.frankfurter.dev/v1/latest?base=USD&symbols=MXN,JPY",
               "https://api.frankfurter.app/latest?from=USD&to=MXN,JPY")


def normalizar(texto):
    """Igual que la pagina (js/buscar.js): sin acentos, minusculas, solo letras y numeros separados por un espacio."""
    import unicodedata
    t = unicodedata.normalize("NFD", str(texto or ""))
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
    t = t.replace("æ", "ae").replace("Æ", "ae").replace("œ", "oe").replace("Œ", "oe").lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", t).split())


def _json_compacto(ruta, obj):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    datos = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ruta.write_bytes(datos)
    return len(datos), hashlib.sha256(datos).hexdigest()


def _cambio(salida, usd_mxn=None, jpy_usd=None):
    """Tipos de cambio del dia: los que se pasen a mano > internet (Frankfurter, Banco Central Europeo) > los del
    manifest anterior > config.json de Stack. JPY -> USD -> MXN, igual que Stack escritorio."""
    import urllib.request
    tc = {"usd_mxn": usd_mxn, "jpy_usd": jpy_usd, "fuente": "manual" if (usd_mxn and jpy_usd) else None, "dia": None}
    if not tc["fuente"]:
        for url in URLS_CAMBIO:
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "stack-datos"}), timeout=20) as r:
                    d = json.loads(r.read().decode("utf-8"))
                tc["usd_mxn"] = tc["usd_mxn"] or float(d["rates"]["MXN"])
                tc["jpy_usd"] = tc["jpy_usd"] or round(1 / float(d["rates"]["JPY"]), 8)
                tc["fuente"], tc["dia"] = "frankfurter", d.get("date")
                break
            except Exception as e:                                  # sin red o servicio caido: se sigue con el respaldo
                print(f"  (tipo de cambio: {url.split('/')[2]} no respondio: {e.__class__.__name__})")
    if not (tc["usd_mxn"] and tc["jpy_usd"]):
        try:
            viejo = json.loads((Path(salida) / "manifest.json").read_text(encoding="utf-8")).get("tc") or {}
            tc["usd_mxn"] = tc["usd_mxn"] or viejo.get("usd_mxn")
            tc["jpy_usd"] = tc["jpy_usd"] or viejo.get("jpy_usd")
            tc["fuente"], tc["dia"] = "anterior", viejo.get("dia")
        except (OSError, ValueError):
            pass
    if not (tc["usd_mxn"] and tc["jpy_usd"]):
        try:
            cfg = json.loads((RAIZ / "data" / "app" / "config.json").read_text(encoding="utf-8"))
            tc["usd_mxn"] = tc["usd_mxn"] or cfg.get("usd_mxn")
            tc["jpy_usd"] = tc["jpy_usd"] or cfg.get("jpy_usd")
            tc["fuente"] = "config de Stack"
        except (OSError, ValueError):
            pass
    return tc


def _descargar(url, destino, total=0, intentos=8):
    """Descarga con reintentos: si la conexion se corta, sigue desde donde se quedo (Range) en vez de empezar de cero."""
    import time
    import urllib.error
    import urllib.request
    destino = Path(destino)
    for intento in range(1, intentos + 1):
        ya = destino.stat().st_size if destino.exists() else 0
        if total and ya >= total:
            return
        pedido = urllib.request.Request(url, headers={"User-Agent": "stack-datos"})
        if ya:
            pedido.add_header("Range", f"bytes={ya}-")
        try:
            with urllib.request.urlopen(pedido, timeout=120) as r:
                modo = "ab" if (ya and r.status == 206) else "wb"      # 200 = el servidor no continuo: desde cero
                with open(destino, modo) as f:
                    shutil.copyfileobj(r, f, 1 << 20)
            if not total or destino.stat().st_size >= total:
                return
        except (OSError, urllib.error.URLError) as e:                 # conexion cortada, sin red un momento, etc.
            hecho = destino.stat().st_size if destino.exists() else 0
            print(f"    se corto ({e.__class__.__name__}) en {hecho / 1e6:.1f} MB; reintento {intento}/{intentos}...")
            time.sleep(min(30, 3 * intento))
    raise RuntimeError(f"No se pudo bajar {url} despues de {intentos} intentos. Vuelve a correrlo: sigue donde quedo.")


def _bajar_release(cache):
    """Baja y descomprime el ultimo release publico de stack-datos en CACHE (para probar en la PC sin tener los datos).
    Lo ya bajado del mismo release no se vuelve a bajar; los releases viejos se borran de la cache."""
    import urllib.request
    base = f"https://github.com/{REPO_DATOS}/releases/latest/download/"
    with urllib.request.urlopen(base + "manifest.json", timeout=60) as r:
        man = json.loads(r.read().decode("utf-8"))
    tag = man.get("tag") or "sin-tag"
    destino = Path(cache) / tag
    destino.mkdir(parents=True, exist_ok=True)
    for viejo in Path(cache).iterdir():
        if viejo.is_dir() and viejo.name != tag:
            shutil.rmtree(viejo, ignore_errors=True)
    print(f"Release {tag} de {REPO_DATOS} (se guarda en {destino})")
    rutas = {}
    for a in man.get("archivos", []):
        if a["asset"] not in ARCHIVOS:                   # la web no usa el emparejado crudo de Hareruya
            continue
        sqlite_ = destino / a["asset"][:-3]
        listo = destino / (a["asset"] + ".ok")
        if sqlite_.is_file() and listo.is_file() and listo.read_text().strip() == a.get("sha256", ""):
            print(f"  {a['asset']:32} ya estaba")
            rutas[sqlite_.name] = sqlite_
            continue
        gz = destino / a["asset"]
        print(f"  {a['asset']:32} {a.get('gz_bytes', 0) / 1e6:6.1f} MB")
        _descargar(base + a["asset"], gz, a.get("gz_bytes") or 0)
        h = hashlib.sha256()
        with open(gz, "rb") as f:
            for bloque in iter(lambda: f.read(1 << 20), b""):
                h.update(bloque)
        if a.get("sha256") and h.hexdigest() != a["sha256"]:
            gz.unlink()
            raise RuntimeError(f"{a['asset']} llego danado (sha256 no coincide). Vuelve a correrlo.")
        with gzip.open(gz, "rb") as fi, open(sqlite_, "wb") as fo:
            shutil.copyfileobj(fi, fo, 1 << 20)
        gz.unlink()
        listo.write_text(a.get("sha256", ""))
        rutas[sqlite_.name] = sqlite_
    return {
        "historial": rutas.get("historial.sqlite"),
        "har": rutas.get("hareruya_historial.sqlite"),
        "scg": rutas.get("scg.sqlite"),
        "ck": rutas.get("ck.sqlite"),
    }


def _rutas_locales():
    d = RAIZ / "data"
    return {
        "historial": d / "precios/historial.sqlite",
        "har": d / "tiendas/hareruya/historial_hareruya.sqlite",
        "scg": d / "tiendas/starcitygames/scg.sqlite",
        "ck": d / "tiendas/cardkingdom/ck.sqlite",
    }


def _pct(hoy, antes):
    if not hoy or not antes:
        return None
    return round((hoy - antes) / antes * 100, 1)


# ------------------------------------------------------------------------------------------------------------------
# Escaner (pagina web): huella de 128 numeros por impresion, calculada con el mismo modelo que corre en el telefono.
# Se guarda en una cache (CARPETA/huellas.npz) para que cada dia solo se calculen las impresiones nuevas.
# Librerias extra (solo para esto): numpy, onnxruntime, opencv-python-headless.
# ------------------------------------------------------------------------------------------------------------------
MODELO_VISION = "stack-vision-1.onnx"                  # vive junto a nube.py (.github/); el nombre cambia si cambia el modelo
VISION_CW, VISION_CH = 244, 340
VISION_ZONA = (0.05, 0.95, 0.04, 0.96)                  # x0, x1, y0, y1: igual que js/escaner.js
VISION_HILOS = 8


def _vision_prep(np, cv2, imgs):
    mean = np.float32([0.485, 0.456, 0.406]); std = np.float32([0.229, 0.224, 0.225])
    x0, x1, y0, y1 = VISION_ZONA
    lote = []
    for im in imgs:
        im = cv2.resize(im, (VISION_CW, VISION_CH), interpolation=cv2.INTER_AREA)
        im = im[int(y0 * VISION_CH):int(y1 * VISION_CH), int(x0 * VISION_CW):int(x1 * VISION_CW)]
        im = cv2.resize(cv2.cvtColor(im, cv2.COLOR_BGR2RGB), (224, 224), interpolation=cv2.INTER_AREA).astype(np.float32) / 255
        lote.append((im - mean) / std)
    return np.stack(lote).transpose(0, 3, 1, 2).astype(np.float32)


def _vision(filas, carpeta_dia, salida, cache):
    import concurrent.futures as cf
    import time
    import urllib.request
    import numpy as np
    import cv2
    import onnxruntime as ort
    modelo = Path(__file__).resolve().parent / MODELO_VISION
    if not modelo.is_file():
        raise FileNotFoundError(f"falta {modelo.name} junto a nube.py")
    # impresiones (sin importar acabado) con su imagen 'small' de Scryfall; la URL trae la version de la imagen
    urls = {}
    for f in filas.values():
        if f.get("img"):
            urls[f["sid"]] = f["img"].replace("/normal/", "/small/")
    cache.mkdir(parents=True, exist_ok=True)
    ruta_cache = cache / "huellas.npz"
    vec, version = {}, {}
    if ruta_cache.is_file():
        z = np.load(ruta_cache, allow_pickle=False)
        if str(z["modelo"]) == MODELO_VISION:
            for sid, u, v in zip(z["ids"], z["urls"], z["vec"]):
                vec[str(sid)], version[str(sid)] = v, str(u)
    faltan = [sid for sid, u in urls.items() if version.get(sid) != u]
    print(f"  Escaner: {len(urls):,} impresiones · {len(urls) - len(faltan):,} ya calculadas · {len(faltan):,} nuevas", flush=True)
    if faltan:
        sesion = ort.InferenceSession(str(modelo), providers=["CPUExecutionProvider"])
        entrada = sesion.get_inputs()[0].name

        def bajar(sid):
            for intento in range(4):
                try:
                    pedido = urllib.request.Request(urls[sid], headers={"User-Agent": "stack-datos"})
                    with urllib.request.urlopen(pedido, timeout=30) as r:
                        im = cv2.imdecode(np.frombuffer(r.read(), np.uint8), cv2.IMREAD_COLOR)
                    if im is not None:
                        return sid, im
                except Exception:
                    time.sleep(2 + 3 * intento)
            return sid, None

        t0, hechas, fallas = time.time(), 0, 0
        with cf.ThreadPoolExecutor(VISION_HILOS) as ex:
            for i in range(0, len(faltan), 256):
                bloque = [x for x in ex.map(bajar, faltan[i:i + 256])]
                buenas = [(sid, im) for sid, im in bloque if im is not None]
                fallas += len(bloque) - len(buenas)
                for j in range(0, len(buenas), 32):
                    parte = buenas[j:j + 32]
                    out = sesion.run(None, {entrada: _vision_prep(np, cv2, [im for _, im in parte])})[0]
                    out /= np.linalg.norm(out, axis=1, keepdims=True)
                    for (sid, _), v in zip(parte, out):
                        vec[sid] = np.clip(np.round(v * 127), -127, 127).astype(np.int8)
                        version[sid] = urls[sid]
                hechas += len(buenas)
                if (i // 256) % 20 == 0:
                    print(f"    {hechas:,}/{len(faltan):,} ({time.time() - t0:.0f} s)", flush=True)
        print(f"  Escaner: {hechas:,} calculadas en {time.time() - t0:.0f} s, {fallas} imagenes no bajaron", flush=True)
        ids_c = sorted(vec)
        np.savez(ruta_cache, modelo=np.array(MODELO_VISION), ids=np.array(ids_c),
                 urls=np.array([version[k] for k in ids_c]), vec=np.stack([vec[k] for k in ids_c]))
    ids = sorted(sid for sid in urls if sid in vec)
    if not ids:
        return None
    m = np.stack([vec[sid] for sid in ids]).astype(np.int8)
    (carpeta_dia / "vision.bin").write_bytes(m.tobytes())
    _json_compacto(carpeta_dia / "vision.json", {"esquema": 1, "modelo": MODELO_VISION, "dims": int(m.shape[1]), "ids": ids})
    destino = salida / "modelo" / MODELO_VISION
    if not destino.is_file() or destino.stat().st_size != modelo.stat().st_size:
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(modelo, destino)
    print(f"  Escaner: indice de {len(ids):,} impresiones ({m.nbytes / 1e6:.1f} MB)", flush=True)
    return {"modelo": f"modelo/{MODELO_VISION}", "dims": int(m.shape[1]), "cartas": len(ids)}


def web(salida, rutas, usd_mxn=None, jpy_usd=None, huellas=None):
    """Arma los archivos de la pagina a partir de los sqlite del dia. Devuelve el manifest.
    huellas: carpeta de cache del escaner (si se pasa, se arma el indice visual vision.bin)."""
    from datetime import date, timedelta
    salida = Path(salida)
    rutas = {k: (Path(v) if v else None) for k, v in rutas.items()}
    if not rutas.get("historial") or not rutas["historial"].is_file():
        raise FileNotFoundError(f"No encontre el historial de precios: {rutas.get('historial')}")

    # --- 1) Catalogo y precio TCG (Scryfall / MTGJSON) -------------------------------------------------------------
    con = abrir_ro(rutas["historial"])
    dias = [r[0] for r in con.execute("SELECT dia FROM dias ORDER BY dia")]
    d_hoy = dias[-1]
    dia = f"{str(d_hoy)[:4]}-{str(d_hoy)[4:6]}-{str(d_hoy)[6:]}"
    d_1 = dias[-2] if len(dias) > 1 else None
    hace7 = int((date.fromisoformat(dia) - timedelta(days=7)).strftime("%Y%m%d"))
    d_7 = max([d for d in dias if d <= hace7], default=None)

    def precios_de(d):
        if d is None:
            return {}
        return {(n, a): c for n, a, c in con.execute("SELECT n, acabado, centavos FROM precios WHERE dia = ?", (d,))}

    hoy, ayer, semana = precios_de(d_hoy), precios_de(d_1), precios_de(d_7)
    cartas = {}
    for n, sid, nombre, sc, sn, num, rar, lang, uri, img in con.execute(
            "SELECT n, id, name, set_code, set_name, collector_number, rarity, lang, scryfall_uri, imagen FROM cartas"):
        cartas[n] = {"sid": sid, "nombre": nombre, "set": sc, "set_nombre": sn, "num": num, "rareza": rar,
                     "idioma": lang, "url_scryfall": (uri or "").split("?")[0] or None, "img": img}
    # nombres en otros idiomas (MTGJSON): espanol primero (nombre_es de la ficha), luego frances, aleman, italiano y
    # portugues (letras latinas: los lee el escaner y la busqueda) y japones aparte (sin normalizar: el escaner lo lee
    # con su propio lector de texto y la busqueda lo compara tal cual)
    alias_es, alias_latinos, alias_ja = {}, {}, {}
    try:
        for name, idioma, nombre in con.execute(
                "SELECT name, idioma, nombre FROM nombres_idioma WHERE idioma IN ('es', 'fr', 'de', 'it', 'pt', 'ja') "
                "ORDER BY name, CASE idioma WHEN 'es' THEN 0 ELSE 1 END, idioma, nombre"):
            if idioma == "es":
                alias_es.setdefault(name, []).append(nombre)
            if idioma == "ja":
                import unicodedata
                ja = "".join(unicodedata.normalize("NFKC", nombre).split())
                if ja:
                    alias_ja.setdefault(name, []).append(ja)
            else:
                alias_latinos.setdefault(name, []).append(nombre)
    except sqlite3.Error:
        pass
    try:
        nuevas_hoy = list(con.execute(
            "SELECT n, acabado, dia_entrada FROM entradas WHERE estado = 'confirmada' AND motivo = 'subio' "
            "AND dia_confirmada = ?", (d_hoy,)))
    except sqlite3.Error:
        nuevas_hoy = []
    con.close()

    filas = {}                                              # k -> fila completa de la ficha
    por_sid = {}                                            # sid -> [k, ...] (otros acabados)
    for (n, a), cent in hoy.items():
        c = cartas.get(n)
        if not c:
            continue
        k = f"{c['sid']}:{LETRA_ACABADO[a]}"
        usd = cent / 100
        fila = {"sid": c["sid"], "nombre": c["nombre"], "set": c["set"], "set_nombre": c["set_nombre"], "num": c["num"],
                "acab": LETRA_ACABADO[a], "idioma": c["idioma"], "rareza": c["rareza"], "img": c["img"],
                "url_scryfall": c["url_scryfall"],
                "tcg": {"usd": usd, "d1": _pct(cent, ayer.get((n, a))), "d7": _pct(cent, semana.get((n, a))),
                        "estado": "ok", "dia": dia}}
        es = alias_es.get(c["nombre"])
        if es:
            fila["nombre_es"] = es[0]
        filas[k] = fila
        por_sid.setdefault(c["sid"], []).append(k)
    for ks in por_sid.values():
        if len(ks) > 1:
            for k in ks:
                filas[k]["otras"] = sorted(x for x in ks if x != k)
    print(f"  TCG: {len(filas):,} impresiones del {dia}")

    fuentes = {"tcg": {"estado": "ok", "dia": dia, "filas": len(filas)}}

    def estado_fuente(nombre, dia_f, n_filas):
        if not dia_f or not n_filas:
            fuentes[nombre] = {"estado": "falta", "dia": dia_f, "filas": 0}
        else:
            fuentes[nombre] = {"estado": "ok" if dia_f >= dia else "vieja", "dia": dia_f, "filas": n_filas}

    # --- 2) Hareruya (EN y JP, NM). Se toma el dia mas reciente de cada impresion en la ultima semana ---------------
    n_har = {"en": 0, "ja": 0}
    dia_har = None
    if rutas.get("har") and rutas["har"].is_file():
        h = abrir_ro(rutas["har"])
        dia_har = h.execute("SELECT MAX(dia) FROM dias WHERE completo = 1").fetchone()[0]
        if dia_har:
            desde = (date.fromisoformat(dia_har) - timedelta(days=7)).isoformat()
            vistos = set()
            for d, sid, acab, idi, jpy, stock in h.execute(
                    "SELECT dia, scryfall_id, acabado, idioma, jpy, stock FROM precios "
                    "WHERE condicion = 'NM' AND idioma IN ('en', 'ja') AND dia >= ? ORDER BY dia DESC", (desde,)):
                k = f"{sid}:{LETRA_ACABADO.get(acab, 'n')}"
                clave = (k, idi)
                if clave in vistos or k not in filas or not jpy:
                    continue
                vistos.add(clave)
                campo = "har" if idi == "en" else "har_jp"
                filas[k][campo] = {"jpy": jpy, "cond": "NM", "stock": stock,
                                   "estado": "ok" if d == dia_har else "previo", "dia": d}
                n_har[idi] += 1
        h.close()
    estado_fuente("har", dia_har, n_har["en"])
    estado_fuente("har_jp", dia_har, n_har["ja"])
    print(f"  Hareruya: {n_har['en']:,} EN, {n_har['ja']:,} JP ({dia_har or 'sin datos'})")

    # --- 3) StarCityGames ------------------------------------------------------------------------------------------
    n_scg, dia_scg = 0, None
    if rutas.get("scg") and rutas["scg"].is_file():
        s = abrir_ro(rutas["scg"])
        meta = dict(s.execute("SELECT key, value FROM meta"))
        dia_scg = meta.get("dia")
        for sid, acab, est, usd, stock, cond, url in s.execute(
                "SELECT scryfall_id, acabado, estado, precio_usd, stock, condicion, url FROM precios"):
            k = f"{sid}:{LETRA_ACABADO.get(acab, 'n')}"
            if k not in filas:
                continue
            if est in ("exacto", "probable") and usd:
                filas[k]["scg"] = {"usd": usd, "cond": "NM" if cond == "Near Mint" else (cond or None), "stock": stock,
                                   "url": url or None, "estado": "ok", "dia": dia_scg}
                n_scg += 1
            elif est in ("sin_coincidencia", "no_encontrada", "ambiguo"):
                filas[k]["scg"] = {"estado": "sin_match", "dia": dia_scg}
        s.close()
    estado_fuente("scg", dia_scg, n_scg)
    print(f"  SCG: {n_scg:,} ({dia_scg or 'sin datos'})")

    # --- 4) Card Kingdom (venta NM y compra/buylist) ----------------------------------------------------------------
    n_ck, dia_ck = 0, None
    if rutas.get("ck") and rutas["ck"].is_file():
        k_ = abrir_ro(rutas["ck"])
        dia_ck = dict(k_.execute("SELECT key, value FROM meta")).get("dia")
        for sid, acab, venta, stock, venta_nm, stock_nm, compra, quiere, url in k_.execute(
                "SELECT scryfall_id, acabado, venta, stock, venta_nm, stock_nm, compra, quiere, url FROM precios"):
            k = f"{sid}:{LETRA_ACABADO.get(acab, 'n')}"
            if k not in filas:
                continue
            filas[k]["ck"] = {"usd": venta_nm or venta, "stock": stock_nm if venta_nm else stock,
                              "compra_usd": compra if compra and compra > 0 else None,
                              "quiere": quiere if compra and compra > 0 else None,
                              "url": url or None, "estado": "ok", "dia": dia_ck}
            n_ck += 1
        k_.close()
    estado_fuente("ck", dia_ck, n_ck)
    print(f"  Card Kingdom: {n_ck:,} ({dia_ck or 'sin datos'})")

    # --- 5) Escribir los archivos del dia -------------------------------------------------------------------------
    carpeta = salida / dia
    if carpeta.exists():
        shutil.rmtree(carpeta)
    archivos = []

    # buscar.json: nombres (+ alias en es/fr/de/it/pt y, aparte, japones) -> impresiones. Impresiones ordenadas por precio TCG, mayor primero.
    grupos = {}
    for k, f in filas.items():
        grupos.setdefault(f["nombre"], []).append(k)
    imp, nombres = [], []
    letras = (("har", "H"), ("har_jp", "J"), ("scg", "S"), ("ck", "C"))
    for nombre in sorted(grupos):
        idx = []
        for k in sorted(grupos[nombre], key=lambda x: -filas[x]["tcg"]["usd"]):
            f = filas[k]
            con_dato = "".join(l for campo, l in letras if (f.get(campo) or {}).get("estado") in ("ok", "previo"))
            idx.append(len(imp))
            val = lambda campo, m: (f.get(campo) or {}).get(m) if (f.get(campo) or {}).get("estado") in ("ok", "previo") else None
            imp.append([k, f["set"], f["num"], f["acab"], f["idioma"], f["tcg"]["usd"], con_dato,
                        val("har", "jpy"), val("scg", "usd"), val("ck", "usd")])   # fuente preferida en la pagina
        textos = [normalizar(nombre)] + [normalizar(a) for a in alias_latinos.get(nombre, [])]
        textos = list(dict.fromkeys(t for t in textos if t))
        fila_n = [nombre, "|".join(textos), idx]
        if alias_ja.get(nombre):
            fila_n.append("|".join(dict.fromkeys(alias_ja[nombre])))      # 4a columna: nombres en japones
        nombres.append(fila_n)
    b, sha = _json_compacto(carpeta / "buscar.json", {
        "esquema": ESQUEMA_WEB, "dia": dia, "nombres": nombres,
        "imp_cols": ["k", "set", "num", "acab", "idioma", "tcg_usd", "fuentes", "har_jpy", "scg_usd", "ck_usd"], "imp": imp})
    archivos.append({"nombre": "buscar.json", "bytes": b, "sha256": sha})

    # p/xx.json: los 256 trozos, aunque alguno quede vacio
    trozos = {f"{i:02x}": {} for i in range(256)}
    for k, f in filas.items():
        trozos.setdefault(f["sid"][:2].lower(), {})[k] = f
    total_p = 0
    for xx, contenido in trozos.items():
        b_, _ = _json_compacto(carpeta / "p" / f"{xx}.json", {"esquema": ESQUEMA_WEB, "dia": dia, "filas": contenido})
        total_p += b_

    # movers.json
    def chico(k, extra):
        f = filas[k]
        return {"k": k, "nombre": f["nombre"], "set": f["set"], "num": f["num"], "acab": f["acab"], **extra}
    cambios = []
    for k, f in filas.items():
        t = f["tcg"]
        if t["d1"] is None:
            continue
        antes = round(t["usd"] / (1 + t["d1"] / 100), 2)
        if abs(t["usd"] - antes) >= MOVERS_MIN_DIF_USD:
            cambios.append((t["d1"], k, antes))
    cambios.sort()
    por_n = {n: c["sid"] for n, c in cartas.items()}
    nuevas = []
    for n, a, d_ent in nuevas_hoy:
        k = f"{por_n.get(n)}:{LETRA_ACABADO[a]}"
        if k in filas:
            dias_arriba = (date.fromisoformat(dia) - date.fromisoformat(
                f"{str(d_ent)[:4]}-{str(d_ent)[4:6]}-{str(d_ent)[6:]}")).days + 1
            nuevas.append(chico(k, {"usd": filas[k]["tcg"]["usd"], "dias": dias_arriba}))
    nuevas.sort(key=lambda x: -x["usd"])
    b, sha = _json_compacto(carpeta / "movers.json", {
        "esquema": ESQUEMA_WEB, "dia": dia, "base": "tcg_usd", "min_usd": UMBRAL_WEB_USD,
        "suben": [chico(k, {"usd": filas[k]["tcg"]["usd"], "usd_ant": antes, "pct": p})
                  for p, k, antes in reversed(cambios[-MOVERS_MAX:]) if p > 0],
        "bajan": [chico(k, {"usd": filas[k]["tcg"]["usd"], "usd_ant": antes, "pct": p})
                  for p, k, antes in cambios[:MOVERS_MAX] if p < 0],
        "nuevas": nuevas[:MOVERS_MAX]})
    archivos.append({"nombre": "movers.json", "bytes": b, "sha256": sha})

    # --- 5b) Escaner: huella visual de cada impresion (opcional; solo si hay cache y librerias) ---------------------
    vision = None
    if huellas:
        try:
            vision = _vision(filas, carpeta, salida, Path(huellas))
        except ImportError as e:
            print(f"  (escaner: falta una libreria ({e.name}); se publica sin indice visual)")
        except Exception as e:                                    # el escaner nunca tumba los precios del dia
            print(f"  (escaner: fallo {e.__class__.__name__}: {e}; se publica sin indice visual)")

    # --- 6) Dias que se quedan y manifest (al final: si algo fallo antes, el manifest viejo sigue apuntando al bueno) --
    guardados = sorted((p.name for p in salida.iterdir() if p.is_dir() and PATRON_DIA.match(p.name)), reverse=True)
    for viejo in guardados[DIAS_WEB:]:
        shutil.rmtree(salida / viejo)
    man = {
        "esquema": ESQUEMA_WEB, "dia": dia, "generado_utc": ahora_utc(), "ruta": f"{dia}/", "ultimo_bueno": dia,
        "dias": guardados[:DIAS_WEB], "min_web": 1, "tc": _cambio(salida, usd_mxn, jpy_usd),
        "fuentes": fuentes, "archivos": archivos, "trozos": 256,
    }
    if vision:
        man["vision"] = vision
    _json_compacto(salida / "manifest.json", man)
    print(f"Listo: {salida} (dia {dia}) · buscar.json {archivos[0]['bytes'] / 1e6:.1f} MB · "
          f"precios {total_p / 1e6:.1f} MB en 256 trozos · USD->MXN {man['tc'].get('usd_mxn')}")
    return man


def main(argv=None):
    ap = argparse.ArgumentParser(prog="nube.py", description="Datos de Stack en GitHub Releases.")
    sub = ap.add_subparsers(dest="accion", required=True)
    p = sub.add_parser("preparar", help="en tu PC: arma subir_a_github/ con tus datos para la primera subida")
    p.add_argument("--salida", default=str(RAIZ / "subir_a_github"))
    p = sub.add_parser("publicar")
    p.add_argument("salida")
    p.add_argument("--tag", required=True)
    p.add_argument("--capa", action="append", default=[])
    p = sub.add_parser("restaurar")
    p.add_argument("carpeta")
    p = sub.add_parser("web", help="archivos de la pagina web (manifest + buscar + precios + movers) en SALIDA")
    p.add_argument("salida")
    p.add_argument("--bajar", action="store_true", help="usar el ultimo release de stack-datos en vez de data/ local")
    p.add_argument("--usd-mxn", type=float, help="tipo de cambio a mano (si no, se busca en internet)")
    p.add_argument("--jpy-usd", type=float, help="tipo de cambio a mano (si no, se busca en internet)")
    p.add_argument("--huellas", help="carpeta de cache del escaner; si se da, se arma el indice visual (vision.bin)")
    a = ap.parse_args(argv)
    if a.accion == "web":
        if a.bajar:
            web(a.salida, _bajar_release(Path(a.salida).resolve().parent / "_descarga"), a.usd_mxn, a.jpy_usd, a.huellas)
        else:
            web(a.salida, _rutas_locales(), a.usd_mxn, a.jpy_usd, a.huellas)
        return 0
    if a.accion == "preparar":
        salida = Path(a.salida)
        if salida.exists():
            shutil.rmtree(salida)
        man = publicar(salida, "datos-inicial", [RAIZ])
        if not man["archivos"]:
            print("No encontre datos en", RAIZ / "data")
            return 1
        print("\nSube TODOS los archivos de esa carpeta a un release con el tag  datos-inicial  (ver el tutorial).")
        return 0
    if a.accion == "publicar":
        man = publicar(a.salida, a.tag, a.capa or [str(RAIZ)])
        return 0 if man["archivos"] else 1
    restaurar(a.carpeta)
    return 0


if __name__ == "__main__":
    sys.exit(main())
