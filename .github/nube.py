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


def restaurar(carpeta):
    """Descomprime en data/ los archivos de un release ya descargado en CARPETA (con su manifest.json)."""
    carpeta = Path(carpeta)
    ruta_man = carpeta / "manifest.json"
    if not ruta_man.is_file():
        print(f"No hay manifest.json en {carpeta}: se empieza sin datos anteriores.")
        return 0
    man = json.loads(ruta_man.read_text(encoding="utf-8"))
    n = 0
    for a in man.get("archivos", []):
        gz = carpeta / a["asset"]
        if not gz.is_file():
            print(f"  falta {a['asset']}")
            continue
        destino = RAIZ / "data" / a["ruta"]
        destino.parent.mkdir(parents=True, exist_ok=True)
        tmp = destino.with_name(destino.name + ".part")
        with gzip.open(gz, "rb") as fi, open(tmp, "wb") as fo:
            shutil.copyfileobj(fi, fo, 1 << 20)
        os.replace(tmp, destino)
        n += 1
        print(f"  {a['asset']} -> data/{a['ruta']} ({a.get('dia') or '-'})")
    print(f"Restaurados {n} archivos del release {man.get('tag')}.")
    return n


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
    a = ap.parse_args(argv)
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
