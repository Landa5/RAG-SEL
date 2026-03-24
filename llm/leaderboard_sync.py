"""
llm/leaderboard_sync.py — Sincronización periódica del leaderboard de arena.ai
Soporta múltiples categorías: text, spanish, coding, instruction_following, hard_prompts.
Nunca consulta arena.ai por cada request. Usa snapshots y caché local.
"""
import re
import json
import time
import threading
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SYNC_INTERVAL_HOURS = 6
_sync_thread: threading.Thread = None
_running = False

# Mapeo categoría → URL
CATEGORY_URLS = {
    "text": "https://arena.ai/leaderboard/text",
    "spanish": "https://arena.ai/leaderboard/text/spanish",
    "coding": "https://arena.ai/leaderboard/text/coding",
    "instruction_following": "https://arena.ai/leaderboard/text/instruction-following",
    "hard_prompts": "https://arena.ai/leaderboard/text/hard-prompts",
}

# Mapeo de nombres arena.ai → model_id del registry
# SUPUESTO: estos mapeos se mantienen actualizados manualmente o vía config
MODEL_NAME_MAP = {
    "gemini-3.1-pro": "gemini-3.1-pro-preview",
    "gemini-3-flash": "gemini-3-flash-preview",
    "gemini-2.0-flash": "gemini-2.0-flash",
    "gpt-4o": "gpt-4o",
    "gpt-4o-mini": "gpt-4o-mini",
    "claude-3.5-sonnet": "claude-3-5-sonnet-20241022",
    "claude-3.5-haiku": "claude-3-5-haiku-20241022",
}


def _parse_leaderboard_html(html: str, category: str) -> list[dict]:
    """
    Parsea la tabla del leaderboard de arena.ai.
    Intenta extraer: rank, nombre, score, preliminary flag.
    """
    models = []
    # Buscar filas de tabla con datos de modelos
    # Formato típico: filas con rank, nombre, score, CI, etc.
    # Usamos regex flexibles para manejar cambios de formato
    
    # Intentar encontrar datos JSON embebidos (muchos leaderboards modernos los incluyen)
    json_match = re.search(r'__NEXT_DATA__.*?(\{.*?\})\s*</script>', html, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group(1))
            # Navegar estructura Next.js
            props = data.get("props", {}).get("pageProps", {})
            if "leaderboard" in props:
                for entry in props["leaderboard"]:
                    models.append({
                        "name": entry.get("model", ""),
                        "rank": entry.get("rank", 0),
                        "score": entry.get("rating", entry.get("score", 0)),
                        "preliminary": entry.get("preliminary", False),
                        "ci_lower": entry.get("ci_lower"),
                        "ci_upper": entry.get("ci_upper"),
                    })
                return models
        except (json.JSONDecodeError, KeyError):
            pass
    
    # Fallback: parsear tabla HTML con regex
    # Buscar filas de tabla
    row_pattern = re.compile(
        r'<tr[^>]*>.*?'
        r'(\d+)\s*</td>.*?'           # rank
        r'>([^<]+)</(?:td|a)>.*?'      # nombre
        r'(\d{3,4}(?:\.\d+)?)\s*</td>' # score
        r'.*?</tr>',
        re.DOTALL | re.IGNORECASE
    )
    
    for match in row_pattern.finditer(html):
        rank = int(match.group(1))
        name = match.group(2).strip()
        score = float(match.group(3))
        preliminary = "preliminary" in match.group(0).lower() or "†" in match.group(0)
        
        models.append({
            "name": name,
            "rank": rank,
            "score": int(score),
            "preliminary": preliminary,
        })
    
    return models


def sync_category(category: str, url: str) -> dict:
    """
    Sincroniza una categoría del leaderboard.
    Devuelve: {"status": "success"|"failed", "models_count": N, "error": ...}
    """
    from db import model_db as db
    
    try:
        import requests as req_lib
        print(f"  📡 Descargando {category}: {url}")
        resp = req_lib.get(url, timeout=30, headers={
            "User-Agent": "RAG-SEL-Sync/2.1",
            "Accept": "text/html,application/json"
        })
        
        if resp.status_code != 200:
            error = f"HTTP {resp.status_code}"
            db.save_leaderboard_snapshot(category, {}, 0, "failed", error)
            return {"status": "failed", "error": error}
        
        html = resp.text
        models = _parse_leaderboard_html(html, category)
        
        if not models:
            # Si no se parsearon modelos, guardar snapshot fallido pero no romper
            db.save_leaderboard_snapshot(
                category, {"raw_length": len(html)}, 0, "partial",
                "No se pudieron parsear modelos del HTML"
            )
            return {"status": "partial", "models_count": 0, 
                    "error": "Parse vacío, manteniendo datos anteriores"}
        
        # Guardar snapshot crudo
        db.save_leaderboard_snapshot(category, {"models": models}, len(models), "success")
        
        # Actualizar model_registry para modelos conocidos
        updated = 0
        for arena_model in models:
            arena_name = arena_model["name"].lower().strip()
            # Buscar en nuestro mapeo
            model_id = None
            for arena_key, our_id in MODEL_NAME_MAP.items():
                if arena_key in arena_name or arena_name in arena_key:
                    model_id = our_id
                    break
            
            if model_id:
                existing = db.get_model_by_id(model_id)
                if existing:
                    # Actualizar scores para esta categoría
                    scores = existing.get("arena_scores") or {}
                    ranks = existing.get("arena_ranks") or {}
                    if isinstance(scores, str):
                        scores = json.loads(scores)
                    if isinstance(ranks, str):
                        ranks = json.loads(ranks)
                    
                    scores[category] = arena_model["score"]
                    ranks[category] = arena_model["rank"]
                    
                    is_preliminary = arena_model.get("preliminary", False)
                    
                    db.upsert_model(
                        provider=existing["provider"],
                        model_id=model_id,
                        display_name=existing["display_name"],
                        arena_scores=scores,
                        arena_ranks=ranks,
                        is_preliminary=is_preliminary,
                        price_input=existing["price_input"],
                        price_output=existing["price_output"],
                    )
                    updated += 1
        
        print(f"  ✅ {category}: {len(models)} modelos parseados, {updated} actualizados")
        return {"status": "success", "models_count": len(models), "updated": updated}
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Error sync {category}: {error_msg}")
        try:
            db.save_leaderboard_snapshot(category, {}, 0, "failed", error_msg)
        except Exception:
            pass
        return {"status": "failed", "error": error_msg}


def sync_leaderboard() -> dict:
    """
    Sincroniza todas las categorías activas del leaderboard.
    Si una falla, las demás continúan. 
    """
    from db import model_db as db
    
    print("\n🔄 Sincronizando leaderboard de arena.ai...")
    categories = db.get_arena_categories()
    
    results = {}
    for cat in categories:
        cat_id = cat["id"]
        url = CATEGORY_URLS.get(cat_id, cat.get("leaderboard_url"))
        if url:
            results[cat_id] = sync_category(cat_id, url)
        else:
            results[cat_id] = {"status": "skipped", "error": "No URL configurada"}
    
    # Guardar historial de precios
    models = db.get_all_models()
    for m in models:
        try:
            db._execute(f"""
                INSERT INTO {db.SCHEMA}.model_price_history
                (model_id, arena_scores, arena_ranks, price_input, price_output, sync_source)
                VALUES (%s, %s::jsonb, %s::jsonb, %s, %s, 'arena.ai')
            """, (m["id"], json.dumps(m.get("arena_scores", {})),
                  json.dumps(m.get("arena_ranks", {})),
                  m["price_input"], m["price_output"]), fetch=False)
        except Exception:
            pass
    
    # Invalidar caché
    db.invalidate_cache()
    db.preload_cache()
    
    success = sum(1 for r in results.values() if r.get("status") == "success")
    print(f"✅ Sync completado: {success}/{len(results)} categorías exitosas\n")
    
    return results


def _sync_loop():
    """Loop de sincronización periódica (ejecuta en thread dedicado)."""
    global _running
    while _running:
        try:
            sync_leaderboard()
        except Exception as e:
            print(f"❌ Error en sync loop: {e}")
        
        # Dormir por intervalo (chequear cada 60s si debemos parar)
        for _ in range(SYNC_INTERVAL_HOURS * 60):
            if not _running:
                return
            time.sleep(60)


def start_sync_scheduler():
    """Inicia thread de sincronización periódica. Llamar al arrancar la app."""
    global _sync_thread, _running
    if _running:
        return
    
    _running = True
    _sync_thread = threading.Thread(target=_sync_loop, daemon=True, name="leaderboard-sync")
    _sync_thread.start()
    print(f"⏰ Scheduler de leaderboard iniciado (cada {SYNC_INTERVAL_HOURS}h)")


def stop_sync_scheduler():
    global _running
    _running = False


if __name__ == "__main__":
    # Sync manual
    results = sync_leaderboard()
    for cat, res in results.items():
        print(f"  {cat}: {res}")
