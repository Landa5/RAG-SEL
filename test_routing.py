import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import model_db as mdb
mdb.preload_cache()

from llm.model_router import route
r = route("cuantos km ha hecho la flota", "agentic_sql")
print(f"Modelo: {r.primary.model_id}, Score: {r.composite_score:.3f}")
print(f"Reason: {r.reason}")
for c in r.all_candidates_scores:
    print(f"  {c['model_id']}: {c['score']}")

from llm.task_classifier import classify
from llm.pipeline_selector import select_pipeline
from llm.execution_planner import create_execution_plan

queries = [
    "hola buenos dias",
    "cuantos km ha hecho la flota este mes",
    "busca en las facturas del proveedor X",
    "predice el gasto en gasoil del proximo mes",
    "analiza la tendencia de productividad por conductor",
]

for q in queries:
    c = classify(q)
    p = select_pipeline(c)
    plan = create_execution_plan(c, p)
    r2 = route(q, p.pipeline_id)
    print(f"\n>>> {q}")
    print(f"    Task: {c.primary_task_family} | Pipeline: {p.pipeline_id} | Model: {r2.primary.model_id} ({r2.composite_score:.3f})")
