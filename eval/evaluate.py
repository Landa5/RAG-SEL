"""
eval/evaluate.py — Evaluación Automática de Calidad del RAG (LLM-as-a-Judge)
Mide la Fidelidad (si la respuesta se basa en el contexto y no inventa)
y la Relevancia (si la respuesta contesta correctamente la pregunta).
"""
import sys
import os
import json
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from llm.generate import generate_answer_agentic
from config import GEMINI_API_KEY

GEMINI_API_URL = (
    "https://generativelanguage.googleapis.com/v1/models/"
    "gemini-2.0-flash:generateContent?key={key}"
)

# Test set: preguntas y respuestas esperadas
EVAL_DATASET = [
    {
        "query": "¿Cuántas reparaciones ha tenido el camión 1713-FHR?",
        "intent": "Obtener el listado y cantidad de reparaciones del vehículo."
    },
    {
        "query": "¿En qué convenio están los trabajadores?",
        "intent": "Identificar el convenio colectivo de la empresa."
    },
    {
        "query": "Dime las ausencias recientes de José Vicente",
        "intent": "Buscar historial de bajas/ausencias/vacaciones."
    }
]

JUDGE_PROMPT = """Eres un evaluador de sistemas RAG. 
Evalúa la RESPUESTA a la PREGUNTA basándote en dos métricas de 1 a 5.

Relevancia (1-5): ¿La respuesta contesta a la pregunta directamente sin divagar?
Fidelidad (1-5): ¿La respuesta parece basarse en fuentes y confiesa si no sabe algo en lugar de inventar (alucinar)?

PREGUNTA: {query}
RESPUESTA DEL SISTEMA: {answer}

Devuelve SOLO este JSON:
{{"relevancia": <int>, "fidelidad": <int>, "explicacion": "<str>"}}
"""


def _evaluate_answer(query: str, answer: str) -> dict:
    prompt = JUDGE_PROMPT.format(query=query, answer=answer)
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.0}
    }
    try:
        url = GEMINI_API_URL.format(key=GEMINI_API_KEY)
        resp = requests.post(url, json=payload, timeout=30)
        text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        
        # Parse JSON
        start = text.find("{")
        end = text.rfind("}") + 1
        return json.loads(text[start:end])
    except Exception as e:
        return {"relevancia": 0, "fidelidad": 0, "explicacion": f"Error evaluando: {e}"}


def run_evaluation():
    print("🚀 Iniciando evaluación automática del Agente RAG...")
    
    total_rel = 0
    total_faith = 0
    
    for i, test in enumerate(EVAL_DATASET, 1):
        query = test["query"]
        print(f"\n[{i}/{len(EVAL_DATASET)}] Evaluando: '{query}'")
        
        # 1. Ejecutar el Agente
        result = generate_answer_agentic(query)
        answer = result["answer"]
        print(f"  🤖 Respuesta: {answer[:100]}...")
        
        # 2. Juzgar la respuesta
        eval_metrics = _evaluate_answer(query, answer)
        rel = eval_metrics.get("relevancia", 0)
        faith = eval_metrics.get("fidelidad", 0)
        
        total_rel += rel
        total_faith += faith
        
        print(f"  📊 Relevancia: {rel}/5 | Fidelidad: {faith}/5")
        print(f"  💡 Explicación: {eval_metrics.get('explicacion', '')}")

    n = len(EVAL_DATASET)
    print("\n" + "="*40)
    print(f"🏆 RESULTADOS GLOBALES ({n} preguntas):")
    print(f"   Relevancia Promedio: {total_rel/n:.1f}/5.0")
    print(f"   Fidelidad Promedio:  {total_faith/n:.1f}/5.0")
    print("="*40)


if __name__ == "__main__":
    run_evaluation()
