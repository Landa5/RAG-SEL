"""
llm/tools.py — Definición de herramientas para Tool Calling de Gemini
"""

# Definición del esquema de herramientas de Gemini
GEMINI_TOOLS = [
    {
        "functionDeclarations": [
            {
                "name": "ejecutar_consulta_sql",
                "description": "Ejecuta una consulta SQL SELECT de solo lectura en la base de datos PostgreSQL de la empresa. Usa el ESQUEMA DE BD proporcionado en las instrucciones del sistema para construir consultas correctas. Solo se permiten SELECT. Los nombres de tabla y columna van entre comillas dobles (ej: \"Empleado\", \"kmRecorridos\").",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "sql": {
                            "type": "STRING",
                            "description": "Consulta SQL SELECT a ejecutar. Los nombres de tabla van entre comillas dobles. Ejemplo: SELECT e.nombre, SUM(u.\"kmRecorridos\") FROM \"UsoCamion\" u JOIN \"Camion\" c ON c.id = u.\"camionId\" JOIN \"JornadaLaboral\" j ON j.id = u.\"jornadaId\" JOIN \"Empleado\" e ON e.id = j.\"empleadoId\" GROUP BY e.nombre"
                        },
                        "descripcion": {
                            "type": "STRING",
                            "description": "Breve descripción de qué busca esta consulta (para el log)"
                        }
                    },
                    "required": ["sql", "descripcion"]
                }
            },
            {
                "name": "buscar_documentos_pdf",
                "description": "Busca en la base de datos vectorial (Qdrant) que contiene el texto y facturas extraídas de los PDFs. Úsalo cuando te pregunten sobre justificantes específicos, datos legales, detalles de facturas o documentos escaneados.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "query": {
                            "type": "STRING",
                            "description": "La frase o palabras clave para buscar en los documentos."
                        },
                        "matricula_filtro": {
                            "type": "STRING",
                            "description": "Filtrar resultados por matrícula de camión (opcional)."
                        }
                    },
                    "required": ["query"]
                }
            }
        ]
    }
]
