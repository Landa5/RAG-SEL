"""
ingestion/watch.py — Indexación Continua (Auto-sync)
Monitorea un directorio específico en busca de nuevos PDFs y lanza el pipeline
de OCR e indexación en Qdrant sin intervención manual.
"""
import sys
import os
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Añadir el path raíz para importaciones absolutas
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.load_pdfs import load_pdf
from ingestion.index_documents import index_chunks

WATCH_DIR = str(Path(__file__).parent.parent / "data" / "docs")

class PDFHandler(FileSystemEventHandler):
    def process_file(self, file_path: str):
        if not file_path.lower().endswith(".pdf"):
            return
            
        print(f"\n[{time.strftime('%H:%M:%S')}] 🔍 Detectado nuevo PDF: {os.path.basename(file_path)}")
        try:
            # Esperar un segundo para asegurar que el archivo terminó de copiarse
            time.sleep(1)
            chunks = load_pdf(file_path)
            index_chunks(chunks, force=True)
            print(f"[{time.strftime('%H:%M:%S')}] ✅ Archivo indexado correctamente.\n")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ❌ Error procesando {file_path}: {e}\n")

    def on_created(self, event):
        if not event.is_directory:
            self.process_file(event.src_path)

    def on_modified(self, event):
        # A veces al descargar/copiar se dispara modified después de created
        # Filtrar o manejar con cuidado (aquí lo ignoramos para no duplicar OCR, 
        # a menos que sepamos gestionar un debounce/lock).
        pass


def start_watcher(directory: str = WATCH_DIR):
    """Inicia el demonio de observación."""
    os.makedirs(directory, exist_ok=True)
    
    event_handler = PDFHandler()
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=True)
    
    print(f"👀 Iniciando indexador continuo en: {directory}")
    print("Suelta cualquier archivo *.pdf ahí y se procesará automáticamente.")
    print("Presiona Ctrl+C para detener.")
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nDeteniendo indexador continuo...")
    observer.join()

if __name__ == "__main__":
    start_watcher()
