"""
scripts/reindex_all.py — Reindexar todos los PDFs en data/uploads
Procesa de forma secuencial con reintentos y control de rate-limiting.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
from ingestion.load_pdfs import load_pdf
from ingestion.index_documents import index_chunks, get_qdrant_client, is_document_indexed

UPLOADS_DIR = Path("data/uploads")
RETRY_DELAY = 2       # segundos entre archivos
ERROR_DELAY = 5       # segundos extra tras un error


def main():
    client = get_qdrant_client()
    
    pdfs = sorted(UPLOADS_DIR.glob("*.pdf"), key=lambda p: p.name.lower())
    pdfs += sorted(UPLOADS_DIR.glob("*.PDF"), key=lambda p: p.name.lower())
    # Deduplicar (por si hay .pdf y .PDF)
    seen = set()
    unique_pdfs = []
    for p in pdfs:
        if p.name.lower() not in seen:
            seen.add(p.name.lower())
            unique_pdfs.append(p)
    pdfs = unique_pdfs
    
    total = len(pdfs)
    print(f"\n📂 Encontrados {total} PDFs en {UPLOADS_DIR}")
    
    # Contar cuáles ya están indexados
    already = 0
    pending = []
    for p in pdfs:
        if is_document_indexed(client, p.name):
            already += 1
        else:
            pending.append(p)
    
    print(f"✅ Ya indexados: {already}")
    print(f"⏳ Pendientes:   {len(pending)}")
    
    if not pending:
        print("\n🎉 ¡Todos los PDFs ya están indexados!")
        return
    
    print(f"\n🚀 Indexando {len(pending)} PDFs pendientes...\n")
    
    ok = 0
    errors = 0
    error_files = []
    
    for i, pdf_path in enumerate(pending):
        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(pending)}] {pdf_path.name}")
        print(f"{'='*60}")
        
        try:
            chunks = load_pdf(str(pdf_path))
            if chunks:
                indexed = index_chunks(chunks, force=True)
                print(f"  ✅ {indexed} chunks indexados")
                ok += 1
            else:
                print(f"  ⚠️ Sin contenido extraíble")
                errors += 1
                error_files.append(pdf_path.name)
        except Exception as e:
            print(f"  ❌ Error: {e}")
            errors += 1
            error_files.append(pdf_path.name)
            time.sleep(ERROR_DELAY)
        
        # Pausa entre archivos para no saturar la API
        if i < len(pending) - 1:
            time.sleep(RETRY_DELAY)
    
    print(f"\n{'='*60}")
    print(f"📊 RESUMEN FINAL")
    print(f"{'='*60}")
    print(f"  Total procesados: {len(pending)}")
    print(f"  ✅ Exitosos:      {ok}")
    print(f"  ❌ Con errores:    {errors}")
    print(f"  📂 Ya indexados:  {already}")
    print(f"  📄 Total PDFs:    {total}")
    
    if error_files:
        print(f"\n❌ Archivos con error:")
        for f in error_files:
            print(f"  - {f}")


if __name__ == "__main__":
    main()
