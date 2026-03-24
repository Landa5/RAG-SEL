"""
llm/fallback_engine.py — Motor de fallback inteligente V2.1
Genera y ejecuta cadenas de fallback con triggers configurables.
Prefiere cambiar proveedor cuando el problema es de proveedor.
"""
from dataclasses import dataclass
from typing import Optional, Callable, Iterator
import time

from llm.model_router import ModelInfo


@dataclass
class FallbackTrigger:
    """Define qué condiciones activan el fallback."""
    on_error: bool = True
    on_timeout: bool = True
    on_rate_limit: bool = True
    on_low_quality: bool = False
    timeout_ms: int = 30000
    max_retries: int = 1


@dataclass
class FallbackStep:
    """Un paso en la cadena de fallback."""
    model: ModelInfo
    trigger: FallbackTrigger
    position: int


@dataclass
class FallbackResult:
    """Resultado de la ejecución con fallback."""
    success: bool
    model_used: ModelInfo
    response: object  # LLMResponse del provider
    fallback_triggered: bool
    fallback_reason: Optional[str]
    attempts: list[dict]  # historial de intentos


class FallbackEngine:
    """
    Motor de fallback que ejecuta una cadena de modelos en orden
    hasta obtener una respuesta exitosa.
    """
    
    def __init__(self, primary: ModelInfo, chain: list[ModelInfo],
                 pipeline_id: str = None):
        self.primary = primary
        self.chain = chain
        self.pipeline_id = pipeline_id
        self._steps = self._build_steps()
    
    def _build_steps(self) -> list[FallbackStep]:
        """Construye pasos de la cadena desde DB config o defaults."""
        from db import model_db as db
        
        steps = []
        fb_chains = db.get_fallback_chains()
        pipeline_chain = fb_chains.get(self.pipeline_id, [])
        
        # Paso 0: modelo primario (siempre)
        primary_config = next(
            (fc for fc in pipeline_chain 
             if fc.get("model_name") == self.primary.model_id),
            None
        )
        steps.append(FallbackStep(
            model=self.primary,
            trigger=FallbackTrigger(
                timeout_ms=primary_config.get("timeout_ms", 30000) if primary_config else 30000,
                max_retries=primary_config.get("max_retries", 1) if primary_config else 1,
            ),
            position=0,
        ))
        
        # Pasos de fallback
        for i, fb_model in enumerate(self.chain):
            fb_config = next(
                (fc for fc in pipeline_chain 
                 if fc.get("model_name") == fb_model.model_id),
                None
            )
            steps.append(FallbackStep(
                model=fb_model,
                trigger=FallbackTrigger(
                    on_error=fb_config.get("trigger_on_error", True) if fb_config else True,
                    on_timeout=fb_config.get("trigger_on_timeout", True) if fb_config else True,
                    on_rate_limit=fb_config.get("trigger_on_rate_limit", True) if fb_config else True,
                    on_low_quality=fb_config.get("trigger_on_low_quality", False) if fb_config else False,
                    timeout_ms=fb_config.get("timeout_ms", 30000) if fb_config else 30000,
                    max_retries=fb_config.get("max_retries", 1) if fb_config else 1,
                ),
                position=i + 1,
            ))
        
        return steps
    
    def execute(self, call_fn: Callable, *args, **kwargs) -> FallbackResult:
        """
        Ejecuta la cadena de fallback de forma síncrona.
        
        Args:
            call_fn: función que recibe (model_id, *args, **kwargs) y devuelve respuesta
        """
        attempts = []
        
        for step in self._steps:
            for retry in range(step.trigger.max_retries + 1):
                attempt = {
                    "model": step.model.model_id,
                    "position": step.position,
                    "retry": retry,
                    "timestamp": time.time(),
                }
                
                try:
                    start = time.time()
                    response = call_fn(step.model.model_id, *args, **kwargs)
                    elapsed_ms = int((time.time() - start) * 1000)
                    
                    attempt["success"] = True
                    attempt["latency_ms"] = elapsed_ms
                    attempts.append(attempt)
                    
                    return FallbackResult(
                        success=True,
                        model_used=step.model,
                        response=response,
                        fallback_triggered=step.position > 0,
                        fallback_reason=attempts[-2]["error"] if step.position > 0 and len(attempts) > 1 else None,
                        attempts=attempts,
                    )
                    
                except _RateLimitError as e:
                    attempt["success"] = False
                    attempt["error"] = f"RATE_LIMIT: {e}"
                    attempt["error_type"] = "rate_limit"
                    attempts.append(attempt)
                    
                    if not step.trigger.on_rate_limit:
                        continue
                    # Rate limit → saltar al siguiente modelo directamente
                    break
                    
                except _TimeoutError as e:
                    attempt["success"] = False
                    attempt["error"] = f"TIMEOUT: {e}"
                    attempt["error_type"] = "timeout"
                    attempts.append(attempt)
                    
                    if not step.trigger.on_timeout:
                        continue
                    break
                    
                except Exception as e:
                    attempt["success"] = False
                    attempt["error"] = f"ERROR: {e}"
                    attempt["error_type"] = "error"
                    attempts.append(attempt)
                    
                    if not step.trigger.on_error:
                        continue
                    # Si quedan reintentos, continuar; si no, siguiente modelo
                    if retry < step.trigger.max_retries:
                        time.sleep(min(2 ** retry, 5))  # backoff exponencial
                        continue
                    break
        
        # Todos los intentos fallaron
        return FallbackResult(
            success=False,
            model_used=self._steps[-1].model if self._steps else self.primary,
            response=None,
            fallback_triggered=True,
            fallback_reason="Todos los modelos en la cadena fallaron",
            attempts=attempts,
        )
    
    def execute_stream(self, call_fn: Callable, *args, **kwargs) -> tuple[ModelInfo, Iterator]:
        """
        Ejecuta la cadena de fallback para streaming.
        Devuelve (model_usado, iterator).
        
        NOTA: Para streaming, el retry es más limitado — no podemos reintentar
        a mitad de un stream. Solo reintentamos si falla al iniciar.
        """
        for step in self._steps:
            try:
                iterator = call_fn(step.model.model_id, *args, **kwargs)
                # Intentar obtener primer chunk para validar
                return step.model, iterator
            except Exception as e:
                error_type = _classify_error(e)
                print(f"⚠️ Fallback stream: {step.model.model_id} → {error_type}: {e}")
                continue
        
        raise RuntimeError("Todos los modelos en la cadena de fallback fallaron para streaming")


# ─────────────────────────────────────────────
# Excepciones tipadas para el fallback
# ─────────────────────────────────────────────

class _RateLimitError(Exception):
    pass

class _TimeoutError(Exception):
    pass


def classify_and_raise(error: Exception):
    """Clasifica un error HTTP y lanza la excepción tipada correcta."""
    error_str = str(error).lower()
    
    if "429" in error_str or "rate limit" in error_str or "quota" in error_str:
        raise _RateLimitError(str(error)) from error
    elif "timeout" in error_str or "timed out" in error_str:
        raise _TimeoutError(str(error)) from error
    else:
        raise error


def _classify_error(error: Exception) -> str:
    error_str = str(error).lower()
    if "429" in error_str or "rate limit" in error_str:
        return "rate_limit"
    elif "timeout" in error_str:
        return "timeout"
    return "error"
