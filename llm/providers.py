"""
llm/providers.py — Abstracción multi-proveedor de LLM V2.1
Interfaz común para Gemini, preparada para OpenAI/Anthropic.
"""
import json
import time
import requests
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import GEMINI_API_KEY


@dataclass
class LLMResponse:
    """Respuesta estandarizada de cualquier proveedor."""
    text: str
    model_id: str
    provider: str
    tokens_input: int
    tokens_output: int
    finish_reason: str
    tool_calls: list[dict]   # function calls si las hay
    raw_response: dict       # respuesta cruda del proveedor
    latency_ms: int


class LLMProvider(ABC):
    """Interfaz abstracta para proveedores de LLM."""
    
    @abstractmethod
    def generate(self, messages: list[dict], model_name: str,
                 tools: list[dict] = None, **kwargs) -> LLMResponse:
        """Generación síncrona."""
        pass
    
    @abstractmethod
    def stream(self, messages: list[dict], model_name: str,
               tools: list[dict] = None, **kwargs) -> Iterator[str]:
        """Generación por streaming (SSE)."""
        pass
    
    @abstractmethod
    def provider_name(self) -> str:
        pass


# ─────────────────────────────────────────────
# Gemini Provider
# ─────────────────────────────────────────────

_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/"


class GeminiProvider(LLMProvider):
    """Proveedor Google Gemini vía REST API."""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or GEMINI_API_KEY
    
    def provider_name(self) -> str:
        return "google"
    
    def generate(self, messages: list[dict], model_name: str,
                 tools: list[dict] = None, **kwargs) -> LLMResponse:
        url = f"{_GEMINI_BASE_URL}{model_name}:generateContent?key={self.api_key}"
        
        body = {"contents": messages}
        if tools:
            body["tools"] = tools
        
        gen_config = kwargs.get("generation_config", {})
        if gen_config:
            body["generationConfig"] = gen_config
        
        system_instruction = kwargs.get("system_instruction")
        if system_instruction:
            body["systemInstruction"] = system_instruction
        
        start = time.time()
        resp = requests.post(url, json=body, timeout=kwargs.get("timeout", 60))
        latency_ms = int((time.time() - start) * 1000)
        
        if resp.status_code == 429:
            from llm.fallback_engine import classify_and_raise
            classify_and_raise(Exception(f"429 Rate Limit: {resp.text[:200]}"))
        
        if resp.status_code != 200:
            raise Exception(f"Gemini API error {resp.status_code}: {resp.text[:300]}")
        
        data = resp.json()
        
        # Extraer respuesta
        candidates = data.get("candidates", [{}])
        candidate = candidates[0] if candidates else {}
        content = candidate.get("content", {})
        parts = content.get("parts", [])
        
        text = ""
        tool_calls = []
        for part in parts:
            if "text" in part:
                text += part["text"]
            elif "functionCall" in part:
                tool_calls.append(part["functionCall"])
        
        # Tokens
        usage = data.get("usageMetadata", {})
        tokens_in = usage.get("promptTokenCount", 0)
        tokens_out = usage.get("candidatesTokenCount", 0)
        
        return LLMResponse(
            text=text,
            model_id=model_name,
            provider="google",
            tokens_input=tokens_in,
            tokens_output=tokens_out,
            finish_reason=candidate.get("finishReason", "STOP"),
            tool_calls=tool_calls,
            raw_response=data,
            latency_ms=latency_ms,
        )
    
    def stream(self, messages: list[dict], model_name: str,
               tools: list[dict] = None, **kwargs) -> Iterator[str]:
        url = (f"{_GEMINI_BASE_URL}{model_name}:streamGenerateContent"
               f"?alt=sse&key={self.api_key}")
        
        body = {"contents": messages}
        if tools:
            body["tools"] = tools
        
        gen_config = kwargs.get("generation_config", {})
        if gen_config:
            body["generationConfig"] = gen_config
        
        system_instruction = kwargs.get("system_instruction")
        if system_instruction:
            body["systemInstruction"] = system_instruction
        
        resp = requests.post(url, json=body, stream=True,
                            timeout=kwargs.get("timeout", 120))
        
        if resp.status_code == 429:
            from llm.fallback_engine import classify_and_raise
            classify_and_raise(Exception(f"429 Rate Limit"))
        
        if resp.status_code != 200:
            raise Exception(f"Gemini stream error {resp.status_code}: {resp.text[:300]}")
        
        for line in resp.iter_lines(decode_unicode=True):
            if line and line.startswith("data: "):
                chunk_str = line[6:]
                if chunk_str.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(chunk_str)
                    candidates = chunk.get("candidates", [{}])
                    if candidates:
                        parts = candidates[0].get("content", {}).get("parts", [])
                        for part in parts:
                            if "text" in part:
                                yield part["text"]
                except json.JSONDecodeError:
                    continue


# ─────────────────────────────────────────────
# OpenAI Provider (stub preparado)
# ─────────────────────────────────────────────

class OpenAIProvider(LLMProvider):
    """
    Proveedor OpenAI — STUB para implementación futura.
    SUPUESTO: Se implementará cuando se necesite multi-proveedor activo.
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
    
    def provider_name(self) -> str:
        return "openai"
    
    def generate(self, messages: list[dict], model_name: str,
                 tools: list[dict] = None, **kwargs) -> LLMResponse:
        raise NotImplementedError("OpenAI provider pendiente de implementación")
    
    def stream(self, messages: list[dict], model_name: str,
               tools: list[dict] = None, **kwargs) -> Iterator[str]:
        raise NotImplementedError("OpenAI provider pendiente de implementación")


# ─────────────────────────────────────────────
# Anthropic Provider (stub preparado)
# ─────────────────────────────────────────────

class AnthropicProvider(LLMProvider):
    """
    Proveedor Anthropic — STUB para implementación futura.
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
    
    def provider_name(self) -> str:
        return "anthropic"
    
    def generate(self, messages: list[dict], model_name: str,
                 tools: list[dict] = None, **kwargs) -> LLMResponse:
        raise NotImplementedError("Anthropic provider pendiente de implementación")
    
    def stream(self, messages: list[dict], model_name: str,
               tools: list[dict] = None, **kwargs) -> Iterator[str]:
        raise NotImplementedError("Anthropic provider pendiente de implementación")


# ─────────────────────────────────────────────
# Registry de providers
# ─────────────────────────────────────────────

_PROVIDERS: dict[str, LLMProvider] = {}


def get_provider(provider_name: str) -> LLMProvider:
    """Obtiene instancia del provider."""
    if provider_name not in _PROVIDERS:
        if provider_name == "google":
            _PROVIDERS[provider_name] = GeminiProvider()
        elif provider_name == "openai":
            _PROVIDERS[provider_name] = OpenAIProvider()
        elif provider_name == "anthropic":
            _PROVIDERS[provider_name] = AnthropicProvider()
        else:
            raise ValueError(f"Provider desconocido: {provider_name}")
    return _PROVIDERS[provider_name]
