"""
Thin RAGFlow HTTP client — retrieval-only.

The RAGFlow instance for this project runs in retrieval-only mode (local Ollama
embedding `nomic-embed-text`, no LLM provider configured), so this client
deliberately does NOT implement chat / generation. It exposes:

  - is_configured()      — quick check; everything else is a no-op when False
  - retrieve()           — POST /api/v1/retrieval, returns ranked chunks
  - upload_document()    — POST /api/v1/datasets/{id}/documents (used by ingest)
  - list_documents()     — GET  /api/v1/datasets/{id}/documents
  - delete_document()    — DELETE /api/v1/datasets/{id}/documents

Configuration via env vars (degrades gracefully if unset):
  RAGFLOW_BASE_URL    — e.g. http://192.168.88.38
  RAGFLOW_API_KEY     — Bearer token from RAGFlow UI Settings → API
  RAGFLOW_DATASET_ID  — default dataset id (callers can override per request)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import requests


DEFAULT_TIMEOUT = 30  # seconds — retrieval is fast, ingest may be longer


@dataclass
class Chunk:
    """One retrieval result chunk."""
    id: str
    content: str
    document_id: str
    document_keyword: Optional[str]   # the document name / page title
    similarity: float
    vector_similarity: float
    term_similarity: float


class RAGFlowClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        default_dataset_id: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.base_url = (base_url or os.environ.get("RAGFLOW_BASE_URL", "")).rstrip("/")
        self.api_key = api_key or os.environ.get("RAGFLOW_API_KEY", "")
        self.default_dataset_id = default_dataset_id or os.environ.get("RAGFLOW_DATASET_ID", "")
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    def is_configured(self) -> bool:
        """Return True if base_url and api_key are set. Callers should check
        this before invoking any other method so we degrade gracefully when
        RAGFlow isn't available."""
        return bool(self.base_url and self.api_key)

    def _headers(self, content_type: Optional[str] = "application/json") -> dict:
        h = {"Authorization": f"Bearer {self.api_key}"}
        if content_type:
            h["Content-Type"] = content_type
        return h

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def retrieve(
        self,
        question: str,
        dataset_ids: Optional[list[str]] = None,
        top_k: int = 5,
        similarity_threshold: float = 0.2,
        vector_similarity_weight: float = 0.3,
    ) -> list[Chunk]:
        """POST /api/v1/retrieval — semantic search over a RAGFlow dataset.

        Returns the top-ranked chunks. Each chunk has:
          .similarity            — combined score (this is what to threshold)
          .vector_similarity     — cosine sim of the embedding
          .term_similarity       — keyword overlap (BM25-ish)
          .document_keyword      — the source document's display name
                                   (we use this as the canonical part name in
                                   the smart drift resolver)
        """
        if not self.is_configured():
            return []

        ids = dataset_ids or ([self.default_dataset_id] if self.default_dataset_id else [])
        if not ids:
            raise ValueError("retrieve(): no dataset_ids and no RAGFLOW_DATASET_ID env var set")

        body = {
            "question": question,
            "dataset_ids": ids,
            "page": 1,
            "page_size": top_k,
            "similarity_threshold": similarity_threshold,
            "vector_similarity_weight": vector_similarity_weight,
            "top_k": max(top_k, 32),  # RAGFlow re-ranks; give it some headroom
        }

        try:
            resp = requests.post(
                self._url("/api/v1/retrieval"),
                headers=self._headers(),
                json=body,
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  RAGFlow retrieve() failed: {e}")
            return []

        payload = resp.json()
        if payload.get("code") != 0:
            print(f"  RAGFlow retrieve() error: {payload.get('message')}")
            return []

        chunks_raw = payload.get("data", {}).get("chunks", []) or []
        return [
            Chunk(
                id=c.get("id", ""),
                content=c.get("content", ""),
                document_id=c.get("document_id", ""),
                document_keyword=c.get("document_keyword") or c.get("docnm_kwd") or c.get("document_name"),
                similarity=float(c.get("similarity", 0.0)),
                vector_similarity=float(c.get("vector_similarity", 0.0)),
                term_similarity=float(c.get("term_similarity", 0.0)),
            )
            for c in chunks_raw
        ]

    # ------------------------------------------------------------------
    # Document management (used by scripts/ragflow_ingest.py)
    # ------------------------------------------------------------------

    def list_documents(self, dataset_id: Optional[str] = None) -> list[dict]:
        """GET /api/v1/datasets/{id}/documents — paginated, returns all docs."""
        if not self.is_configured():
            return []
        ds = dataset_id or self.default_dataset_id
        if not ds:
            raise ValueError("list_documents(): no dataset_id provided")

        all_docs: list[dict] = []
        page = 1
        page_size = 100
        while True:
            try:
                resp = requests.get(
                    self._url(f"/api/v1/datasets/{ds}/documents"),
                    headers=self._headers(content_type=None),
                    params={"page": page, "page_size": page_size},
                    timeout=self.timeout,
                )
                resp.raise_for_status()
            except requests.RequestException as e:
                print(f"  RAGFlow list_documents() failed: {e}")
                return all_docs

            payload = resp.json()
            if payload.get("code") != 0:
                print(f"  RAGFlow list_documents() error: {payload.get('message')}")
                return all_docs

            data = payload.get("data", {}) or {}
            docs = data.get("docs", []) or []
            if not docs:
                break
            all_docs.extend(docs)
            if len(docs) < page_size:
                break
            page += 1

        return all_docs

    def upload_document(
        self,
        filename: str,
        content: bytes,
        dataset_id: Optional[str] = None,
        content_type: str = "text/markdown",
    ) -> Optional[str]:
        """POST /api/v1/datasets/{id}/documents — upload one file.

        Returns the new document_id, or None on failure.
        """
        if not self.is_configured():
            return None
        ds = dataset_id or self.default_dataset_id
        if not ds:
            raise ValueError("upload_document(): no dataset_id provided")

        try:
            resp = requests.post(
                self._url(f"/api/v1/datasets/{ds}/documents"),
                headers=self._headers(content_type=None),  # multipart sets its own
                files={"file": (filename, content, content_type)},
                timeout=self.timeout * 2,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  RAGFlow upload_document({filename}) failed: {e}")
            return None

        payload = resp.json()
        if payload.get("code") != 0:
            print(f"  RAGFlow upload_document({filename}) error: {payload.get('message')}")
            return None

        # Response shape: {data: [{id, name, ...}]}
        data = payload.get("data", []) or []
        if data and isinstance(data, list):
            return data[0].get("id")
        return None

    def delete_document(self, document_id: str, dataset_id: Optional[str] = None) -> bool:
        if not self.is_configured():
            return False
        ds = dataset_id or self.default_dataset_id
        try:
            resp = requests.delete(
                self._url(f"/api/v1/datasets/{ds}/documents"),
                headers=self._headers(),
                json={"ids": [document_id]},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json().get("code") == 0
        except requests.RequestException as e:
            print(f"  RAGFlow delete_document({document_id}) failed: {e}")
            return False

    def parse_document(self, document_ids: list[str], dataset_id: Optional[str] = None) -> bool:
        """POST /api/v1/datasets/{id}/chunks — kick off parsing/embedding for
        newly-uploaded documents. RAGFlow doesn't auto-parse on upload.

        Uses a longer timeout because RAGFlow blocks on the parse pipeline
        starting (the request returns once docs are queued, not when they
        finish embedding)."""
        if not self.is_configured():
            return False
        ds = dataset_id or self.default_dataset_id
        try:
            resp = requests.post(
                self._url(f"/api/v1/datasets/{ds}/chunks"),
                headers=self._headers(),
                json={"document_ids": document_ids},
                timeout=300,  # parse-trigger can take a while on slow VMs
            )
            resp.raise_for_status()
            return resp.json().get("code") == 0
        except requests.RequestException as e:
            print(f"  RAGFlow parse_document() failed: {e}")
            return False
