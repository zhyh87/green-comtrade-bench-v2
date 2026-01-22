from __future__ import annotations

import os
import errno
import time
import shutil
import logging
import uuid
import concurrent.futures
from pathlib import Path
from typing import Any, Dict, Optional, Union

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .tasks import get_task
from .judge import score_output


# ---------------------------------------------------------------------------
# A2A JSON-RPC Models
# ---------------------------------------------------------------------------

class JsonRpcRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[Union[str, int]] = None
    method: str
    params: Optional[Dict[str, Any]] = None


class JsonRpcError(BaseModel):
    code: int
    message: str
    data: Optional[Any] = None


class JsonRpcErrorResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[Union[str, int]] = None
    error: JsonRpcError


class JsonRpcSuccessResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[Union[str, int]] = None
    result: Any


# In-memory task store for A2A tasks/get
TASK_STORE: Dict[str, Dict[str, Any]] = {}

# Shared agent card info
AGENT_CARD = {
    "name": "green-comtrade-bench",
    "version": "0.1.0",
    "description": "Green Agent benchmark for Comtrade API evaluation",
    "url": "http://green-agent:9009/a2a/rpc",
    "endpoints": {"rpc": "/a2a/rpc", "health": "/healthz"},
    "capabilities": {"streaming": False, "pushNotifications": False},
    "defaultInputModes": ["application/json"],
    "defaultOutputModes": ["application/json"],
    "skills": [
        {
            "id": "a2a.rpc",
            "name": "a2a.rpc",
            "description": "Handle A2A JSON-RPC requests via /a2a/rpc",
            "tags": ["a2a", "rpc"],
        }
    ],
}

APP_HOST = os.environ.get("HOST", "0.0.0.0")
APP_PORT = int(os.environ.get("PORT", "9009"))

MOCK_URL = os.environ.get("MOCK_URL", "http://mock-comtrade:8000")
PURPLE_OUTPUT_ROOT = Path(os.environ.get("PURPLE_OUTPUT_ROOT", "/workspace/purple_output"))
SCORE_TIMEOUT = float(os.environ.get("SCORE_TIMEOUT", "8"))
STAGE_TIMEOUT = float(os.environ.get("STAGE_TIMEOUT", "8"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("green-agent")

app = FastAPI(title="Green Comtrade Bench (MVP)")


class AssessRequest(BaseModel):
    task_id: str
    purple_output_subdir: Optional[str] = None


# macOS Docker bind-mounts can intermittently raise EDEADLK (Errno 35)
_RETRY_ERRNOS = {getattr(errno, "EDEADLK", 35), 35}


def _with_retries(
    func,
    *,
    attempts: int = 10,
    base_sleep: float = 0.05,
    max_sleep: float = 0.5,
    max_elapsed: float = 5.0,
):
    last_exc: Exception | None = None
    start = time.monotonic()
    for i in range(attempts):
        try:
            return func()
        except OSError as e:
            last_exc = e
            if getattr(e, "errno", None) in _RETRY_ERRNOS:
                if time.monotonic() - start >= max_elapsed:
                    break
                time.sleep(min(base_sleep * (2**i), max_sleep))
                continue
            raise
    if last_exc is not None:
        if isinstance(last_exc, OSError) and getattr(last_exc, "errno", None) in _RETRY_ERRNOS:
            raise TimeoutError("I/O retry deadline exceeded")
        raise last_exc


def _copy_file_retry(src: Path, dst: Path, *, max_elapsed: float) -> None:
    def _copy():
        dst.parent.mkdir(parents=True, exist_ok=True)
        with src.open("rb") as fsrc, dst.open("wb") as fdst:
            for chunk in iter(lambda: fsrc.read(1024 * 1024), b""):
                fdst.write(chunk)

    _with_retries(_copy, max_elapsed=max_elapsed)


def _copy_output_dir_retry(src_dir: Path, dst_dir: Path, *, max_elapsed: float) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    # Copy only what judge needs (+ optional manifest)
    for name in ["data.jsonl", "metadata.json", "run.log", "manifest.json"]:
        p = src_dir / name
        if p.exists():
            _copy_file_retry(p, dst_dir / name, max_elapsed=max_elapsed)


@app.get("/agent-card")
def agent_card() -> Dict[str, Any]:
    return {
        "name": "green-comtrade-bench",
        "version": "0.1.0",
        "endpoints": {"assess": "/assess"},
        "notes": "MVP skeleton. Adapt to A2A protocol for AgentBeats submission.",
    }


@app.post("/assess")
def assess(req: AssessRequest) -> Dict[str, Any]:
    """Original /assess endpoint - uses shared internal logic."""
    return _run_assess_internal(req.task_id, req.purple_output_subdir)


# ---------------------------------------------------------------------------
# A2A Discovery & Health Endpoints
# ---------------------------------------------------------------------------

@app.get("/.well-known/agent.json")
def a2a_agent_card():
    """A2A Agent Card discovery endpoint."""
    return JSONResponse(content=AGENT_CARD)


@app.get("/health")
def health() -> Dict[str, Any]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/.well-known/agent-card.json")
@app.post("/.well-known/agent-card.json")
def a2a_agent_card_json():
    """A2A Agent Card discovery endpoint (alternate path).
    
    Returns full AGENT_CARD dict via JSONResponse to avoid any response_model filtering.
    Supports both GET and POST methods for compatibility with different A2A clients.
    Smoke test: curl -s http://localhost:9009/.well-known/agent-card.json | jq .
    """
    return JSONResponse(content=AGENT_CARD)


# ---------------------------------------------------------------------------
# Internal helper to run assessment (shared by /assess and tasks/send)
# ---------------------------------------------------------------------------

def _run_assess_internal(task_id: str, purple_output_subdir: Optional[str] = None) -> Dict[str, Any]:
    """
    Internal function to run assessment logic.
    Returns the assess result dict or raises HTTPException on failure.
    """
    task = get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Unknown task_id: {task_id}")

    logger.info("assess start task_id=%s", task_id)

    # Configure mock service for this task
    try:
        r = requests.post(
            f"{MOCK_URL}/configure",
            json={
                "task_id": task.task_id,
                "query": task.query,
                "constraints": task.constraints,
                "fault_injection": task.fault_injection,
            },
            timeout=5,
        )
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to configure mock service: {e}")

    # Where Purple outputs are expected (bind-mounted)
    out_dir = PURPLE_OUTPUT_ROOT / (purple_output_subdir or task_id)

    # IMPORTANT: stage outputs into container FS first to avoid Errno 35 bind-mount deadlocks
    tmp_root = Path("/tmp/purple_output_cache")
    tmp_dir = tmp_root / (purple_output_subdir or task_id)

    try:
        logger.info("staging outputs from %s to %s", out_dir, tmp_dir)
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        _copy_output_dir_retry(out_dir, tmp_dir, max_elapsed=STAGE_TIMEOUT)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stage purple outputs for scoring: {e}")

    logger.info("scoring output dir %s", tmp_dir)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            score_output,
            tmp_dir,
            task_expected={
                "task_id": task.task_id,
                "query": task.query,
                "constraints": task.constraints,
                "fault_injection": task.fault_injection,
            },
        )
        try:
            result = future.result(timeout=SCORE_TIMEOUT)
        except concurrent.futures.TimeoutError:
            raise HTTPException(status_code=504, detail=f"Scoring timed out after {SCORE_TIMEOUT}s")

    logger.info("assess done task_id=%s total=%s", task_id, result.total)
    return {
        "task_id": task.task_id,
        "score_total": result.total,
        "score_breakdown": result.breakdown,
        "errors": result.errors,
        "details": result.details,
    }


# ---------------------------------------------------------------------------
# A2A JSON-RPC Endpoint
# ---------------------------------------------------------------------------

def _jsonrpc_error(req_id: Optional[Union[str, int]], code: int, message: str, data: Any = None) -> JSONResponse:
    """Helper to create a JSON-RPC error response."""
    return JSONResponse(
        content={
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": code, "message": message, "data": data},
        }
    )


def _jsonrpc_success(req_id: Optional[Union[str, int]], result: Any) -> JSONResponse:
    """Helper to create a JSON-RPC success response."""
    return JSONResponse(
        content={
            "jsonrpc": "2.0",
            "id": req_id,
            "result": result,
        }
    )


@app.post("/a2a/rpc")
def a2a_rpc(req: JsonRpcRequest) -> JSONResponse:
    """
    A2A JSON-RPC 2.0 endpoint.
    Supports methods: tasks/send, tasks/get, tasks/cancel, tasks/sendSubscribe
    """
    method = req.method
    params = req.params or {}
    req_id = req.id

    # Validate jsonrpc version
    if req.jsonrpc != "2.0":
        return _jsonrpc_error(req_id, -32600, "Invalid Request: jsonrpc must be '2.0'")

    # -------------------------------------------------------------------------
    # tasks/send - Execute assessment and return result
    # -------------------------------------------------------------------------
    if method == "tasks/send":
        try:
            # Extract task_id from params.task.input.content
            task_input = params.get("task", {}).get("input", {})
            content = task_input.get("content", {})
            
            if isinstance(content, str):
                # Try parsing as JSON if string
                import json
                try:
                    content = json.loads(content)
                except json.JSONDecodeError:
                    return _jsonrpc_error(req_id, -32602, "Invalid params: content must be valid JSON object")
            
            task_id = content.get("task_id")
            if not task_id:
                return _jsonrpc_error(req_id, -32602, "Invalid params: task_id is required in content")
            
            purple_output_subdir = content.get("purple_output_subdir")
            
            # Generate A2A task ID
            a2a_task_id = str(uuid.uuid4())
            
            # Run assessment using shared logic
            assess_result = _run_assess_internal(task_id, purple_output_subdir)
            
            # Store result
            TASK_STORE[a2a_task_id] = {
                "id": a2a_task_id,
                "status": "completed",
                "output": {
                    "type": "object",
                    "content": assess_result,
                },
            }
            
            return _jsonrpc_success(req_id, {"task": TASK_STORE[a2a_task_id]})
        
        except HTTPException as e:
            # Convert HTTP exceptions to JSON-RPC errors
            a2a_task_id = str(uuid.uuid4())
            TASK_STORE[a2a_task_id] = {
                "id": a2a_task_id,
                "status": "failed",
                "error": {"message": e.detail},
            }
            return _jsonrpc_error(req_id, -32000, f"Assessment failed: {e.detail}")
        except Exception as e:
            return _jsonrpc_error(req_id, -32000, f"Internal error: {str(e)}")

    # -------------------------------------------------------------------------
    # tasks/get - Retrieve task status/result
    # -------------------------------------------------------------------------
    elif method == "tasks/get":
        a2a_task_id = params.get("task_id")
        if not a2a_task_id:
            return _jsonrpc_error(req_id, -32602, "Invalid params: task_id is required")
        
        if a2a_task_id not in TASK_STORE:
            return _jsonrpc_error(req_id, -32001, "Task not found", {"task_id": a2a_task_id})
        
        return _jsonrpc_success(req_id, {"task": TASK_STORE[a2a_task_id]})

    # -------------------------------------------------------------------------
    # tasks/cancel - Cancel a task (stub: tasks complete synchronously)
    # -------------------------------------------------------------------------
    elif method == "tasks/cancel":
        a2a_task_id = params.get("task_id")
        if not a2a_task_id:
            return _jsonrpc_error(req_id, -32602, "Invalid params: task_id is required")
        
        if a2a_task_id not in TASK_STORE:
            return _jsonrpc_error(req_id, -32001, "Task not found", {"task_id": a2a_task_id})
        
        task_data = TASK_STORE[a2a_task_id]
        # If task is already completed or failed, return as-is
        if task_data.get("status") in ("completed", "failed", "cancelled"):
            return _jsonrpc_success(req_id, {"task": task_data})
        
        # Mark as cancelled (this case won't happen since tasks complete sync)
        task_data["status"] = "cancelled"
        return _jsonrpc_success(req_id, {"task": task_data})

    # -------------------------------------------------------------------------
    # tasks/sendSubscribe - Streaming (not implemented)
    # -------------------------------------------------------------------------
    elif method == "tasks/sendSubscribe":
        return _jsonrpc_error(
            req_id,
            -32001,
            "Streaming not implemented",
            {"info": "This bench executes tasks synchronously. Use tasks/send instead."}
        )

    # -------------------------------------------------------------------------
    # message/send - Simple message-based interaction
    # -------------------------------------------------------------------------
    elif method == "message/send":
        try:
            import json

            # Extract message content (A2A standard format)
            message = params.get("message", {})
            parts = message.get("parts", [])

            # Get text from first part
            if not parts:
                return _jsonrpc_error(req_id, -32602, "Invalid params: message must have parts")

            content_text = parts[0].get("text", "")
            logger.info(f"Extracted content_text: {content_text}")

            # Parse JSON content
            try:
                content = json.loads(content_text) if content_text else {}
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error: {e}")
                return _jsonrpc_error(req_id, -32602, f"Invalid params: content must be valid JSON: {e}")

            # Check if this is AgentBeats EvalRequest format (participants + config)
            if "participants" in content and "config" in content:
                logger.info("Received AgentBeats EvalRequest format")
                participants = content.get("participants", {})
                config = content.get("config", {})
                tasks = config.get("tasks", [])

                # Return A2A standard SendMessageResponse
                # Generate unique IDs
                import uuid
                message_id = str(uuid.uuid4())
                context_id = message.get("contextId") or str(uuid.uuid4())

                result_message = {
                    "kind": "message",
                    "messageId": message_id,
                    "contextId": context_id,
                    "role": "agent",
                    "parts": [{
                        "kind": "text",
                        "text": json.dumps({
                            "status": "acknowledged",
                            "message": "Green agent received evaluation request",
                            "participants": list(participants.keys()),
                            "tasks_count": len(tasks),
                            "tasks": tasks,
                            "note": "Full evaluation logic to be implemented"
                        })
                    }]
                }

                # SendMessageResponse format
                send_message_response = {
                    "message": result_message,
                    "messageId": message_id,
                    "contextId": context_id
                }

                response = _jsonrpc_success(req_id, send_message_response)
                logger.info(f"Returning SendMessageResponse with messageId={message_id}")
                return response

            # Legacy format: single task_id
            task_id = content.get("task_id")
            if not task_id:
                return _jsonrpc_error(req_id, -32602, "Invalid params: Either (participants+config) or task_id is required in content")

            purple_output_subdir = content.get("purple_output_subdir")

            # Run assessment using shared logic
            assess_result = _run_assess_internal(task_id, purple_output_subdir)

            # Return result as message response
            return _jsonrpc_success(req_id, {
                "result": {
                    "type": "object",
                    "content": assess_result,
                }
            })

        except HTTPException as e:
            return _jsonrpc_error(req_id, -32000, f"Assessment failed: {e.detail}")
        except Exception as e:
            return _jsonrpc_error(req_id, -32000, f"Internal error: {str(e)}")

    # -------------------------------------------------------------------------
    # Unknown method
    # -------------------------------------------------------------------------
    else:
        return _jsonrpc_error(req_id, -32601, f"Method not found: {method}")


if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="Green Comtrade Bench Agent")
    parser.add_argument("--host", default=APP_HOST, help="Server host")
    parser.add_argument("--port", type=int, default=APP_PORT, help="Server port")
    parser.add_argument("--card-url", default=None, help="Agent card URL (ignored)")
    
    # Parse known args, ignore unknown for compose compatibility
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"Ignoring unknown args: {unknown}")

    uvicorn.run("src.agent:app", host=args.host, port=args.port, reload=False)
