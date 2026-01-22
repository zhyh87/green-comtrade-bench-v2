"""
Green Comtrade Bench - A2A Server Implementation

Uses A2A Server SDK for proper protocol compliance.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import uvicorn
from pydantic import BaseModel

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.apps import A2AStarletteApplication
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import TaskState
from a2a.utils import new_agent_text_message

from .tasks import get_task
from .judge import score_output

# Configure logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("green-agent")

# Environment variables
MOCK_URL = os.environ.get("MOCK_URL", "http://mock-comtrade:8000")
PURPLE_OUTPUT_ROOT = Path(os.environ.get("PURPLE_OUTPUT_ROOT", "/workspace/purple_output"))
SCORE_TIMEOUT = float(os.environ.get("SCORE_TIMEOUT", "8"))


# ============================================================================
# Models
# ============================================================================

class EvalRequest(BaseModel):
    """AgentBeats EvalRequest format."""
    participants: Dict[str, str]  # role -> endpoint mapping
    config: Dict[str, Any]


class EvalResult(BaseModel):
    """Evaluation result."""
    status: str
    message: str
    details: Dict[str, Any]


# ============================================================================
# Green Agent Implementation
# ============================================================================

class GreenComtradeBenchJudge:
    """Green agent for Comtrade benchmark evaluation."""

    def __init__(self):
        self.mock_url = MOCK_URL
        self.purple_output_root = PURPLE_OUTPUT_ROOT
        self.score_timeout = SCORE_TIMEOUT

    async def run_eval(self, request: EvalRequest, updater: TaskUpdater) -> None:
        """
        Execute benchmark evaluation.

        Args:
            request: EvalRequest with participants and config
            updater: TaskUpdater for status updates
        """
        participants = request.participants
        config = request.config
        tasks = config.get("tasks", [])

        logger.info(f"Starting evaluation with {len(tasks)} tasks")
        logger.info(f"Participants: {list(participants.keys())}")

        await updater.update_status(
            TaskState.working,
            new_agent_text_message(
                f"Starting benchmark evaluation\nTasks: {tasks}\nParticipants: {list(participants.keys())}"
            )
        )

        # TODO: Implement actual task execution
        # For now, return acknowledgment
        results = {
            "status": "acknowledged",
            "tasks": tasks,
            "participants": list(participants.keys()),
            "note": "Full evaluation implementation in progress"
        }

        await updater.update_status(
            TaskState.working,
            new_agent_text_message(f"Evaluation complete: {json.dumps(results, indent=2)}")
        )

    def validate_request(self, request: EvalRequest) -> tuple[bool, str]:
        """Validate evaluation request."""
        if not request.participants:
            return False, "No participants provided"

        if not request.config:
            return False, "No config provided"

        tasks = request.config.get("tasks", [])
        if not tasks:
            return False, "No tasks specified in config"

        return True, "Valid request"


# ============================================================================
# A2A Executor
# ============================================================================

class GreenExecutor(AgentExecutor):
    """A2A AgentExecutor for Green Comtrade Bench."""

    def __init__(self, agent: GreenComtradeBenchJudge):
        self.agent = agent

    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        """Execute evaluation request."""
        # Get user input (message content)
        request_text = context.get_user_input()

        logger.info(f"Received request: {request_text[:200]}...")

        # Parse as EvalRequest
        try:
            request_data = json.loads(request_text)
            eval_request = EvalRequest(**request_data)
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Failed to parse EvalRequest: {e}")
            raise ValueError(f"Invalid EvalRequest format: {e}")

        # Validate request
        is_valid, msg = self.agent.validate_request(eval_request)
        if not is_valid:
            raise ValueError(f"Invalid request: {msg}")

        # Create task
        from a2a.utils import new_task
        msg_obj = context.message
        if msg_obj:
            task = new_task(msg_obj)
            await event_queue.enqueue_event(task)
        else:
            raise ValueError("Missing message in context")

        # Create task updater
        updater = TaskUpdater(event_queue, task.id, task.context_id)

        # Run evaluation
        try:
            await self.agent.run_eval(eval_request, updater)
            await updater.complete()
        except Exception as e:
            logger.error(f"Evaluation error: {e}")
            await updater.failed(new_agent_text_message(f"Evaluation failed: {e}"))
            raise

    async def cancel(
        self, request: RequestContext, event_queue: EventQueue
    ) -> None:
        """Cancel is not supported."""
        from a2a.types import UnsupportedOperationError
        from a2a.utils.errors import ServerError
        raise ServerError(error=UnsupportedOperationError())


# ============================================================================
# Agent Card
# ============================================================================

def create_agent_card(agent_url: str) -> dict:
    """Create agent card for green bench."""
    return {
        "name": "green-comtrade-bench",
        "version": "2.0.0",
        "description": "Green Agent benchmark for Comtrade API evaluation (A2A)",
        "url": agent_url,
        "endpoints": {
            "rpc": "/a2a/rpc",
            "health": "/healthz"
        },
        "capabilities": {
            "streaming": False,
            "pushNotifications": False
        },
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "skills": [
            {
                "id": "comtrade.benchmark.eval",
                "name": "evaluate",
                "description": "Evaluate agent performance on Comtrade API benchmark",
                "tags": ["benchmark", "evaluation", "a2a"]
            }
        ]
    }


# ============================================================================
# Main
# ============================================================================

async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Green Comtrade Bench (A2A)")
    parser.add_argument("--host", default="0.0.0.0", help="Server host")
    parser.add_argument("--port", type=int, default=9009, help="Server port")
    parser.add_argument("--card-url", default=None, help="External agent URL")

    args, unknown = parser.parse_known_args()
    if unknown:
        logger.info(f"Ignoring unknown args: {unknown}")

    # Determine agent URL
    agent_url = args.card_url or f"http://{args.host}:{args.port}"

    # Create agent and executor
    agent = GreenComtradeBenchJudge()
    executor = GreenExecutor(agent)

    # Create agent card
    agent_card = create_agent_card(agent_url)

    # Create request handler
    request_handler = DefaultRequestHandler(
        agent_executor=executor,
        task_store=InMemoryTaskStore(),
    )

    # Create A2A server
    a2a_server = A2AStarletteApplication(
        agent_card=agent_card,
        http_handler=request_handler,
    )

    # Build app
    app = a2a_server.build()

    logger.info(f"Starting Green Comtrade Bench on {args.host}:{args.port}")
    logger.info(f"Agent URL: {agent_url}")

    # Run server
    config = uvicorn.Config(app, host=args.host, port=args.port)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
