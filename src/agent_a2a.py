"""
Green Comtrade Bench - A2A Server Implementation

Uses A2A Server SDK for proper protocol compliance.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
import requests
import uvicorn
from pydantic import BaseModel

from uuid import uuid4

from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.apps import A2AStarletteApplication
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCard, AgentSkill, AgentCapabilities, TaskState, Part, TextPart, Message, Role
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

        # Get purple agent endpoint
        if "purple-comtrade-baseline-v2" not in participants:
            raise ValueError("purple-comtrade-baseline-v2 participant not found")

        purple_url = participants["purple-comtrade-baseline-v2"]
        logger.info(f"Purple agent endpoint: {purple_url}")

        # Run each task
        all_results = []
        for task_id in tasks:
            await updater.update_status(
                TaskState.working,
                new_agent_text_message(f"Running task: {task_id}")
            )
            logger.info(f"Starting task: {task_id}")

            # Get task definition
            task = get_task(task_id)
            if not task:
                logger.error(f"Unknown task_id: {task_id}")
                all_results.append({
                    "task_id": task_id,
                    "score_total": 0.0,
                    "error": f"Unknown task_id: {task_id}"
                })
                continue

            # Configure mock service
            try:
                logger.info(f"Configuring mock service for {task_id}")
                r = requests.post(
                    f"{self.mock_url}/configure",
                    json={
                        "task_id": task.task_id,
                        "query": task.query,
                        "constraints": task.constraints,
                        "fault_injection": task.fault_injection,
                    },
                    timeout=5,
                )
                r.raise_for_status()
                logger.info(f"Mock service configured for {task_id}")
            except Exception as e:
                logger.error(f"Failed to configure mock service: {e}")
                all_results.append({
                    "task_id": task_id,
                    "score_total": 0.0,
                    "error": f"Failed to configure mock service: {e}"
                })
                continue

            # Call purple agent via A2A
            try:
                logger.info(f"Calling purple agent for {task_id}")
                async with httpx.AsyncClient(timeout=300) as httpx_client:
                    resolver = A2ACardResolver(httpx_client=httpx_client, base_url=purple_url)
                    agent_card = await resolver.get_agent_card()
                    client_config = ClientConfig(httpx_client=httpx_client, streaming=False)
                    factory = ClientFactory(client_config)
                    client = factory.create(agent_card)

                    # Send task request to purple agent
                    task_message = Message(
                        kind="message",
                        role=Role.user,
                        parts=[Part(root=TextPart(kind="text", text=json.dumps({
                            "task_id": task_id,
                            "mock_url": self.mock_url,
                            "output_dir": f"/workspace/purple_output/{task_id}"
                        })))],
                        message_id=uuid4().hex,
                        context_id=None
                    )

                    async for event in client.send_message(task_message):
                        # Just consume events, purple agent will write to file system
                        pass

                    logger.info(f"Purple agent completed {task_id}")

            except Exception as e:
                logger.error(f"Failed to call purple agent: {e}")
                all_results.append({
                    "task_id": task_id,
                    "score_total": 0.0,
                    "error": f"Failed to call purple agent: {e}"
                })
                continue

            # Wait a bit for file system writes to complete
            await asyncio.sleep(2)

            # Read purple output and score
            try:
                out_dir = self.purple_output_root / task_id
                tmp_root = Path("/tmp/purple_output_cache")
                tmp_dir = tmp_root / task_id

                logger.info(f"Staging outputs from {out_dir} to {tmp_dir}")
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir)
                shutil.copytree(out_dir, tmp_dir)

                logger.info(f"Scoring output for {task_id}")
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
                    result = future.result(timeout=self.score_timeout)

                logger.info(f"Task {task_id} scored: {result.total}")
                all_results.append({
                    "task_id": task_id,
                    "score_total": result.total,
                    "score_breakdown": result.breakdown,
                    "errors": result.errors,
                    "details": result.details,
                })

            except Exception as e:
                logger.error(f"Failed to score output: {e}")
                all_results.append({
                    "task_id": task_id,
                    "score_total": 0.0,
                    "error": f"Failed to score output: {e}"
                })
                continue

        # Calculate total score
        total_score = sum(r.get("score_total", 0.0) for r in all_results)
        avg_score = total_score / len(all_results) if all_results else 0.0

        await updater.update_status(
            TaskState.working,
            new_agent_text_message(
                f"Evaluation complete\nTotal score: {total_score:.2f}\nAverage: {avg_score:.2f}"
            )
        )

        # Add result artifact
        summary = f"""Comtrade Benchmark Results
===================================
Tasks completed: {len(all_results)}/{len(tasks)}
Total score: {total_score:.2f}
Average score: {avg_score:.2f}

Per-task results:
"""
        for r in all_results:
            summary += f"\n{r['task_id']}: {r.get('score_total', 0.0):.2f}"
            if 'error' in r:
                summary += f" (ERROR: {r['error']})"

        await updater.add_artifact(
            parts=[
                Part(root=TextPart(text=summary)),
                Part(root=TextPart(text=json.dumps(all_results, indent=2))),
            ],
            name="Result",
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

def create_agent_card(agent_url: str) -> AgentCard:
    """Create agent card for green bench."""
    skill = AgentSkill(
        id="comtrade.benchmark.eval",
        name="evaluate",
        description="Evaluate agent performance on Comtrade API benchmark",
        tags=["benchmark", "evaluation", "a2a"]
    )

    return AgentCard(
        name="green-comtrade-bench",
        version="2.0.0",
        description="Green Agent benchmark for Comtrade API evaluation (A2A)",
        url=agent_url,
        default_input_modes=["text"],
        default_output_modes=["text"],
        capabilities=AgentCapabilities(streaming=False),
        skills=[skill]
    )


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
