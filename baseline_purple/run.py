"""
Baseline Purple Agent - CLI Entrypoint

Usage:
    python3 -m baseline_purple.run --task-id T1_single_page
    python3 -m baseline_purple.run --task-id T7_totals_trap --mock-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from baseline_purple.purple_agent import PurpleAgent


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Baseline Purple Agent for green-comtrade-bench",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--task-id",
        default="T1_single_page",
        help="Task ID to run (default: T1_single_page)",
    )
    
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: _purple_output/<task_id>/)",
    )
    
    parser.add_argument(
        "--mock-url",
        default="http://localhost:8000",
        help="Mock service URL (default: http://localhost:8000)",
    )
    
    args = parser.parse_args()
    
    # Set default output directory
    if args.output_dir is None:
        args.output_dir = f"_purple_output/{args.task_id}"
    
    # Run agent
    agent = PurpleAgent()
    success = agent.run(
        task_id=args.task_id,
        output_dir=args.output_dir,
        mock_url=args.mock_url,
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
