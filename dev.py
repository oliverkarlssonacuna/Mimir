"""
Interactive console for testing the agent locally.
Run: python dev.py
"""

import sys
import os

# Make sure src/ is on the path when running from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from config import Config
from bq_client import BQClient
from agent import Agent

def main():
    print("=== BQ Bot – dev console ===")
    print(f"Project : {Config.GCP_PROJECT_ID}")
    print(f"Model   : {Config.GEMINI_MODEL}")
    print(f"Table   : {Config.BQ_TABLE}")
    print("Type your question (or 'quit' to exit)\n")

    bq = BQClient(project_id=Config.GCP_PROJECT_ID, max_rows=Config.MAX_QUERY_ROWS)
    agent = Agent(config=Config, bq_client=bq)

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break

        print("Thinking...\n")
        response = agent.ask(question)

        print(f"Bot: {response.text}")
        if response.chart_path:
            print(f"\n[Chart saved to: {response.chart_path}]")
        print()


if __name__ == "__main__":
    main()
