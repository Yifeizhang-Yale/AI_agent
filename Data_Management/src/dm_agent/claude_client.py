"""Unified Claude client — supports both CLI (claude command) and API backends.

Usage:
    client = create_client(config)
    response_text = client.ask(system_prompt, user_prompt)
"""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ClaudeClient(ABC):
    """Abstract base for Claude backends."""

    @abstractmethod
    def ask(self, system_prompt: str, user_prompt: str) -> str:
        """Send a prompt and return the response text."""
        ...


class CLIClient(ClaudeClient):
    """Call Claude via the `claude` CLI command (Claude Code)."""

    def __init__(self, model: Optional[str] = None, max_tokens: Optional[int] = None):
        self.model = model
        self.max_tokens = max_tokens

    def ask(self, system_prompt: str, user_prompt: str) -> str:
        full_prompt = f"{system_prompt}\n\n---\n\n{user_prompt}"

        # Use stdin pipe for large prompts to avoid ARG_MAX limits
        use_stdin = len(full_prompt) > 100_000

        if use_stdin:
            cmd = ["claude", "-p", "-", "--output-format", "text", "--dangerously-skip-permissions"]
        else:
            cmd = ["claude", "-p", full_prompt, "--output-format", "text", "--dangerously-skip-permissions"]

        if self.model:
            cmd.extend(["--model", self.model])

        logger.debug(f"Running: claude -p <prompt> ({len(full_prompt)} chars, stdin={use_stdin})")

        result = subprocess.run(
            cmd,
            input=full_prompt if use_stdin else None,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip() or f"claude exited with code {result.returncode}"
            raise RuntimeError(f"Claude CLI failed: {error_msg}")

        return result.stdout.strip()


class APIClient(ClaudeClient):
    """Call Claude via the Anthropic Python SDK."""

    def __init__(self, model: str = "claude-sonnet-4-20250514", max_tokens: int = 4096):
        import anthropic
        self.client = anthropic.Anthropic()
        self.model = model
        self.max_tokens = max_tokens

    def ask(self, system_prompt: str, user_prompt: str) -> str:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return response.content[0].text.strip()


def create_client(config_dict: Dict[str, Any]) -> ClaudeClient:
    """Create a Claude client based on config.

    config_dict should have:
        backend: "cli" | "api"   (default: "cli")
        model: optional model name
        max_tokens: optional max tokens
    """
    backend = config_dict.get("backend", "cli")
    model = config_dict.get("model")
    max_tokens = config_dict.get("max_tokens", 4096)

    if backend == "cli":
        logger.info("Using Claude CLI backend")
        return CLIClient(model=model, max_tokens=max_tokens)
    elif backend == "api":
        logger.info("Using Claude API backend")
        return APIClient(model=model or "claude-sonnet-4-20250514", max_tokens=max_tokens)
    else:
        raise ValueError(f"Unknown claude backend: {backend}. Use 'cli' or 'api'.")


def parse_json_response(text: str) -> Dict[str, Any]:
    """Parse JSON from Claude's response, handling markdown code blocks and surrounding text."""
    text = text.strip()
    if not text:
        raise ValueError("Empty response from Claude")

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try extracting from markdown code block
    if "```" in text:
        parts = text.split("```")
        for i in range(1, len(parts), 2):
            block = parts[i]
            if block.startswith("json"):
                block = block[4:]
            block = block.strip()
            try:
                return json.loads(block)
            except json.JSONDecodeError:
                continue

    # Try finding JSON object in the text
    start = text.find("{")
    if start != -1:
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start:i + 1])
                    except json.JSONDecodeError:
                        break

    raise ValueError(f"Could not parse JSON from response: {text[:200]}")
