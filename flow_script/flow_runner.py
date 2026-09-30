from __future__ import annotations

import logging
import subprocess
import sys
from typing import Optional

logger = logging.getLogger(__name__)


class FlowRunner:
    """Runs the ario_api CLI as a subprocess, so you can move/delete flows
    from your script without retyping the command each time.

    Equivalent to:
        python -m ario_api.cli move-flow --collect-code-name test --cluster cc2 --cleanup

    Example:
        runner = ArioFlowRunner()
        runner.move_flows("test", cluster="cc2", cleanup=True)
        runner.delete_flows("test", cluster="cc2")
    """

    def __init__(
        self,
        *,
        python: str = sys.executable,
        module: str = "ario_api.cli",
        cwd: Optional[str] = None,
        env: Optional[dict[str, str]] = None,
        check: bool = True,
        capture_output: bool = True,
        timeout: Optional[float] = None,
    ) -> None:
        self.python = python          # same interpreter/venv by default
        self.module = module
        self.cwd = cwd
        self.env = env
        self.check = check            # raise on non-zero exit
        self.capture_output = capture_output
        self.timeout = timeout

    # --- public API ---------------------------------------------------------

    def move_flows(
        self,
        collect_code_name: str,
        cluster: str,
        *,
        cleanup: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        args = [
            "move-flow",
            "--collect-code-name", collect_code_name,
            "--cluster", cluster,
        ]
        if cleanup:
            args.append("--cleanup")
        return self._run(args)

    def delete_flows(
        self,
        collect_code_name: str,
        cluster: str,
        *,
        cleanup: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        args = [
            "delete-flow",
            "--collect-code-name", collect_code_name,
            "--cluster", cluster,
        ]
        if cleanup:
            args.append("--cleanup")
        return self._run(args)

    # --- internals ----------------------------------------------------------

    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        cmd = [self.python, "-m", self.module, *args]
        logger.debug("Running: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            cwd=self.cwd,
            env=self.env,
            check=self.check,
            capture_output=self.capture_output,
            text=True,
            timeout=self.timeout,
        )
        if self.capture_output and result.stdout:
            logger.debug("stdout: %s", result.stdout.strip())
        return result