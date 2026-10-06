from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
from typing import Optional

logger = logging.getLogger(__name__)


class ArioFlowRunner:
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

    # --- constructors --------------------------------------------------------

    @classmethod
    def from_laas_deployment(
        cls,
        current_env: str = "in",
        *,
        base_template: str = "/applis/19623-laas-{env}",
        build_match: str = "ario-api",
        **kwargs,
    ) -> "ArioFlowRunner":
        """Reproduce the pre-launch shell setup, then return a ready runner.

        Equivalent to:
            set -a; source ".../etc/laas_api/env"; set +a
            DEPLOYED_VER=$(ls -Art ".../bin" | grep ario-api | tail -n 1)
            source ".../bin/$DEPLOYED_VER/bin/activate"
        """
        base = base_template.format(env=current_env)

        # 1. source the env file (set -a makes every var exported) and capture it
        env_file = f"{base}/etc/laas_api/env"
        merged_env = cls._source_env(env_file, current_env)

        # 2. newest deployed build, same as `ls -Art <bin> | grep ario-api | tail -n 1`
        bin_dir = f"{base}/bin"
        deployed_ver = cls._latest_build(bin_dir, build_match)
        venv = f"{bin_dir}/{deployed_ver}"

        # 3. instead of sourcing activate, run the venv's python directly and
        #    set the two vars activate would have set (for any child processes)
        merged_env["VIRTUAL_ENV"] = venv
        merged_env["PATH"] = f"{venv}/bin:" + merged_env.get("PATH", "")
        merged_env["CURRENT_ENV"] = current_env

        logger.debug("Using deployed build %s", deployed_ver)
        return cls(python=f"{venv}/bin/python", env=merged_env, **kwargs)

    @staticmethod
    def _source_env(env_file: str, current_env: str) -> dict[str, str]:
        # Let bash do the sourcing so quoting/expansions behave exactly as in your
        # shell, then dump the resulting environment NUL-delimited and parse it.
        cmd = ["bash", "-c", f"set -a && source {shlex.quote(env_file)} && env -0"]
        prep = {**os.environ, "CURRENT_ENV": current_env}
        out = subprocess.run(
            cmd, capture_output=True, text=True, check=True, env=prep
        ).stdout
        env: dict[str, str] = {}
        for entry in out.split("\0"):
            if entry:
                key, _, value = entry.partition("=")
                env[key] = value
        return env

    @staticmethod
    def _latest_build(bin_dir: str, needle: str) -> str:
        matches = [name for name in os.listdir(bin_dir) if needle in name]
        if not matches:
            raise FileNotFoundError(f"no {needle!r} build found in {bin_dir}")
        matches.sort(key=lambda n: os.path.getmtime(os.path.join(bin_dir, n)))
        return matches[-1]  # newest by mtime

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