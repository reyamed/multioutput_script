from __future__ import annotations

import logging
import shlex
import subprocess
import sys
from typing import Optional

logger = logging.getLogger(__name__)


class ArioFlowRunner:
    """Runs the ario_api CLI to move/delete flows.

    Two modes:
      * plain: runs `<python> -m ario_api.cli ...` directly.
      * laas:  runs the full pre-launch setup (source env, pick newest build,
               activate venv) + the CLI in one shell, optionally as another OS
               user (e.g. gunicorn), so the deployment folder is readable.

    Example (laas, as gunicorn):
        runner = ArioFlowRunner.from_laas_deployment("in", run_as="gunicorn")
        runner.move_flows("nunhectare", cluster="laas-ccl", cleanup=False)
        runner.delete_flows("nunhectare", cluster="laas-ccl")
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
        run_as: Optional[str] = None,     # OS user to sudo to, e.g. "gunicorn"
        prelude: Optional[str] = None,    # shell run before the CLI (source env, activate)
    ) -> None:
        self.python = python
        self.module = module
        self.cwd = cwd
        self.env = env
        self.check = check
        self.capture_output = capture_output
        self.timeout = timeout
        self.run_as = run_as
        self.prelude = prelude

    # --- constructors --------------------------------------------------------

    @classmethod
    def from_laas_deployment(
        cls,
        current_env: str = "in",
        *,
        run_as: Optional[str] = "gunicorn",
        base_template: str = "/applis/19623-laas-{env}",
        build_match: str = "ario-api",
        **kwargs,
    ) -> "ArioFlowRunner":
        """Build a runner that reproduces the shell setup inside a single shell,
        run as `run_as` (set run_as=None to run as the current user)."""
        base = base_template.format(env=current_env)
        # This whole block runs in the target user's shell, so folder access and
        # the sourced environment are all under that user.
        prelude = (
            "set -a\n"
            f'source "{base}/etc/laas_api/env"\n'
            "set +a\n"
            f'DEPLOYED_VER=$(ls -Art "{base}/bin" | grep {shlex.quote(build_match)} | tail -n 1)\n'
            f'source "{base}/bin/$DEPLOYED_VER/bin/activate"'
        )
        # venv is activated by the prelude, so `python` resolves to the venv's.
        return cls(python="python", run_as=run_as, prelude=prelude, **kwargs)

    # --- public API ----------------------------------------------------------

    def move_flows(
        self,
        collect_code_name: str,
        cluster: str,
        *,
        cleanup: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        args = ["move-flow", "--collect-code-name", collect_code_name, "--cluster", cluster]
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
        args = ["delete-flow", "--collect-code-name", collect_code_name, "--cluster", cluster]
        if cleanup:
            args.append("--cleanup")
        return self._run(args)

    # --- internals -----------------------------------------------------------

    def _build_cmd(self, args: list[str]) -> list[str]:
        if self.prelude:
            # Run prep + CLI in one shell. Args are passed positionally ("$@"),
            # so nothing from the flow/cluster names is interpreted by the shell.
            script = f'{self.prelude}\nexec {shlex.quote(self.python)} -m {shlex.quote(self.module)} "$@"'
            cmd = ["bash", "-c", script, "bash", *args]
        else:
            cmd = [self.python, "-m", self.module, *args]
        if self.run_as:
            # -n: never prompt for a password; fail fast if sudo isn't allowed.
            cmd = ["sudo", "-n", "-u", self.run_as, *cmd]
        return cmd

    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        cmd = self._build_cmd(args)
        logger.debug("Running: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            cwd=self.cwd,
            env=self.env,
            check=False,  # handled below so we can surface the CLI's output
            capture_output=self.capture_output,
            text=True,
            timeout=self.timeout,
        )
        if result.returncode != 0:
            if self.capture_output:
                logger.error(
                    "command failed (exit %s): %s\n--- stdout ---\n%s\n--- stderr ---\n%s",
                    result.returncode,
                    " ".join(cmd),
                    (result.stdout or "").strip(),
                    (result.stderr or "").strip(),
                )
            if self.check:
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, result.stdout, result.stderr
                )
        elif self.capture_output and result.stdout:
            logger.debug("stdout: %s", result.stdout.strip())
        return result