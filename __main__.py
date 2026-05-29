"""Entry point for ``python -m agentfleetmem``."""

import sys

# Force UTF-8 stdout/stderr so the consolidated report renders on Windows
# consoles whose default codec is cp1252 / cp1255 / etc. Without this,
# em-dashes and similar punctuation crash the run with a UnicodeEncodeError
# at the point of typer.echo / typer.secho.
for _stream in (sys.stdout, sys.stderr):
    reconfigure = getattr(_stream, "reconfigure", None)
    if reconfigure is not None:
        reconfigure(encoding="utf-8", errors="replace")

from agentfleetmem.cli import app

if __name__ == "__main__":
    app()
