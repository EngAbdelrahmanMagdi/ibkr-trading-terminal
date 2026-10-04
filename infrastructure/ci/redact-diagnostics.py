"""Remove locally generated secret values from bounded service diagnostics."""

import sys
from pathlib import Path

directory, secrets = map(Path, sys.argv[1:])
values = [file.read_text().strip() for file in secrets.glob('*_password')]
for file in directory.iterdir():
    text = file.read_text()
    for value in values:
        if value:
            text = text.replace(value, '[REDACTED]')
    file.write_text(text)
