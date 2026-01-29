import os
from pathlib import Path

print("CWD:", os.getcwd())
print("Files here:", [p.name for p in Path.cwd().iterdir()][:10])

import sys
from pathlib import Path

p = Path.cwd().resolve()
while p != p.parent and not (p / "src").exists():
    p = p.parent

if not (p / "src").exists():
    raise FileNotFoundError("Couldn't find a 'src' folder in any parent directory. Move the notebook into the repo or open Jupyter from repo root.")

sys.path.insert(0, str(p))  # insert at FRONT (important)
print("Added to sys.path:", p)

import src
print("✅ src import works!")
