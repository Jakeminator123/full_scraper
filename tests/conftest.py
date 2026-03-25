import sys
from pathlib import Path

# Package root (parent of tests/) so `import db` resolves.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
