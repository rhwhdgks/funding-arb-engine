import os
import runpy
import sys
from pathlib import Path

from dotenv import load_dotenv


TEST_SERVER_ROOT = Path(__file__).resolve().parent
REPO_ROOT = TEST_SERVER_ROOT.parent
ENV_FILE = TEST_SERVER_ROOT / ".env"


def main() -> None:
    load_dotenv(ENV_FILE, override=True)
    os.environ.setdefault("STRATEGY_ENV_FILE", str(ENV_FILE))
    os.environ.setdefault("STRATEGY_TEST_ENV", "1")

    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    os.chdir(REPO_ROOT)
    runpy.run_path(str(REPO_ROOT / "main.py"), run_name="__main__")


if __name__ == "__main__":
    main()
