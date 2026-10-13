"""python -m workflow_engine starts the standalone management control plane."""
from .management import main


if __name__ == '__main__':
    raise SystemExit(main())
