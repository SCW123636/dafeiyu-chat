import os

_ROOT = os.path.dirname(os.path.abspath(__file__))


def load_env(path=".env"):
    p = os.path.join(_ROOT, path)
    if not os.path.exists(p):
        return False
    with open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("\"'"))
    return True
