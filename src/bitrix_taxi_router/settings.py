from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


@dataclass
class Settings:
    app_env: str
    app_host: str
    app_port: int
    db_path: Path
    public_base_url: str | None = None

    @classmethod
    def load(cls, base_dir: Path | None = None) -> "Settings":
        root = base_dir or Path.cwd()
        _load_dotenv(root / ".env")

        db_path = Path(os.getenv("DB_PATH", "./data/bitrix_taxi_router.sqlite3"))
        app_port_raw = os.getenv("APP_PORT")
        port_raw = os.getenv("PORT")
        return cls(
            app_env=os.getenv("APP_ENV", "dev"),
            app_host=os.getenv("APP_HOST", "127.0.0.1"),
            app_port=int(app_port_raw or port_raw or 8000),
            db_path=(root / db_path).resolve() if not db_path.is_absolute() else db_path,
            public_base_url=(os.getenv("APP_PUBLIC_BASE_URL") or "").strip() or None,
        )

    def ensure_runtime_dirs(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
