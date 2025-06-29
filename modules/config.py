from dotenv import dotenv_values  # type: ignore
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
env = dotenv_values(BASE_DIR / ".env")

OFFERS_URL = env.get(
    "OFFERS_URL",
    "",
)

DB_NAME = env.get(
    "DB_NAME",
    "job_board.db",
)

DB_PATH = (
    Path(
        env.get(
            "DB_PATH",
            BASE_DIR / "data",
        )
    )
    / DB_NAME
)
