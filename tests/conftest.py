import os

# Tests default to the local Compose stack; CI overrides these via the environment.
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://hopper:hopper@127.0.0.1:5432/hopper")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")
