import asyncio
import sys

import pytest
from httpx import AsyncClient

from main import app, config
from main.libs.log import get_logger


logger = get_logger(__name__)

if config.ENVIRONMENT != "test":
    logger.error('Tests must be run with "ENVIRONMENT=test"')
    sys.exit(1)


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()

    yield loop

    loop.close()


@pytest.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client
