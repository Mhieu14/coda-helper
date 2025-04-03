from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security.api_key import APIKeyHeader

from main._config import config
from main.schemas.coda_schemas import MergeResult
from main.services.table_merger import TableMerger


router = APIRouter(prefix="/coda", tags=["coda"])

# API Key authentication
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# Rate limiting storage - stores last request time for each API key
rate_limit_store: dict[str, datetime] = {}


async def get_api_key(api_key_header: str = Depends(api_key_header)):
    if api_key_header is None:
        raise HTTPException(status_code=401, detail="API Key header missing")

    if api_key_header != config.API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")

    return api_key_header


async def check_rate_limit(request: Request, api_key: str = Depends(get_api_key)):
    # Get the current time
    current_time = datetime.now()

    # Check if this API key has made a request before
    if api_key in rate_limit_store:
        last_request_time = rate_limit_store[api_key]
        # Check if the last request was less than 1 minute ago
        if current_time - last_request_time < timedelta(minutes=1):
            # Calculate remaining time until next allowed request
            wait_time = 60 - (current_time - last_request_time).seconds
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Try again in {wait_time} seconds.",
            )

    # Update the last request time for this API key
    rate_limit_store[api_key] = current_time
    return api_key


@router.post("/merge", response_model=MergeResult)
async def merge_tables(api_key: str = Depends(check_rate_limit)) -> MergeResult:
    merger = TableMerger(
        api_token=config.CODA_API_TOKEN,
        destination_doc_id=config.MERGE_TABLE_CONFIG.destination_doc_id,
        destination_table_id=config.MERGE_TABLE_CONFIG.destination_table_id,
        source_tables=config.MERGE_TABLE_CONFIG.source_tables,
    )

    return await merger.merge_tables()
