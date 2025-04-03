import asyncio
from typing import Any

import httpx

from main.libs.log import get_logger


logger = get_logger(__name__)


class CodaClient:
    """Client for interacting with the Coda API"""

    BASE_URL = "https://coda.io/apis/v1"

    def __init__(self, api_token: str):
        """Initialize the Coda API client"""
        self.api_token = api_token
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def _log_request(
        self,
        level: str,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Log request details at the specified log level"""
        log_method = getattr(logger, level)
        log_method("Request details:")
        log_method(f"  Method: {method}")
        log_method(f"  URL: {url}")
        log_method(f"  Params: {params}")
        log_method(f"  Data: {data}")

    async def make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        retry_count: int = 3,
        retry_delay: int = 1,
        timeout: int = 300,  # 5 minutes in seconds
    ) -> dict[str, Any]:
        """Make an async request to the Coda API with retry logic for rate limits"""
        url = f"{self.BASE_URL}{endpoint}"
        logger.info(f"Making {method} request to: {url}")

        for attempt in range(1, retry_count + 1):
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    params=params,
                    json=data,
                    timeout=timeout,  # Add timeout parameter
                )

            # Log response status for debugging
            logger.info(f"Response status: {response.status_code}")

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", retry_delay))
                logger.warning(
                    f"Rate limited. Waiting {retry_after} seconds... "
                    f"(Attempt {attempt}/{retry_count})",
                )
                await asyncio.sleep(retry_after)
                continue

            # Log detailed error information
            if response.status_code >= 400:
                logger.error(f"Error response: {response.text}")
                self._log_request("error", method, url, params, data)

            if response.status_code >= 500:
                retry_after = 5
                logger.warning(
                    f"Server error. Waiting {retry_after} seconds... "
                    f"(Attempt {attempt}/{retry_count})",
                )
                await asyncio.sleep(retry_after)
                continue

            response.raise_for_status()
            return response.json()

        # If we've exhausted all retries
        logger.error(f"Request failed after {retry_count} attempts")
        self._log_request("error", method, url, params, data)
        raise Exception(f"Request failed after {retry_count} attempts")

    async def get_user_info(self) -> dict[str, Any]:
        """Get information about the authenticated user"""
        return await self.make_request("GET", "/whoami")

    async def get_doc_info(self, doc_id: str) -> dict[str, Any]:
        """Get information about a document"""
        return await self.make_request("GET", f"/docs/{doc_id}")

    async def get_table_info(self, doc_id: str, table_id: str) -> dict[str, Any]:
        """Get information about a table"""
        return await self.make_request("GET", f"/docs/{doc_id}/tables/{table_id}")

    async def get_tables(self, doc_id: str) -> list[dict[str, Any]]:
        """Get all tables in a document"""
        response = await self.make_request("GET", f"/docs/{doc_id}/tables")
        return response.get("items", [])

    async def get_table_schema(
        self,
        doc_id: str,
        table_id: str,
    ) -> list[dict[str, Any]]:
        """Get the schema (columns) of a table"""
        response = await self.make_request(
            "GET",
            f"/docs/{doc_id}/tables/{table_id}/columns",
            params={"visibleOnly": "false"},
        )
        return response.get("items", [])

    async def get_table_data(self, doc_id: str, table_id: str) -> list[dict[str, Any]]:
        """Get all rows from a table"""
        all_rows = []
        next_page_token = None

        loop_limit = 100
        loop_count = 0
        while True:
            params = {
                "limit": 100,
                "useColumnNames": True,
            }

            if next_page_token:
                params["pageToken"] = next_page_token

            response = await self.make_request(
                "GET",
                f"/docs/{doc_id}/tables/{table_id}/rows",
                params=params,
            )
            all_rows.extend(response.get("items", []))

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

            loop_count += 1
            if loop_count > loop_limit:
                break

        return all_rows

    async def create_table(
        self,
        doc_id: str,
        table_name: str,
        schema: list[dict[str, Any]],
    ) -> str:
        """Create a new table in a document"""
        table_data = {
            "name": table_name,
            "columns": [{"name": col["name"], "type": col["type"]} for col in schema],
        }

        response = await self.make_request(
            "POST",
            f"/docs/{doc_id}/tables",
            data=table_data,
        )
        return response["id"]

    async def upsert_rows(
        self,
        doc_id: str,
        table_id: str,
        rows: list[dict[str, Any]],
        key_columns: list[str],
    ) -> None:
        """Add or update rows in a table"""
        if not rows:
            logger.info("No rows to add")
            return

        # Add rows in batches to avoid API limits
        BATCH_SIZE = 40
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]

            # Make a copy of each row's values and remove any 'id' field
            for row in batch:
                if "id" in row["values"]:
                    del row["values"]["id"]

            row_data = {
                "rows": [
                    {
                        "cells": [
                            {"column": col, "value": val}
                            for col, val in row["values"].items()
                        ],
                    }
                    for row in batch
                ],
                "keyColumns": key_columns,
            }

            await self.make_request(
                "POST",
                f"/docs/{doc_id}/tables/{table_id}/rows",
                data=row_data,
            )
            logger.info(f"Added batch of {len(batch)} rows ({i+1} to {i+len(batch)})")

        logger.info(f"Successfully added {len(rows)} rows to the destination table")

    async def delete_rows(self, doc_id: str, table_id: str, row_ids: list[str]) -> None:
        """Delete rows from a table"""
        if not row_ids:
            return

        # Delete rows in batches
        BATCH_SIZE = 40
        for i in range(0, len(row_ids), BATCH_SIZE):
            batch = row_ids[i : i + BATCH_SIZE]
            await self.make_request(
                "DELETE",
                f"/docs/{doc_id}/tables/{table_id}/rows",
                data={"rowIds": batch},
            )
            logger.info(f"Deleted batch of {len(batch)} rows ({i+1} to {i+len(batch)})")
