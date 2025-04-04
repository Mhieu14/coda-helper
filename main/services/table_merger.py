import hashlib
import json
import re
from typing import Any

from main.libs.log import get_logger
from main.schemas.coda_schemas import MergeResult
from main.schemas.config import SourceTable
from main.services.coda_client import CodaClient


logger = get_logger(__name__)


class TableMerger:
    """Service for merging multiple Coda tables into one"""

    def __init__(
        self,
        api_token: str,
        destination_doc_id: str,
        destination_table_id: str,
        source_tables: list[SourceTable],
    ):
        """Initialize the table merger with configuration"""
        self.coda_client = CodaClient(api_token=api_token)
        self.destination_doc_id = destination_doc_id
        self.destination_table_id = destination_table_id
        self.source_tables = [
            (src.doc_id, src.table_id, src.project) for src in source_tables
        ]
        # Initialize column mappings (source column name -> destination column name)
        self.column_mappings: dict[str, str] = {}

    async def verify_api_access(self) -> bool:
        """Verify API token and document access"""
        # Test API token by getting user info
        user_info = await self.coda_client.get_user_info()
        logger.info(f"API authenticated as: {user_info.get('name', 'Unknown user')}")

        # Test document access
        doc_info = await self.coda_client.get_doc_info(self.destination_doc_id)
        logger.info(
            f"Successfully accessed document: {doc_info.get('name')}",
        )

        # Test source document access
        for i, (doc_id, _, _) in enumerate(self.source_tables, 1):
            doc_info = await self.coda_client.get_doc_info(doc_id)
            logger.info(
                f"Successfully accessed source document {i}: {doc_info.get('name')}",
            )

        return True

    # ... existing code ...

    async def detect_and_handle_duplicates(
        self,
        existing_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Detect and handle duplicate unique keys in the destination table"""
        logger.info("Checking for duplicate unique keys in destination table...")

        # Create a dictionary to track occurrences of each unique key
        key_counts: dict[str, list[dict[str, Any]]] = {}
        duplicate_rows: list[dict[str, Any]] = []

        for row in existing_rows:
            if unique_key := row["values"].get("unique_key"):
                if unique_key in key_counts:
                    key_counts[unique_key].append(row)
                    duplicate_rows.append(row)
                else:
                    key_counts[unique_key] = [row]

        # Filter to only keys with multiple rows
        if duplicates := {k: v for k, v in key_counts.items() if len(v) > 1}:
            logger.info(
                f"Found {len(duplicates)} unique keys with duplicates, "
                f"affecting {len(duplicate_rows)} rows",
            )

            # Delete all but the LAST occurrence of each duplicate key
            rows_to_delete = []
            for rows in duplicates.values():
                # Keep the last row, delete the rest
                rows_to_delete.extend(rows[:-1])

            if rows_to_delete:
                # Delete rows in batches
                row_ids = [row["id"] for row in rows_to_delete]
                await self.coda_client.delete_rows(
                    self.destination_doc_id,
                    self.destination_table_id,
                    row_ids,
                )
                logger.info(
                    f"Deleted {len(rows_to_delete)} duplicate rows "
                    "(keeping only the most recent occurrence)",
                )

                # Remove the deleted rows from the existing_rows list
                deleted_ids = {row["id"] for row in rows_to_delete}
                existing_rows = [
                    row for row in existing_rows if row["id"] not in deleted_ids
                ]

                logger.info(
                    "Updated existing_rows list "
                    f"now contains {len(existing_rows)} rows",
                )
        else:
            logger.info("No duplicate unique keys found in destination table")

        return existing_rows

    @staticmethod
    def get_row_hash(row_values: dict[str, Any]) -> str:
        """Generate a hash for a row based on its values to detect changes"""
        # Convert the row values to a string and hash it
        row_str = json.dumps(row_values, sort_keys=True)
        return hashlib.md5(row_str.encode()).hexdigest()  # noqa: S324

    def map_column_names(
        self,
        row_values: dict[str, Any],
        destination_columns: set[str],
    ) -> dict[str, Any]:
        """
        Map source column names to destination column names using both explicit mappings
        and automatic fuzzy matching based on case and special characters.
        """
        mapped_values = {}

        def normalize_column_name(name: str) -> str:
            # Remove special characters and normalize spaces
            return re.sub(
                r"\s+",
                " ",
                name.lower().replace("/", "").replace("(", "").replace(")", ""),
            ).strip()

        for col_name, value in row_values.items():
            # Case 1: If there's an explicit mapping, use it
            if col_name in self.column_mappings:
                mapped_values[self.column_mappings[col_name]] = value
                continue

            # Case 2: If the column name already exists in destination, use as is
            if col_name in destination_columns:
                mapped_values[col_name] = value
                continue

            # Case 3: Try to find a match by normalizing names
            normalized_source = normalize_column_name(col_name)
            found_match = False

            for dest_col in destination_columns:
                normalized_dest = normalize_column_name(dest_col)
                if normalized_source == normalized_dest:
                    mapped_values[dest_col] = value
                    found_match = True
                    break

            # Case 4: If no match found, keep the original name
            if not found_match:
                mapped_values[col_name] = value

        return mapped_values

    @staticmethod
    def add_source_info_to_rows(
        rows: list[dict[str, Any]],
        source_id: str,
        project_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Add source information and hash to each row"""
        for row in rows:
            # Add source identifier to the row
            row["source_id"] = source_id
            # Add project name if provided
            if project_name:
                row["values"]["Project"] = project_name
            # Add a unique key if it doesn't exist (using row ID as fallback)
            if "unique_key" not in row.get("values", {}):
                row_id = row.get("id", "")
                # Try to create a unique key from the row data or use the row ID
                row["values"]["unique_key"] = f"{source_id}_{row_id}"
            # Add a hash of the row values to detect changes
            row["hash"] = TableMerger.get_row_hash(row["values"])
        return rows

    async def merge_tables(self) -> MergeResult:  # noqa: C901
        """Main function to merge multiple tables into one"""
        logger.info(
            f"Starting merge process for {len(self.source_tables)} source tables...",
        )

        # Verify API access first
        await self.verify_api_access()

        if not self.source_tables:
            raise ValueError("No source tables specified. Check your configuration.")

        # Get schema from the first table
        first_doc_id, first_table_id, _ = self.source_tables[0]
        schema = await self.coda_client.get_table_schema(first_doc_id, first_table_id)

        # Make sure we have unique_key and hash columns in the schema
        has_unique_key = any(col["name"] == "unique_key" for col in schema)
        has_hash = any(col["name"] == "row_hash" for col in schema)
        has_project = any(col["name"] == "Project" for col in schema)

        if not has_unique_key:
            schema.append({"name": "unique_key", "type": "text"})
        if not has_hash:
            schema.append({"name": "row_hash", "type": "text"})
        if not has_project:
            schema.append({"name": "Project", "type": "text"})

        # Verify the table exists
        table_info = await self.coda_client.get_table_info(
            self.destination_doc_id,
            self.destination_table_id,
        )
        logger.info(
            f"Using existing destination table: {table_info.get('name')} "
            f"(ID: {self.destination_table_id})",
        )

        # Get the destination table schema
        destination_schema = await self.coda_client.get_table_schema(
            self.destination_doc_id,
            self.destination_table_id,
        )
        destination_column_names = {col["name"] for col in destination_schema}
        logger.info(f"Destination table has {len(destination_column_names)} columns")

        # 1. Get existing rows from destination
        logger.info("Fetching existing rows from destination table...")
        existing_rows = await self.coda_client.get_table_data(
            self.destination_doc_id,
            self.destination_table_id,
        )
        logger.info(f"Found {len(existing_rows)} existing rows in destination table")

        # Check for and handle duplicate unique keys
        existing_rows = await self.detect_and_handle_duplicates(existing_rows)

        # Create a dictionary of existing rows by unique key
        existing_by_key: dict[str, dict[str, Any]] = {}
        for row in existing_rows:
            if unique_key := row["values"].get("unique_key"):
                existing_by_key[unique_key] = row

        # 2. Fetch all source rows with their hashes
        all_source_rows: list[dict[str, Any]] = []
        source_keys: set[str] = set()  # Track all keys from source tables

        for i, (doc_id, table_id, project_name) in enumerate(self.source_tables, 1):
            logger.info(
                f"Fetching data from source {i}: Doc {doc_id}, Table {table_id}...",
            )
            rows = await self.coda_client.get_table_data(doc_id, table_id)
            source_id = f"src_{doc_id}_{table_id}"

            # Use the project name from config or default
            project = project_name or f"Project {i}"

            # Apply column name mapping before adding source info
            for row in rows:
                row["values"] = self.map_column_names(
                    row["values"],
                    destination_column_names,
                )

            processed_rows = self.add_source_info_to_rows(rows, source_id, project)

            # Filter out columns that don't exist in the destination table
            for row in processed_rows:
                filtered_values: dict[str, Any] = {
                    col_name: value
                    for col_name, value in row["values"].items()
                    if col_name in destination_column_names
                    or col_name
                    in [
                        "unique_key",
                        "row_hash",
                    ]
                }
                row["values"] = filtered_values

            all_source_rows.extend(processed_rows)

            # Add keys to the tracking set
            for row in processed_rows:
                if unique_key := row["values"].get("unique_key"):
                    source_keys.add(unique_key)

            logger.info(f"Retrieved {len(rows)} rows from source {i}")

        # 3. Determine which rows to add, update, or delete
        rows_to_add: list[dict[str, Any]] = []
        rows_to_update: list[dict[str, Any]] = []
        rows_to_delete: list[dict[str, Any]] = []

        # Find rows to add or update
        for row in all_source_rows:
            unique_key = row["values"].get("unique_key")
            if not unique_key:
                continue

            # Add the hash to the values for storage
            row["values"]["row_hash"] = row["hash"]

            if unique_key not in existing_by_key:
                # This is a new row
                rows_to_add.append(row)
            else:
                # This is an existing row, check if it changed
                existing_row = existing_by_key[unique_key]
                existing_hash = existing_row["values"].get("row_hash", "")

                if row["hash"] != existing_hash:
                    # Row has changed, update it
                    row["id"] = existing_row["id"]  # Keep the existing row ID
                    rows_to_update.append(row)

        # Find rows to delete (in destination but not in source)
        rows_to_delete.extend(
            row
            for unique_key, row in existing_by_key.items()
            if unique_key not in source_keys
        )

        # 4. Apply the changes
        logger.info(
            f"Changes to apply: "
            f"{len(rows_to_add)} new, "
            f"{len(rows_to_update)} updates, "
            f"{len(rows_to_delete)} deletions",
        )

        # Add new rows and update existing ones
        if rows_to_add or rows_to_update:
            await self.coda_client.upsert_rows(
                self.destination_doc_id,
                self.destination_table_id,
                rows_to_add + rows_to_update,
                ["unique_key"],
            )

        # Delete removed rows
        if rows_to_delete:
            row_ids = [row["id"] for row in rows_to_delete]
            await self.coda_client.delete_rows(
                self.destination_doc_id,
                self.destination_table_id,
                row_ids,
            )
            logger.info(f"Deleted {len(rows_to_delete)} removed rows")

        logger.info("Merge completed successfully!")
        return MergeResult(
            success=True,
            totalRowsProcessed=len(all_source_rows),
            newRows=len(rows_to_add),
            updatedRows=len(rows_to_update),
            deletedRows=len(rows_to_delete),
            destinationTableId=self.destination_table_id,
        )
