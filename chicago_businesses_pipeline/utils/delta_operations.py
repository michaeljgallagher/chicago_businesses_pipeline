import logging
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger(__name__)


def save_delta_table(
    licenses_owners_df: pl.DataFrame, location: str | Path, retention_hours: int
):
    """
    Save a Polars DataFrame to a Delta table.
    Perform compaction and vacuuming after writing.

    Args:
        licenses_owners_df (pl.DataFrame): LicensesOwners dataframe
        location (str | Path): Location of Delta table
        retention_hours (int): Retention period (in hours) for vacuuming
    """
    # Convert to Arrow table before write
    licenses_owners_arrow = licenses_owners_df.to_arrow()

    # Write to Delta table in `overwrite` mode
    # Overwrite is ideal since incremental deletes will clone most of table anyway
    logger.info("Updating Delta table")
    write_deltalake(
        location,
        licenses_owners_arrow,
        mode="overwrite",
        name="licenses_owners",
        description="Chicago Business Licenses and Owners data",
        engine="rust",
        configuration={
            "compression": "SNAPPY",
        },
    )

    dt = DeltaTable(location)

    # Vacuum (delete older files to save storage)
    logger.info(
        "Deleting files that are no longer referenced by the Delta table "
        f"and are older than {retention_hours} hours"
    )
    deleted_files = dt.vacuum(
        dry_run=False,
        enforce_retention_duration=False,
        retention_hours=retention_hours,
    )
    logger.info(f"Deleted {len(deleted_files)} files")
    if len(deleted_files) > 0:
        logger.info("Deleted files:\n" + "\n".join(deleted_files))

    logger.info(f"Delta table updated. Current version: {dt.version()}")
