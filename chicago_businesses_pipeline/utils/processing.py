import logging

import polars as pl

logger = logging.getLogger(__name__)


def transform_owners(owners_df: pl.DataFrame) -> pl.DataFrame:
    """
    Process the Business Owners dataset.
    Convert name columns to uppercase, create `owner_full_name` column,
    drop duplicate rows.

    Args:
        owners_df (pl.DataFrame): Business Owners dataframe

    Returns:
        pl.DataFrame: Transformed Business Owners dataframe
    """
    logger.info("Begin processing Business Owners dataset")

    # Convert columns with names of owners to all caps (helps deduplication)
    logger.info("Converting columns with names of owners to all caps")
    owners_df = owners_df.with_columns(
        pl.col("owner_first_name").str.to_uppercase(),
        pl.col("owner_middle_initial").str.to_uppercase(),
        pl.col("owner_last_name").str.to_uppercase(),
    )

    # Add `owner_full_name` column
    # Format is LAST,FIRST MIDDLE
    logger.info("Creating `owner_full_name` column")
    owners_df = owners_df.with_columns(
        pl.concat_str(
            [
                pl.concat_str(
                    [pl.col("owner_last_name"), pl.col("owner_first_name")],
                    separator=",",
                    ignore_nulls=True,
                ),
                pl.col("owner_middle_initial"),
            ],
            separator=" ",
            ignore_nulls=True,
        ).alias("owner_full_name")
    )

    # Drop duplicates
    duplicate_count = owners_df.filter(owners_df.is_duplicated()).shape[0]
    if duplicate_count > 0:
        logger.info("Dropping duplicate rows from Business Owners")
        start_count = owners_df.shape[0]
        owners_df = owners_df.unique()
        logger.info(f"Dropped {start_count - owners_df.shape[0]} duplicate row(s)")

    return owners_df


def transform_licenses(licenses_df: pl.DataFrame) -> pl.DataFrame:
    """
    Process the Business Licenses dataset.
    Converting datetime columns to dates, remove redundant columns,
    drop duplicate rows.

    Args:
        licenses_df (pl.DataFrame): Business Licenses dataframe

    Returns:
        pl.DataFrame: Transformed Business Licenses dataframe
    """
    logger.info("Begin processing Business Licenses dataset")

    # Convert datetime columns to dates
    logger.info("Converting datetime columns to dates")
    licenses_df = licenses_df.with_columns(
        pl.col("application_created_date").cast(pl.Date),
        pl.col("application_requirements_complete").cast(pl.Date),
        pl.col("payment_date").cast(pl.Date),
        pl.col("license_start_date").cast(pl.Date),
        pl.col("expiration_date").cast(pl.Date),
        pl.col("license_approved_for_issuance").cast(pl.Date),
        pl.col("date_issued").cast(pl.Date),
        pl.col("license_status_change_date").cast(pl.Date),
    )

    # Remove redundant `location` column
    licenses_df = licenses_df.drop("location")

    # Drop duplicates
    duplicate_count = licenses_df.filter(licenses_df.is_duplicated()).shape[0]
    if duplicate_count > 0:
        logger.info("Dropping duplicate rows from Business Licenses")
        start_count = licenses_df.shape[0]
        licenses_df = licenses_df.unique()
        logger.info(f"Dropped {start_count - licenses_df.shape[0]} duplicate row(s)")

    return licenses_df


def join_licenses_owners(
    licenses_df: pl.DataFrame, owners_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Join Business Licenses and Business Owners datasets on `account_number`
    and ensure `legal_name` matches between the datasets.

    Args:
        licenses_df (pl.DataFrame): Transformed Business Licenses dataframe
        owners_df (pl.DataFrame): Transformed Business Owners dataframe

    Returns:
        pl.DataFrame: LicensesOwners dataframe
    """
    logger.info("Joining Business Licenses and Business Owners on `account_number`")
    logger.info(f"Business Licenses row count: {licenses_df.shape[0]}")
    logger.info(f"Business Owners row count: {owners_df.shape[0]}")

    # Join Business Licenses and Business Owners
    # Inner join on account_number; only want owners (accounts) with licenses
    licenses_owners_df = licenses_df.join(owners_df, on="account_number")

    # Check if any `legal_name` don't match between two datasets
    # If there are any that don't match, warn and filter them out
    # Note this is effectively updating the inner join to join on both
    # the `account_number` and `legal_name` columns, but we'd like to check and notify
    # if there are discrepancies
    if (
        num_rows_not_matching := licenses_owners_df.filter(
            pl.col("legal_name") != pl.col("legal_name_right")
        ).shape[0]
    ) > 0:
        logger.warn(
            f"Found {num_rows_not_matching} rows where `legal_name` in Business Licenses "
            "did not match `legal_name` in Business Owners for a given `account_number`"
        )
        logger.warn("Filtering out those rows")
        licenses_owners_df = licenses_owners_df.filter(
            pl.col("legal_name") == pl.col("legal_name_right")
        )

    # Drop ambiguous column
    licenses_owners_df = licenses_owners_df.drop("legal_name_right")

    logger.info(f"LicensesOwners row count: {licenses_owners_df.shape[0]}")

    return licenses_owners_df
