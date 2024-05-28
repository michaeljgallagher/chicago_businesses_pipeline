import configparser
import io
import logging
import os
import time
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from chicago_businesses_pipeline.utils.delta_operations import save_delta_table
from chicago_businesses_pipeline.utils.fetch import get_dataset
from chicago_businesses_pipeline.utils.processing import (
    join_licenses_owners,
    transform_licenses,
    transform_owners,
)
from chicago_businesses_pipeline.utils.schemas import LICENSES_SCHEMA, OWNERS_SCHEMA
from chicago_businesses_pipeline.utils.webhook import (
    error_message,
    post_to_webhook,
    success_message,
)


def main():
    # Read config file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(current_dir, "pipeline.ini")
    parser = configparser.ConfigParser()
    parser.read(config_file_path)
    config = namedtuple("config", parser.sections())(
        **{
            section: namedtuple(section, options)(*parser[section].values())
            for section, options in (x for x in parser.items() if x[0] != "DEFAULT")
        }
    )

    # Get current time (UTC)
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Set up logging
    os.makedirs(config.logs.log_dir, exist_ok=True)
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(f"{config.logs.log_dir}/{now}.log"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger(__name__)

    # Log start of the script
    logger.info("Chicago businesses pipeline started")

    try:
        # Fetch Business Owners dataset
        owners_res = get_dataset(
            config.datasource_api.base_url,
            config.datasource_api.owners_identifier,
            config.datasource_api.app_token,
        )
        owners_df = pl.read_csv(
            io.StringIO(owners_res),
            schema=OWNERS_SCHEMA,
        )

        # Fetch Business Licenses dataset
        licenses_res = get_dataset(
            config.datasource_api.base_url,
            config.datasource_api.licenses_identifier,
            config.datasource_api.app_token,
        )
        licenses_df = pl.read_csv(
            io.StringIO(licenses_res),
            schema=LICENSES_SCHEMA,
        )

        # Perform transformations
        owners_df = transform_owners(owners_df)
        licenses_df = transform_licenses(licenses_df)

        # Join the two datasets (inner, on `account_number`)
        licenses_owners_df = join_licenses_owners(licenses_df, owners_df)

        # Save to Delta table, clean older files
        save_delta_table(
            licenses_owners_df,
            Path(config.delta_table.location),
            int(config.delta_table.retention_hours),
        )

        # Post success message to webhook
        post_to_webhook(
            config.webhook.webhook_url,
            success_message(licenses_owners_df.shape[0]),
        )
        logger.info("Chicago businesses pipeline completed successfully")

    except Exception as e:
        # Log exception and notify
        logger.exception(
            "An error occurred during Chicago businesses pipeline:\n" + str(e)
        )
        post_to_webhook(
            config.webhook.webhook_url,
            error_message(now),
        )


if __name__ == "__main__":
    main()
