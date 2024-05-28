import logging

import requests

logger = logging.getLogger(__name__)


def get_dataset(base_url: str, dataset_identifier: str, app_token: str) -> str:
    """
    Fetch a dataset from a specific URL (base should be Chicago Data Portal).

    Args:
        base_url (str): The base URL to Chicago Data Portal
        dataset_identifier (str): String identifying dataset (e.g. `ezma-pppn`)
        app_token (str): App token for authorization

    Raises:
        Exception: Raised with the error message if request fails

    Returns:
        str: The content of the dataset in CSV format
    """
    logger.info(f"Fetching dataset: {dataset_identifier}")

    url = f"{base_url}/{dataset_identifier}.csv"
    headers = {"X-App-Token": app_token}
    params = {"$limit": 9_999_999}

    res = requests.get(
        url=url,
        headers=headers,
        params=params,
    )

    if res.status_code != 200:
        raise Exception(f"Could not fetch dataset: {res.text}")

    return res.text
