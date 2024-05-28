import requests


def post_to_webhook(webhook_url: str, message: str) -> None:
    """
    Post a message to a Teams webhook URL

    Args:
        webhook_url (str): URL of the webhook
        message (str): Message to post

    Raises:
        RuntimeError: Raised if there is an error posting the message
    """
    res = requests.post(
        webhook_url,
        json={
            "title": "Chicago Businesses Pipeline",
            "text": message,
        },
    )
    if res.status_code != 200:
        raise RuntimeError(res.text)


def success_message(num: int) -> str:
    """
    Create a success message for the pipeline

    Args:
        num (int): Number of rows in the latest dataset

    Returns:
        str: The success message
    """
    message = (
        "<h1>✅ Successfully ran Chicago Businesses Pipeline</h1></br>"
        + f"<p>Delta table now contains {num} rows.</p>"
    )
    return message


def error_message(ts: str) -> str:
    """
    Create an error message for the pipeline

    Args:
        ts (str): Timestamp of the pipeline run

    Returns:
        str: The error message
    """
    message = (
        "<h1>❌ An error occurred when running the Strava pipeline.</h1></br>"
        + f"<p>Check logs located in `{ts}.log` for more details.</p>"
    )
    return message
