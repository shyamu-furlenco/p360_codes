import json
import urllib.request
import os


def handler(event, context):
    """
    AWS Lambda function registered as a Redshift external function.

    Redshift external functions send batched row arguments in this shape:
        {"arguments": [[msg1], [msg2], ...]}

    Returns the shape Redshift expects:
        {"success": True, "results": [True, True, ...]}

    Environment variables required:
        SLACK_WEBHOOK_URL  — incoming webhook URL for the target Slack channel
    """
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]
    results = []

    for row in event.get("arguments", []):
        message = row[0]
        payload = json.dumps({"text": message}).encode()
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
        results.append(True)

    return {"success": True, "results": results}
