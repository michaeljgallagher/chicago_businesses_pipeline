# Chicago Businesses Data Pipeline

## Overview

From the Chicago Data Portal, we can access two datasets: Business Licenses, which lists all licenses issued by the Department of Business Affairs and Consumer Protection in the City of Chicago from 2002 to the present, and Business Owners, which contains the business owner information for all accounts in the former dataset.

This pipeline processes and transforms the Business Owners and Business Licenses datasets, joins them into a single denormalized dataset, and saves the resulting data to a Delta table. Data transformations include casting integer types to strings, reformatting dates, reformatting names, and deduplication. The Delta table can be configured with a retention period, so older versions of the dataset can be cleared to save storage.

The processing is done in-memory using [Polars](https://pola.rs/). The Delta table is created using [delta-rs](https://delta-io.github.io/delta-rs/) (which requires no Spark or JVM dependencies).

For more information:

- [Business Owners dataset documentation](https://data.cityofchicago.org/dataset/Business-Owners/ezma-pppn)
- [Business Licenses dataset documentation](https://data.cityofchicago.org/dataset/Business-Licenses/r5kz-chrr)
- [Polars API reference](https://docs.pola.rs/api/)
- [delta-rs API reference](https://delta-io.github.io/delta-rs/api/delta_table/)

## Installation and usage

### Prerequisites

#### 1. Create an App Token for the Chicago Data Portal

The best way to efficiently leverage the API is to generate an App Token. This can be done by creating an account [on the Chicago Data Portal](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys) and then following [this guide](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys) to generate the token.

#### 2. Create an Incoming Webhook for alerting to Teams

Alerting for the success or failure of the pipeline is done by an Incoming Webhook that will post messages to Teams. Creating this can be done by following [this guide](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook/).

### Installation

**NOTE**: This was developed using Python 3.10.12; I make no guarantees if you use an earlier version of Python.

Clone this repository:

```bash
git clone https://github.com/michaeljgallagher/chicago_businesses_pipeline && cd chicago_businesses_pipeline
```

Create and activate a Python virtual environment:

```bash
python -m venv env && source env/bin/activate
```

Update `pipeline.ini` with your App Token, path to the Delta table, path to store logs, and URL for your Incoming Webhook. The retention period for the Delta table can be configured (in hours). The URL for the data source as well as the identifiers for the datasets can be configured in case they change in the future.

```ini
[datasource_api]
base_url = https://data.cityofchicago.org/resource
app_token = <APP_TOKEN>
owners_identifier = ezma-pppn
licenses_identifier = r5kz-chrr

[delta_table]
location = <PATH_TO_DELTA_TABLE>
retention_hours = 168

[logs]
log_dir = <PATH_TO_LOGS_DIR>

[webhook]
webhook_url = <WEBHOOK_URL>
```

Once that's been updated, it's a good idea to change permissions on this file to read-only:

```bash
chmod 400 pipeline.ini
```

With the virtual environment activated and from within the root `chicago_businesses_pipeline` directory, install via the `setup.py` script:

```bash
pip install --upgrade pip && pip install .
```

### Usage and automation

From here, the pipeline can be run manually by calling `chi-data-pipeline` when the virtual environment is activated.

To automate this process, a cron job can be created that points to this command in the virtual environment:

```bash
30 10 * * * /path/to/chicago_businesses_pipeline/env/bin/chi-data-pipeline
```

Since the datasets are usually updated in the early morning, mid-morning is a good time to run the cron job.

## Logging

Logs are stored in the directory specified in the log_dir configuration. Each pipeline run generates a log file with a timestamp.

## Error Handling

If an error occurs during the pipeline execution, an error message is logged, and a notification is sent to the configured Teams Incoming Webhook.

## Testing

Currently, there are a few unit tests included to test the basic functionality of the transformations and the joining. These can be run with pytest (from the root `chicago_businesses_pipeline` directory, with the virtual environment activated):

```bash
pytest -v
```

More unit tests can be added for completeness, with more edge cases.

For end-to-end testing, a mock API can be set up that returns our own curated test datasets. We can configure another instance of the pipeline that points to this mock API and validate the outputs against what we expect.

Edge cases that we could consider include changes in schemas and invalid data types for certain fields.
