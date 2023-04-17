# waltti-apc-pilot-analysis

Analyze the data from the APC pilot and gather insight from it.

Download onto and analyze the data on one computing node.

This repository has been created as part of the [Waltti APC](https://github.com/tvv-lippu-ja-maksujarjestelma-oy/waltti-apc) project.

## Installation

1. Install [`poetry`](https://python-poetry.org/).
1. `poetry install`

## Running

1. Create an `.env` file with the appropriate configuration.
   See below for the environment variables.
1. `poetry run python src/waltti_apc_pilot_analysis/main.py`

## Configuration

| Environment variable               | Required? | Default value | Description                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ---------------------------------- | --------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATA_ROOT_PATH`                   | ✅ Yes    |               | The path of the directory where to download the Pulsar messages.                                                                                                                                                                                                                                                                                                                                                                              |
| `PULSAR_GTFSRTVP_TOPIC_JSON_ARRAY` | ✅ Yes    |               | A JSON array of the Pulsar topics for the GTFS Realtime VehiclePosition messages to download. Each of the topics will be downloaded starting from the earliest message that is still locally missing. Topic regex pattern is not used here so that each topic can be downloaded separately. An example value could be `["persistent://tenant/source/gtfs-realtime-vp-fi-kuopio","persistent://tenant/source/gtfs-realtime-vp-fi-jyvaskyla"]`. |
| `PULSAR_OAUTH2_AUDIENCE`           | ✅ Yes    |               | The OAuth 2.0 audience.                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `PULSAR_OAUTH2_ISSUER_URL`         | ✅ Yes    |               | The OAuth 2.0 issuer URL.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `PULSAR_OAUTH2_KEY_PATH`           | ✅ Yes    |               | The path to the OAuth 2.0 private key JSON file.                                                                                                                                                                                                                                                                                                                                                                                              |
| `PULSAR_ONBOARD_APC_TOPIC`         | ✅ Yes    |               | The topic for the onboard APC messages to download. The download will start from the earliest message that is still locally missing. An example value could be `"persistent://tenant/source/mqtt-apc-from-vehicle"`.                                                                                                                                                                                                                          |
| `PULSAR_SERVICE_URL`               | ✅ Yes    |               | The service URL.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `PULSAR_TLS_VALIDATE_HOSTNAME`     | ✅ Yes    |               | Whether to validate the hostname on its TLS certificate. This option exists because some Apache Pulsar hosting providers cannot handle Apache Pulsar clients setting this to `true`.                                                                                                                                                                                                                                                          |
