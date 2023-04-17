"""Main."""

import json
import logging
import os

import dotenv

from waltti_apc_pilot_analysis import analysis
from waltti_apc_pilot_analysis import download


def read_configuration():
    dotenv.load_dotenv()
    configuration = {
        "pulsar": {
            "oauth2": {
                "audience": os.environ["PULSAR_OAUTH2_AUDIENCE"],
                "issuer_url": os.environ["PULSAR_OAUTH2_ISSUER_URL"],
                "private_key": os.environ["PULSAR_OAUTH2_KEY_PATH"],
            },
            "client": {
                "service_url": os.environ["PULSAR_SERVICE_URL"],
                "validate_tls_hostname": os.environ[
                    "PULSAR_TLS_VALIDATE_HOSTNAME"
                ]
                == "True",
            },
            "readers": {
                "gtfsrtvp_topic_list": json.loads(
                    os.environ["PULSAR_GTFSRTVP_TOPIC_JSON_ARRAY"]
                ),
                "onboard_apc_topic": os.environ["PULSAR_ONBOARD_APC_TOPIC"],
            },
        },
        "counting_system_map": json.loads(os.environ["COUNTING_SYSTEM_MAP"]),
        "data_root_path": os.environ["DATA_ROOT_PATH"],
        "run_phases": json.loads(os.environ["RUN_PHASES_JSON_ARRAY"]),
    }
    return configuration


def main():
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=logging.INFO,
    )
    configuration = read_configuration()
    if "download" in configuration["run_phases"]:
        download.download_all(
            configuration["data_root_path"], configuration["pulsar"]
        )
    if "analysis" in configuration["run_phases"]:
        analysis.run_analysis(
            configuration["data_root_path"],
            configuration["counting_system_map"],
        )


if __name__ == "__main__":
    main()
