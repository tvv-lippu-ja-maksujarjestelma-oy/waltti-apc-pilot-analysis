"""Download local copies of Pulsar topics."""

import collections
import datetime
import errno
import logging
import pathlib
import tempfile

import polars as pl
import pulsar


def convert_milliseconds_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(
        timestamp / 1000, tz=datetime.timezone.utc
    )


def append_to_sequences(base, addition):
    for key, value in addition.items():
        base[key].append(value)


def combine_and_write_df(
    directory, current_timeframe, create_empty_df, existing_df, new_rows
):
    path = (directory / current_timeframe).with_suffix(".arrow")
    new_df = pl.from_dict(new_rows, schema=create_empty_df().schema)
    existing_df.vstack(new_df).rechunk().write_ipc(path, compression="zstd")


def get_publish_timeframe(message):
    return convert_milliseconds_to_datetime(
        message.publish_timestamp()
    ).strftime("%Y-%m-%d")


def is_directory_writable(path):
    try:
        testfile = tempfile.TemporaryFile(dir=path)
        testfile.close()
    except OSError as e:
        if e.errno == errno.EACCES:
            return False
        e.filename = path
        raise
    return True


def convert_topic_to_directory(topic):
    return "".join(c for c in topic if c.isalnum())


def convert_onboard_apc_message_to_dict(message):
    properties = message.properties()
    return {
        "message_id": message.message_id().serialize(),
        "event_time": convert_milliseconds_to_datetime(
            message.event_timestamp()
        ),
        "publish_time": convert_milliseconds_to_datetime(
            message.publish_timestamp()
        ),
        "mqtt_topic": properties["mqttTopic"],
        "mqtt_qos": int(properties["mqttQos"]),
        "mqtt_is_retained": bool(properties["mqttIsRetained"]),
        "mqtt_is_duplicate": bool(properties["mqttIsDuplicate"]),
        "payload": message.data().decode("utf-8", "strict"),
    }


def create_empty_onboard_apc_df():
    return pl.DataFrame(
        [
            pl.Series("message_id", [], dtype=pl.Binary),
            pl.Series("event_time", [], dtype=pl.Datetime(time_zone="UTC")),
            pl.Series("publish_time", [], dtype=pl.Datetime(time_zone="UTC")),
            pl.Series("mqtt_topic", [], dtype=pl.Utf8),
            pl.Series("mqtt_qos", [], dtype=pl.UInt8),
            pl.Series("mqtt_is_retained", [], dtype=pl.Boolean),
            pl.Series("mqtt_is_duplicate", [], dtype=pl.Boolean),
            pl.Series("payload", [], dtype=pl.Utf8),
        ]
    )


def convert_gtfsrtvp_message_to_dict(message):
    return {
        "message_id": message.message_id().serialize(),
        "event_time": convert_milliseconds_to_datetime(
            message.event_timestamp()
        ),
        "publish_time": convert_milliseconds_to_datetime(
            message.publish_timestamp()
        ),
        "payload": message.data(),
    }


def create_empty_gtfsrtvp_df():
    return pl.DataFrame(
        [
            pl.Series("message_id", [], dtype=pl.Binary),
            pl.Series("event_time", [], dtype=pl.Datetime(time_zone="UTC")),
            pl.Series("publish_time", [], dtype=pl.Datetime(time_zone="UTC")),
            pl.Series("payload", [], dtype=pl.Binary),
        ]
    )


def update_one_local_topic_copy(
    data_root,
    pulsar_client,
    topic,
    create_empty_df,
    convert_message_to_dict,
    log_interval,
):
    logging.info(f"Start updating local copy of topic {topic}")
    subdirectory = convert_topic_to_directory(topic)
    directory = data_root / subdirectory
    directory.mkdir(parents=True, exist_ok=True)
    if not is_directory_writable(directory):
        raise ValueError(f"Directory {directory} must be writable.")
    file_list = [f for f in directory.iterdir() if f.is_file()]

    existing_df = create_empty_df()
    message_id = pulsar.MessageId.earliest
    current_timeframe = None
    if file_list:
        latest_file = max(file_list)
        existing_df = pl.read_ipc(latest_file)
        message_id = pulsar.MessageId.deserialize(
            existing_df.select("message_id").tail(1).item()
        )
        current_timeframe = latest_file.stem

    if current_timeframe is not None:
        logging.info(
            f"Start reading from message id #{str(message_id)} in timeframe {current_timeframe}"
        )
    else:
        logging.info("Start reading from the beginning")
    reader = pulsar_client.create_reader(topic, message_id)
    new_rows = collections.defaultdict(list)
    message_counter = 0
    try:
        while reader.has_message_available():
            message = reader.read_next()

            timeframe = get_publish_timeframe(message)
            if current_timeframe is None:
                current_timeframe = timeframe
            elif current_timeframe != timeframe:
                logging.info(
                    f"Total number of messages in timeframe {current_timeframe}: {existing_df.height + message_counter}"
                )
                combine_and_write_df(
                    directory,
                    current_timeframe,
                    create_empty_df,
                    existing_df,
                    new_rows,
                )
                new_rows = collections.defaultdict(list)
                message_counter = 0
                current_timeframe = timeframe
                existing_df = create_empty_df()

            append_to_sequences(new_rows, convert_message_to_dict(message))
            message_counter = message_counter + 1
            if message_counter % log_interval == 0:
                logging.info(
                    f"Number of messages handled for timeframe {current_timeframe} thus far: {message_counter}"
                )
        logging.info(
            f"Total number of messages in timeframe {current_timeframe} by the time topic ended: {existing_df.height + message_counter}"
        )
        combine_and_write_df(
            directory,
            current_timeframe,
            create_empty_df,
            existing_df,
            new_rows,
        )
    finally:
        reader.close()


def update_local_topic_copies(data_root_path, configuration, pulsar_client):
    data_root = pathlib.Path(data_root_path)
    for topic in configuration["gtfsrtvp_topic_list"]:
        update_one_local_topic_copy(
            data_root,
            pulsar_client,
            topic,
            create_empty_gtfsrtvp_df,
            convert_gtfsrtvp_message_to_dict,
            1e5,
        )
    update_one_local_topic_copy(
        data_root,
        pulsar_client,
        configuration["onboard_apc_topic"],
        create_empty_onboard_apc_df,
        convert_onboard_apc_message_to_dict,
        1e5,
    )


def create_pulsar_client(configuration):
    auth = pulsar.AuthenticationOauth2(
        f"""{{
        "audience": "{configuration["oauth2"]["audience"]}",
        "issuer_url": "{configuration["oauth2"]["issuer_url"]}",
        "private_key": "{configuration["oauth2"]["private_key"]}"
        }}"""
    )
    client = pulsar.Client(
        configuration["client"]["service_url"],
        authentication=auth,
        use_tls=True,
        tls_validate_hostname=configuration["client"]["validate_tls_hostname"],
    )
    return client


def download_all(data_root_path, configuration):
    pulsar_client = None
    try:
        pulsar_client = create_pulsar_client(configuration)
        update_local_topic_copies(
            data_root_path, configuration["readers"], pulsar_client
        )
    finally:
        if pulsar_client is not None:
            pulsar_client.close()
