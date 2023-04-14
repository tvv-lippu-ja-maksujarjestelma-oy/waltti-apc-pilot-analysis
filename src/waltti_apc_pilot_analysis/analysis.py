"""Analyze downloaded data."""

import collections
import datetime
import json
import logging
import pathlib

from google.transit import gtfs_realtime_pb2

# FIXME: remove
import google.protobuf.message
import plotly.express as px
import polars as pl
import pyarrow.dataset


def get_data_by_suffix(data_root, subdirs, suffix):
    path = data_root / [s for s in subdirs if s.endswith(suffix)][0]
    return pl.scan_pyarrow_dataset(
        pyarrow.dataset.dataset(
            path,
            format="ipc",
            # partitioning=pyarrow.dataset.partitioning(
            #    field_names=["year", "month", "day"], flavor="filename"
            # ),
        )
    )


def is_json(string):
    result = False
    try:
        json.loads(string)
        result = True
    except json.JSONDecodeError:
        pass
    return result


def parse_gtfsrtvp(interesting_vehicles, gtfsrtvp):
    feed = gtfs_realtime_pb2.FeedMessage()
    positions_result = {
        "message_timestamp": None,
        "trip_id": None,
        "start_time": None,
        "start_date": None,
        "schedule_relationship": None,
        "route_id": None,
        "direction_id": None,
        "current_stop_sequence": None,
        "current_status": None,
        "vehicle_timestamp": None,
        "stop_id": None,
        "vehicle_id": None,
    }
    try:
        feed.ParseFromString(gtfsrtvp)
        positions = collections.defaultdict(list)
        found_vehicle_ids = []
        for entity in feed.entity:
            vehicle = entity.vehicle
            vehicle_id = vehicle.vehicle.id
            if vehicle_id in interesting_vehicles:
                found_vehicle_ids.append(vehicle_id)
                trip = vehicle.trip
                positions["message_timestamp"].append(feed.header.timestamp)
                positions["trip_id"].append(trip.trip_id)
                positions["start_time"].append(trip.start_time)
                positions["start_date"].append(trip.start_date)
                positions["schedule_relationship"].append(
                    gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                        trip.schedule_relationship
                    )
                )
                positions["route_id"].append(trip.route_id)
                positions["direction_id"].append(trip.direction_id)
                positions["current_stop_sequence"].append(
                    vehicle.current_stop_sequence
                )
                positions["current_status"].append(
                    gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(
                        vehicle.current_status
                    )
                )
                positions["vehicle_timestamp"].append(vehicle.timestamp)
                positions["stop_id"].append(vehicle.stop_id)
                positions["vehicle_id"].append(vehicle_id)
        for missed_vehicle in list(
            set(interesting_vehicles).difference(found_vehicle_ids)
        ):
            positions["message_timestamp"].append(None)
            positions["trip_id"].append(None)
            positions["start_time"].append(None)
            positions["start_date"].append(None)
            positions["schedule_relationship"].append(None)
            positions["route_id"].append(None)
            positions["direction_id"].append(None)
            positions["current_stop_sequence"].append(None)
            positions["current_status"].append(None)
            positions["vehicle_timestamp"].append(None)
            positions["stop_id"].append(None)
            positions["vehicle_id"].append(missed_vehicle)
        for k, v in positions.items():
            positions_result[k] = v
    except google.protobuf.message.DecodeError:
        logging.info(f"This is not GTFS Realtime: {str(gtfsrtvp)}")
    return positions_result


# FIXME: remove if does not reduce memory usage
def parse_gtfsrtvp_2(interesting_vehicles, gtfsrtvp):
    feed = gtfs_realtime_pb2.FeedMessage()
    positions_result = []
    # positions_result = [{
    #    "message_timestamp": None,
    #    "trip_id": None,
    #    "start_time": None,
    #    "start_date": None,
    #    "schedule_relationship": None,
    #    "route_id": None,
    #    "direction_id": None,
    #    "current_stop_sequence": None,
    #    "current_status": None,
    #    "vehicle_timestamp": None,
    #    "stop_id": None,
    #    "vehicle_id": None,
    # }]
    try:
        feed.ParseFromString(gtfsrtvp)
        found_vehicle_ids = []
        for entity in feed.entity:
            vehicle = entity.vehicle
            vehicle_id = vehicle.vehicle.id
            if vehicle_id in interesting_vehicles:
                found_vehicle_ids.append(vehicle_id)
                trip = vehicle.trip
                positions_result.append(
                    {
                        "message_timestamp": feed.header.timestamp,
                        "trip_id": trip.trip_id,
                        "start_time": trip.start_time,
                        "start_date": trip.start_date,
                        "schedule_relationship": gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                            trip.schedule_relationship
                        ),
                        "route_id": trip.route_id,
                        "direction_id": trip.direction_id,
                        "current_stop_sequence": vehicle.current_stop_sequence,
                        "current_status": gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(
                            vehicle.current_status
                        ),
                        "vehicle_timestamp": vehicle.timestamp,
                        "stop_id": vehicle.stop_id,
                        "vehicle_id": vehicle_id,
                    }
                )
        for missed_vehicle in list(
            set(interesting_vehicles).difference(found_vehicle_ids)
        ):
            positions_result.append(
                {
                    "message_timestamp": None,
                    "trip_id": None,
                    "start_time": None,
                    "start_date": None,
                    "schedule_relationship": None,
                    "route_id": None,
                    "direction_id": None,
                    "current_stop_sequence": None,
                    "current_status": None,
                    "vehicle_timestamp": None,
                    "stop_id": None,
                    "vehicle_id": missed_vehicle,
                }
            )
    except google.protobuf.message.DecodeError:
        logging.info(f"This is not GTFS Realtime: {str(gtfsrtvp)}")
    return positions_result


def parse_apc_json(possibly_json_string):
    counts = {
        "schema_version": None,
        "counting_system_id_from_json": None,
        "apc_message_id": None,
        "apc_message_timestamp_string": None,
        "count_quality": None,
        "door": None,
        "count_class": None,
        "count_in": None,
        "count_out": None,
    }
    try:
        apc = json.loads(possibly_json_string)["APC"]
        vehicle_counts = apc["vehiclecounts"]
        if vehicle_counts is not None:
            counts_from_json = collections.defaultdict(list)
            for door_count in vehicle_counts["doorcounts"]:
                for class_count in door_count["count"]:
                    counts_from_json["schema_version"].append(
                        apc["schemaVersion"]
                    )
                    counts_from_json["counting_system_id_from_json"].append(
                        # FIXME: Why cannot `apply` handle this None?
                        # apc.get("countingSystemId", None),
                        apc.get("countingSystemId", "Nonni"),
                    )
                    counts_from_json["apc_message_id"].append(apc["messageId"])
                    counts_from_json["apc_message_timestamp_string"].append(
                        apc["tst"]
                    )
                    counts_from_json["count_quality"].append(
                        vehicle_counts["countquality"]
                    )
                    counts_from_json["door"].append(door_count["door"])
                    counts_from_json["count_class"].append(
                        class_count["class"]
                    )
                    counts_from_json["count_in"].append(class_count["in"])
                    counts_from_json["count_out"].append(class_count["out"])
            for k, v in counts_from_json.items():
                counts[k] = v
    except json.JSONDecodeError:
        pass
    except BaseException as err:
        logging.error(
            f"Something went wrong while parsing APC JSON: {err=}, {type(err)=}"
        )
        raise
    return counts


def keep_first_of_each_run(df, group_column, run_column):
    columns = df.columns
    row_index_name = "original_row_index"
    group_id_name = "group_id"
    df = df.with_row_count(row_index_name)
    first_rows_df = (
        df.groupby(group_column)
        .agg(
            [
                pl.when(pl.col(run_column).is_null())
                .then(
                    pl.col(run_column)
                    .is_null()
                    .ne(pl.col(run_column).is_null().shift(1))
                )
                .otherwise(pl.col(run_column).ne(pl.col(run_column).shift(1)))
                .cast(pl.UInt32)
                .cumsum()
                .alias(group_id_name),
                pl.col(row_index_name),
            ]
        )
        .explode([group_id_name, row_index_name])
        .groupby([group_column, group_id_name], maintain_order=True)
        .first()
        .sort(row_index_name)
    )
    df = df.join(first_rows_df, on=row_index_name, how="semi").select(columns)
    return df


def get_gtfsrtvp(
    data_root, subdirs, path_suffix, start_time, end_time, vehicle_df
):
    authority_name = vehicle_df.get_column("authority_name").unique().item()
    interesting_vehicles = (
        vehicle_df.get_column("vehicle_id").unique().to_list()
    )
    lf = get_data_by_suffix(data_root, subdirs, path_suffix)
    df = (
        lf.filter(pl.col("publish_time").is_between(start_time, end_time))
        .with_columns(
            [
                pl.lit(authority_name).alias("authority_name"),
                pl.col("payload")
                .apply(
                    # FIXME: unnest, then explode
                    lambda gtfsrtvp: parse_gtfsrtvp(
                        interesting_vehicles, gtfsrtvp
                    )
                    # # FIXME: explode, then unnest
                    # lambda gtfsrtvp: parse_gtfsrtvp_2(
                    #     interesting_vehicles, gtfsrtvp
                    # ),
                    # return_dtype=pl.List(pl.Struct(
                    #     [
                    #         pl.Field("message_timestamp", pl.Int64),
                    #         pl.Field("trip_id", pl.Utf8),
                    #         pl.Field("start_time", pl.Utf8),
                    #         pl.Field("start_date", pl.Utf8),
                    #         pl.Field("schedule_relationship", pl.Utf8),
                    #         pl.Field("route_id", pl.Utf8),
                    #         pl.Field("direction_id", pl.Int64),
                    #         pl.Field("current_stop_sequence", pl.Int64),
                    #         pl.Field("current_status", pl.Utf8),
                    #         pl.Field("vehicle_timestamp", pl.Int64),
                    #         pl.Field("stop_id", pl.Utf8),
                    #         pl.Field("vehicle_id", pl.Utf8),
                    #     ]
                    # )),
                )
                .alias("positions"),
            ]
        )
        .unnest("positions")
        # FIXME: Workaround for polars bug of breaking on explode() for lazy df
        .collect()
    )
    logging.info(f"parsed gtfsrtvp df shape {df.shape}")
    logging.info(f"parsed gtfsrtvp df schema {df.schema}")
    logging.info(f"parsed gtfsrtvp df head {df.head(8)}")
    df = (
        df.explode(
            [
                "message_timestamp",
                "trip_id",
                "start_time",
                "start_date",
                "schedule_relationship",
                "route_id",
                "direction_id",
                "current_stop_sequence",
                "current_status",
                "vehicle_timestamp",
                "stop_id",
                "vehicle_id",
            ]
        )
        .lazy()
        .drop_nulls("vehicle_id")
        # FIXME: remove
        .collect()
    )
    logging.info(f"after explosion df shape {df.shape}")
    logging.info(f"after explosion df schema {df.schema}")
    logging.info(f"after explosion df head {df.head(8)}")
    df = (
        df.lazy()
        .with_columns(
            [
                pl.concat_str(
                    [
                        pl.col("authority_name"),
                        pl.col("vehicle_id"),
                        pl.col("start_date"),
                        pl.col("start_time"),
                        pl.col("route_id"),
                        pl.col("direction_id"),
                        pl.col("trip_id"),
                    ],
                    separator="-",
                ).alias("unique_trip_id"),
                pl.concat_str(
                    [
                        pl.col("authority_name"),
                        pl.col("vehicle_id"),
                        pl.col("start_date"),
                        pl.col("start_time"),
                        pl.col("route_id"),
                        pl.col("direction_id"),
                        pl.col("current_stop_sequence").cast(str).str.zfill(3),
                        pl.col("trip_id"),
                    ],
                    separator="-",
                ).alias("stop_sequence_id"),
                pl.concat_str(
                    [
                        pl.col("authority_name"),
                        pl.col("vehicle_id"),
                        pl.col("start_date"),
                        pl.col("start_time"),
                        pl.col("route_id"),
                        pl.col("direction_id"),
                        pl.col("current_stop_sequence").cast(str).str.zfill(3),
                        pl.col("current_status"),
                        pl.col("trip_id"),
                    ],
                    separator="-",
                ).alias("unique_sequence_id"),
                pl.col("message_timestamp")
                .mul(1_000)
                .cast(pl.Datetime(time_unit="ms", time_zone="UTC")),
                pl.col("start_date").str.strptime(pl.Date, "%Y%m%d"),
                pl.col("vehicle_timestamp")
                .mul(1_000)
                .cast(pl.Datetime(time_unit="ms", time_zone="UTC")),
            ]
        )
        .with_row_count()
        .pipe(
            keep_first_of_each_run,
            group_column="vehicle_id",
            run_column="unique_sequence_id",
        )
        .collect()
    )

    # FIXME:
    # Set the maximum length of strings displayed in the output
    pl.Config.set_fmt_str_lengths(200)

    # Set the maximum width of the table
    pl.Config.set_tbl_width_chars(300)

    logging.info(f"df shape is {df.shape}")
    logging.info(f"df schema is {df.schema}")
    logging.info(f"df head is {df.head(8)}")
    logging.info(
        f"df some is {df.sample(5).with_columns([pl.col('publish_time').sub(pl.col('vehicle_timestamp')).alias('time_diff')]).select('time_diff')}"
    )
    logging.info(
        f"df somf is {df.filter(pl.col('message_timestamp').is_not_null()).sample(5).select(['event_time', 'publish_time', 'message_timestamp', 'vehicle_timestamp', 'start_date'])}"
    )
    logging.info(
        f"df uniques is {df.head(5).select(['publish_time', 'message_timestamp', 'unique_sequence_id'])}"
    )
    return df


def parse_apc_json_the_other_way(possibly_json_string):
    counts = [
        {
            "schema_version": None,
            "counting_system_id": None,
            "apc_message_id": None,
            "apc_message_timestamp_string": None,
            "apc_timestamp_precision": None,
            "count_quality": None,
            "door": None,
            "count_class": None,
            "count_in": None,
            "count_out": None,
        }
    ]
    try:
        apc = json.loads(possibly_json_string)["APC"]
        vehicle_counts = apc["vehiclecounts"]
        counts = []
        if vehicle_counts is not None:
            for door_count in vehicle_counts["doorcounts"]:
                for class_count in door_count["count"]:
                    # timestamp = None
                    # timestamp_precision = "subsecond"
                    # try:
                    #    timestamp = (
                    #        datetime.datetime.strptime(
                    #            apc["tst"], "%Y-%m-%dT%H:%M:%S.%f%z"
                    #        ),
                    #    )
                    # except ValueError as value_err:
                    #    timestamp = (
                    #        datetime.datetime.strptime(
                    #            apc["tst"], "%Y-%m-%dT%H:%M:%S%z"
                    #        ),
                    #    )
                    #    timestamp_precision = "second"
                    counts.append(
                        {
                            "schema_version": apc["schemaVersion"],
                            "counting_system_id": apc.get(
                                "countingSystemId", None
                            ),
                            "apc_message_id": apc["messageId"],
                            "apc_message_timestamp_string": apc["tst"],
                            "count_quality": vehicle_counts["countquality"],
                            "door": door_count["door"],
                            "count_class": class_count["class"],
                            "count_in": class_count["in"],
                            "count_out": class_count["out"],
                        }
                    )
    except json.JSONDecodeError:
        if possibly_json_string:
            logging.info(f"Not JSON: {possibly_json_string}")
        else:
            logging.info("Not JSON and empty string")
    except BaseException as err:
        logging.error(
            f"Something went wrong while parsing APC JSON: {err=}, {type(err)=}"
        )
        raise
    logging.info(f"counts: {counts}")
    return counts


def calculate_message_counts(df):
    return (
        df.filter(pl.col("is_apc_message"))
        .sort("publish_time")
        .groupby_dynamic(
            "publish_time",
            every="1m",
            period="30m",
            # FIXME: maybe not?
            by=["counting_system_id", "count_class"],
            # FIXME: maybe not?
            include_boundaries=True,
        )
        # .agg(pl.count().inspect().eq(pl.lit(0)).inspect().alias("message_count_zero"))
        # .filter(pl.col("message_count_zero"))
        .agg(pl.count().alias("message_count"))
        .with_columns(
            [
                pl.col("publish_time").dt.date().alias("publish_time_date"),
                pl.col("publish_time").dt.time().alias("publish_time_time"),
            ]
        )
        # # .groupby("publish_time_time")
        # .groupby(["publish_time_time", "counting_system_id", "count_class"])
        # .agg(
        #     pl.col("message_count")
        #     .sum()
        #     .add(1)
        #     .log10()
        #     .alias("message_count_total_log")
        # )
        # .filter(pl.col("message_count_total") < 10)
        .sort("publish_time_time")
    )


def plot_message_count_detailed(df):
    df_pandas = df.to_pandas()

    fig = px.line(
        df_pandas,
        x="publish_time_time",
        y="message_count",
        facet_row="counting_system_id",
        facet_col="count_class",
        category_orders={"publish_time_time": df_pandas["publish_time_time"]},
        log_y=True,
        labels={
            "publish_time_time": "Publish time",
            "message_count": "Message count",
            "counting_system_id": "Counting system ID",
        },
        title="Message count per 30 minute slot",
    )
    fig.show()


def plot_message_count(df):
    df_pandas = (
        df.with_columns(
            [
                pl.col("_upper_boundary").dt.date().alias("operating_date"),
            ]
        )
        .sort(["operating_date", "counting_system_id"])
        .to_pandas()
    )
    fig = px.bar(
        df_pandas,
        x="operating_date",
        y="message_count",
        facet_row="counting_system_id",
        # FIXME: unnecessary
        # facet_col="count_class",
        # categoryorder="category ascending",
        # category_orders={"publish_time_time": df_pandas["publish_time_time"]},
        # facet_row=df4["publish_time_date"],
        log_y=True,
        labels={
            "operating_date": "Operating date",
            "message_count": "Message count",
        },
        title="Message count",
        # height=600,
    )
    fig.show()


def plot_diff_ratio(df):
    df_pandas = (
        df.with_columns(
            [
                pl.col("_upper_boundary").dt.date().alias("operating_date"),
            ]
        )
        .sort(["operating_date", "counting_system_id"])
        .to_pandas()
    )
    fig = px.bar(
        df_pandas,
        x="operating_date",
        # y=["sum_in", "sum_out"],
        # y="absolute_diff",
        y="diff_ratio",
        facet_row="counting_system_id",
        # FIXME: remove as not necessary
        # facet_col="count_class",
        log_y=True,
        color="is_in_greater_than_out",
        # categoryorder="category ascending",
        # category_orders={"publish_time_time": df_pandas["publish_time_time"]},
        # facet_row=df4["publish_time_date"],
        # color_discrete_sequence=px.colors.qualitative.Plotly,
        # barmode="group",
        labels={
            "diff_ratio": "Error ratio",
            "operating_date": "Operating date",
        },
        # title="Absolute difference of sum of incoming and sum of outgoing over the operating date",
        # title="Error: |∑IN - ∑OUT| over the operating date",
        title="Error: |∑IN - ∑OUT| / min(∑IN, ∑OUT) over the operating date",
        # height=600,
    )
    fig.show()


def plot_trip_errors(df):
    logging.info(f"df_pandas shape {df.shape}")
    logging.info(f"df_pandas schema {df.schema}")
    # df = df.filter(
    #    pl.col("vehicle_id") == "44517_160"
    # )
    df_pandas = df.to_pandas()
    fig = px.box(
        df_pandas,
        y="count_diff",
        x="start_date",
        color="counting_system_id",
        # log_y=True,
        # box=True,
        points="all",
        # points=False,
        hover_data=df.columns,
        # facet_row="vendor_name",
        facet_col="vehicle_id",
        labels={
            "count_diff": "(∑IN - ∑OUT) for each trip",
            "start_date": "Operating date",
        },
        title="Error: (∑IN - ∑OUT) for each trip, distributions for each operating date",
    )
    # fig.update_yaxes(matches=None)
    # fig = px.bar(
    #    df_pandas,
    #    x="operating_date",
    #    # y=["sum_in", "sum_out"],
    #    # y="absolute_diff",
    #    y="diff_ratio",
    #    facet_row="counting_system_id",
    #    # FIXME: remove as not necessary
    #    # facet_col="count_class",
    #    log_y=True,
    #    color="is_in_greater_than_out",
    #    # categoryorder="category ascending",
    #    # category_orders={"publish_time_time": df_pandas["publish_time_time"]},
    #    # facet_row=df4["publish_time_date"],
    #    # color_discrete_sequence=px.colors.qualitative.Plotly,
    #    # barmode="group",
    #    labels={
    #        "diff_ratio": "Error ratio",
    #        "operating_date": "Operating date",
    #    },
    #    # title="Absolute difference of sum of incoming and sum of outgoing over the operating date",
    #    # title="Error: |∑IN - ∑OUT| over the operating date",
    #    title="Error: |∑IN - ∑OUT| / min(∑IN, ∑OUT) over the operating date",
    #    # height=600,
    # )
    fig.show()


def create_counting_system_df(counting_system_map):
    counting_systems = collections.defaultdict(list)
    for counting_system_id, details in counting_system_map:
        splitted = details[0].split(":", maxsplit=2)
        authority_name = splitted[1]
        vehicle_id = splitted[2]
        vendor_name = details[1]
        counting_systems["counting_system_id"].append(counting_system_id)
        counting_systems["authority_name"].append(authority_name)
        counting_systems["vehicle_id"].append(vehicle_id)
        counting_systems["vendor_name"].append(vendor_name)
    df = pl.DataFrame(counting_systems)
    logging.info(f"counting_system_df is {df}")
    return df


def get_apc(
    data_root, subdirs, path_suffix, start_time, end_time, counting_system_df
):
    lf = get_data_by_suffix(data_root, subdirs, path_suffix)
    df = (
        lf.filter(
            pl.col("publish_time").is_between(start_time, end_time)
            # FIXME: figure out how to do disconnected dedup later
            & (
                # pl.col("payload").str.contains(r'^\s*\{\s*"APC"\s*:').is_not() |
                pl.col("payload").is_first()
            )
            # FIXME: empty string to null, also in download
        )
        .with_columns(
            [
                pl.col("mqtt_topic")
                .str.splitn(by="/", n=7)
                .struct.rename_fields(
                    [
                        "drop1",
                        "drop2",
                        "drop3",
                        "drop4",
                        "vendor",
                        "counting_system_id",
                        "mqtt_topic_suffix",
                    ]
                )
                .alias("topic_parts"),
                # pl.col("payload").apply(split_apc_json)
                ## .arr.explode()
                # .alias("apc_parts"),
            ]
        )
        .unnest(["topic_parts"])
        # .unnest(["topic_parts", "apc_parts"])
        .drop(["mqtt_topic", "drop1", "drop2", "drop3", "drop4"])
        # .filter(pl.col("counting_system_id").is_in(accepted_devices))
        # .explode("apc_parts")
        # .unnest("apc_parts")
        # FIXME remove unnecessary collects
        .collect(
            # # FIXME: Too hard
            # #apc_json_dtype = pl.Struct(pl.Field("payload_apc"[pl.Field("a", pl.Int64), pl.Field("b", pl.Boolean)])
            # df.select(pl.col("json").str.json_extract(apc_json_dtype))
            # df3 = df2.with_columns([
            #     pl.when(pl.col("payload").apply(is_json))
            #     .then(pl.col("payload").str.json_extract())
            #     .alias("payload_extracted")
            #     ]).unnest("payload_extracted").unnest("payload_apc")
            # ]).with_columns([
            #     pl.col("is_payload_json").on_true
            # ])
        )
        .with_columns(
            [
                pl.col("payload").apply(parse_apc_json).alias("apc_parts"),
            ]
        )
        .with_columns(
            [pl.col("apc_parts").is_not_null().alias("is_apc_message")]
        )
        .unnest("apc_parts")
        .explode(
            [
                "schema_version",
                "counting_system_id_from_json",
                "apc_message_id",
                "apc_message_timestamp_string",
                "count_quality",
                "door",
                "count_class",
                "count_in",
                "count_out",
            ]
        )
        # FIXME: investigate later
        # .with_columns(
        #    [
        #        pl.when(pl.col("apc_message_timestamp_string").is_not_null())
        #        .then(
        #            pl.col("apc_message_timestamp_string").str.strptime(
        #                # FIXME: use different %f, literal 'Z' and add explicit timezone afterwards
        #                # compare which formats have subsecond precision
        #                # https://docs.rs/chrono/latest/chrono/format/strftime/index.html#fn5
        #                pl.Datetime,
        #                fmt="%+",
        #                strict=True,
        #            )
        #        )
        #        .alias("apc_message_timestamp")
        #    ]
        # )
        # .drop("apc_message_timestamp_string")
        .join(counting_system_df, on="counting_system_id", how="inner")
    )
    logging.info(f"vehicle shape is {df.shape}")
    logging.info(f"vehicle schema is {df.schema}")
    return df


def run_unclean_vehicle_analysis(vehicle_lf, start_time, end_time):
    # FIXME: remove
    # logging.info(f"vehicle_lf height is {vehicle_lf.collect().height}")
    df = vehicle_lf.filter(
        pl.col("publish_time").is_between(start_time, end_time)
        # FIXME: figure out how to do disconnected dedup later
        & (
            # pl.col("payload").str.contains(r'^\s*\{\s*"APC"\s*:').is_not() |
            pl.col("payload").is_first()
        )
    )
    # logging.info(
    #    f"first payload {df.select('payload').head(1).collect().item()}"
    # )
    # FIXME: empty string to null, also in download
    df2 = (
        df.with_columns(
            [
                pl.col("mqtt_topic")
                .str.splitn(by="/", n=7)
                .struct.rename_fields(
                    [
                        "drop1",
                        "drop2",
                        "drop3",
                        "drop4",
                        "vendor",
                        "counting_system_id",
                        "mqtt_topic_suffix",
                    ]
                )
                .alias("topic_parts"),
                # pl.col("payload").apply(split_apc_json)
                ## .arr.explode()
                # .alias("apc_parts"),
            ]
        ).unnest(["topic_parts"])
        # .unnest(["topic_parts", "apc_parts"])
        .drop(["mqtt_topic", "drop1", "drop2", "drop3", "drop4"])
        # .filter(pl.col("counting_system_id").is_in(accepted_devices))
        # .explode("apc_parts")
        # .unnest("apc_parts")
        # FIXME remove unnecessary collects
    ).collect()
    # # FIXME: Too hard
    # #apc_json_dtype = pl.Struct(pl.Field("payload_apc"[pl.Field("a", pl.Int64), pl.Field("b", pl.Boolean)])
    # df.select(pl.col("json").str.json_extract(apc_json_dtype))
    # df3 = df2.with_columns([
    #     pl.when(pl.col("payload").apply(is_json))
    #     .then(pl.col("payload").str.json_extract())
    #     .alias("payload_extracted")
    #     ]).unnest("payload_extracted").unnest("payload_apc")
    # ]).with_columns([
    #     pl.col("is_payload_json").on_true
    # ])
    df3 = (
        df2.with_columns(
            [
                pl.col("payload").apply(parse_apc_json).alias("apc_parts"),
            ]
        )
        .with_columns(
            [pl.col("apc_parts").is_not_null().alias("is_apc_message")]
        )
        .unnest("apc_parts")
        .explode(
            [
                "schema_version",
                "counting_system_id_from_json",
                "apc_message_id",
                "apc_message_timestamp_string",
                "count_quality",
                "door",
                "count_class",
                "count_in",
                "count_out",
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("apc_message_timestamp_string").is_not_null())
                .then(
                    pl.col("apc_message_timestamp_string").str.strptime(
                        # FIXME: use different %f, literal 'Z' and add explicit timezone afterwards
                        # compare which formats have subsecond precision
                        # https://docs.rs/chrono/latest/chrono/format/strftime/index.html#fn5
                        pl.Datetime,
                        fmt="%+",
                        strict=True,
                    )
                )
                .alias("apc_message_timestamp")
            ]
        )
        .drop("apc_message_timestamp_string")
    )
    logging.info(
        f"first vendor is {df2.select(pl.col(['vendor'])).head(1).item()}"
    )
    logging.info(f"df2 height is {df2.height}")
    logging.info(f"df2 schema is {df2.schema}")
    logging.info(f"sample of df2 is {df2.sample(1)}")
    logging.info(f"df3 height is {df3.height}")
    logging.info(f"df3 schema is {df3.schema}")
    logging.info(f"sample of df3 is {df3.sample(10)}")

    df4 = calculate_message_counts(df3)

    logging.info(f"df4 height is {df4.height}")
    logging.info(f"df4 schema is {df4.schema}")
    logging.info(f"df4 firsts are {df4.head(10)}")

    plot_message_count_detailed(df4)

    # FIXME: move elsewhere
    # Visually a suitable split time is between 21:37 and 00:23+30min UTC.
    # 23:15 UTC so 02:15 Finnish time.
    # cut_off_time = datetime.time(23, 15, tzinfo=datetime.timezone.utc)

    df5 = (
        df3.filter(pl.col("is_apc_message"))
        .sort("publish_time")
        .groupby_dynamic(
            "publish_time",
            every="24h",
            offset="23h15m",
            # FIXME: maybe not?
            by=["counting_system_id", "count_class"],
            # FIXME: maybe not?
            include_boundaries=True,
        )
        # .agg(pl.count().inspect().eq(pl.lit(0)).inspect().alias("message_count_zero"))
        # .filter(pl.col("message_count_zero"))
        .agg(
            [
                pl.count().alias("message_count"),
                pl.col("count_in").sum().alias("sum_in"),
                pl.col("count_out").sum().alias("sum_out"),
            ]
        )
        .with_columns(
            [
                pl.col("sum_in")
                .sub(pl.col("sum_out"))
                .abs()
                .alias("absolute_diff"),
                pl.col("sum_in")
                .gt(pl.col("sum_out"))
                .alias("is_in_greater_than_out"),
                pl.col("sum_in")
                .sub(pl.col("sum_out"))
                .truediv(pl.min([pl.col("sum_in"), pl.col("sum_out")]))
                .abs()
                .alias("diff_ratio"),
                pl.col("message_count")
                .sub(pl.col("message_count").median())
                .abs()
                .alias("message_count_absolute_deviation"),
                # pl.col("publish_time").dt.date().alias("publish_time_date"),
                # pl.col("publish_time").dt.time().alias("publish_time_time"),
            ]
        )
        # #.groupby("publish_time_time")
        # .groupby(["publish_time_time", "counting_system_id"])
        # .agg(pl.col("message_count").sum().add(1).log10().alias("message_count_total_log"))
        # # .filter(pl.col("message_count_total") < 10)
        .sort(["_lower_boundary", "counting_system_id", "count_class"])
    )
    logging.info(f"df5 height is {df5.height}")
    logging.info(f"df5 schema is {df5.schema}")
    logging.info(f"df5 firsts are {df5.head(10)}")
    # logging.info(f"sample of df5 is {df5.sample(10)}")

    plot_message_count(df5)
    plot_diff_ratio(df5)

    # FIXME: plot message densities over time for each device, plot also 23:15 lines
    # FIXME: plot distribution of counts for each door, in/out (color/linetype) and device
    # FIXME: plot running sum of in and running sum of out over 5/15/30 min for each device (and door?)


# FIXME: later
# def adjust_final_stop_counts(df):
#     return (df
#         .groupby("unique_trip_id", maintain_order=True)
#     )


def sum_over_sequences(apc_df, gtfsrtvp_df):
    gtfsrtvp_df = gtfsrtvp_df.with_columns(
        [
            # FIXME: 20 seconds is hopefully enough
            (pl.col("publish_time") + pl.duration(seconds=20)).alias(
                "comparison_time"
            )
        ]
    ).sort(["vehicle_id", "comparison_time"])
    apc_df = apc_df.with_columns(
        [pl.col("publish_time").alias("comparison_time")]
    ).sort(["vehicle_id", "comparison_time"])

    combined_df = apc_df.join_asof(
        gtfsrtvp_df, on="comparison_time", by="vehicle_id", tolerance="30m"
    )

    logging.info(f"combined_df shape is {combined_df.shape}")
    logging.info(f"combined_df schema is {combined_df.schema}")
    logging.info(f"combined_df head is {combined_df.head(5)}")
    logging.info(f"combined_df sample is {combined_df.sample(5)}")
    agg_df = (
        combined_df.sort(["counting_system_id", "publish_time"])
        .groupby(
            ["counting_system_id", "stop_sequence_id", "count_class"],
        )
        .agg(
            [
                pl.col("count_in").sum().alias("sum_in"),
                pl.col("count_out").sum().alias("sum_out"),
                pl.first("start_date"),
                pl.first("unique_trip_id"),
                pl.first("authority_name"),
                pl.first("vehicle_id"),
                pl.first("vendor_name"),
            ]
        )
        .with_columns(
            [pl.col("sum_in").sub(pl.col("sum_out")).alias("count_diff")]
        )
        # FIXME: later
        # FIXME: Correct for last stop - first stop, second_stop
        # .pipe(adjust_final_stop_counts)
    )
    agg_df = (
        agg_df.groupby(
            ["counting_system_id", "unique_trip_id", "count_class"],
        )
        .agg(
            [
                pl.sum("count_diff"),
                pl.first("start_date"),
                pl.first("authority_name"),
                pl.first("vehicle_id"),
                pl.first("vendor_name"),
            ]
        )
        .with_columns(
            [
                pl.col("count_diff").abs().alias("count_diff_abs"),
                pl.col("count_diff").ge(0).alias("count_diff_nonnegative"),
            ]
        )
    )
    logging.info(f"agg_df shape is {agg_df.shape}")
    logging.info(f"agg_df schema is {agg_df.schema}")
    logging.info(f"agg_df head is {agg_df.head(5)}")
    logging.info(f"agg_df sample is {agg_df.sample(5)}")
    return agg_df


def run_analysis(data_root_path, counting_system_map):
    # Deal-Comp made another release.
    # start_time = datetime.date(2023, 4, 1)
    # start_time = datetime.datetime(
    #     2023, 3, 31, 23, 15, tzinfo=datetime.timezone.utc
    # )
    start_time = datetime.datetime(
        2023, 4, 4, 9, 15, tzinfo=datetime.timezone.utc
    )
    # start_time = datetime.datetime(
    #     2023, 4, 19, 5, 15, tzinfo=datetime.timezone.utc
    # )

    # # Year 2023
    # start_time = datetime.datetime(
    #     2022, 12, 31, 23, 15, tzinfo=datetime.timezone.utc
    # )

    # FIXME: remove
    # end_time = datetime.date(2023, 4, 11)
    end_time = datetime.datetime(
        2023, 5, 3, 23, 15, tzinfo=datetime.timezone.utc
    )
    # end_time = datetime.datetime(
    #   2023, 4, 24, 23, 15, tzinfo=datetime.timezone.utc
    # )
    # end_time = datetime.datetime(
    #     2023, 5, 3, 23, 15, tzinfo=datetime.timezone.utc
    # )
    # end_time = datetime.datetime.now(tz=datetime.timezone.utc)

    logging.info(f"start_time is {start_time} and end_time is {end_time}")

    counting_system_df = create_counting_system_df(counting_system_map)

    # # FIXME: remove?
    # interesting_vehicles = {
    #     "Kuopio": ["44517_6", "44517_160"],
    #     "Jyväskylä": ["6714_518", "6714_521"],
    # }

    # FIXME: remove?
    # accepted_devices = [
    #    "dmccla123",
    #    "JL518-APC",
    #    "JL521-APC",
    #    "KL006-APC",
    #    "KL160-APC",
    # ]

    data_root = pathlib.Path(data_root_path)
    dirs = [d for d in data_root.iterdir() if d.is_dir()]
    subdirs = [d.stem for d in dirs]

    # FIXME: testing
    # kuopio_lf = get_data_by_suffix(data_root, subdirs, "kuopio")
    # kuopio_lf = get_data_by_suffix(data_root, subdirs, "kuopioshort")
    # FIXME: just kuopio now
    # jyvaskyla_lf = get_data_by_suffix(data_root, subdirs, "jyvaskyla")
    # logging.info(f"kuopio_lf head is {kuopio_lf.head(5).collect()}")

    # vehicle_lf = get_data_by_suffix(data_root, subdirs, "vehicle")

    # run_unclean_vehicle_analysis(vehicle_lf, start_time, end_time)

    kuopio_df = get_gtfsrtvp(
        data_root,
        subdirs,
        "kuopioshort",
        start_time,
        end_time,
        counting_system_df.filter(
            pl.col("authority_name") == "kuopio"
        ).unique(),
    )
    jyvaskyla_df = get_gtfsrtvp(
        data_root,
        subdirs,
        "jyvaskylashort",
        start_time,
        end_time,
        counting_system_df.filter(
            pl.col("authority_name") == "jyvaskyla"
        ).unique(),
    )
    gtfsrtvp_df = (
        pl.concat(
            [
                kuopio_df,
                jyvaskyla_df,
            ],
            how="vertical",
        )
        .sort(["authority_name", "row_nr"])
        .with_row_count("global_row_nr")
        .drop("row_nr")
    )
    # FIXME: plot time diff distributions for jyvaskyla and kuopio, every hour
    apc_df = get_apc(
        data_root, subdirs, "vehicle", start_time, end_time, counting_system_df
    )
    summed_df = sum_over_sequences(apc_df, gtfsrtvp_df)
    plot_trip_errors(summed_df)
