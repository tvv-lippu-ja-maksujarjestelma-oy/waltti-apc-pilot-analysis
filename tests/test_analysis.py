import polars as pl
import polars.testing as testing

from waltti_apc_pilot_analysis import analysis


def test_keep_first_of_each_run():
    df = pl.DataFrame(
        {
            "vehicle_id": [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
            ],
            "unique_sequence_id": [
                None,
                "foo-a",
                "foo-a",
                None,
                "bar-a",
                "bar-a",
                "bar-b",
                None,
                None,
                "foo-c",
                "foo-d",
                "foo-d",
                None,
            ],
            "data": list(range(20, 33)),
        }
    ).with_row_count("check_idx")
    expected_df = pl.DataFrame(
        {
            "check_idx": pl.Series(
                [0, 1, 3, 4, 6, 9, 10, 12], dtype=pl.UInt32
            ),
            "vehicle_id": [
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "foo",
                "foo",
                "bar",
            ],
            "unique_sequence_id": [
                None,
                "foo-a",
                None,
                "bar-a",
                "bar-b",
                "foo-c",
                "foo-d",
                None,
            ],
            "data": [20, 21, 23, 24, 26, 29, 30, 32],
        }
    )
    assert df.columns == expected_df.columns
    assert df.schema == expected_df.schema
    testing.assert_frame_equal(
        df.pipe(
            analysis.keep_first_of_each_run,
            group_column="vehicle_id",
            run_column="unique_sequence_id",
        ),
        expected_df,
    )
