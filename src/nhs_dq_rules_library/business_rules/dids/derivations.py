from pyspark.sql import functions as F


def days_between_dates(end_date, start_date):
    """
    Takes 2 date columns and determines the number of whole days between them.

    Args:
    end_date: Column containing end dates
    start_date: Column containing start dates

    Returns:
       Column with duration in days
    """

    duration = F.when(
        end_date.isNotNull() & start_date.isNotNull(), F.datediff(end_date, start_date)
    ).otherwise(None)
    return duration


def first_non_null_col(column1, column2):
    """
    Takes 2 columns and returns the first non-null value.
    If all are null returns null.

    Args:
        column1: A column to return if not null
        column2: A column to return if not null and column1 is null

    Returns:
        Column: First non-null value (or null if all inputs are null)
    """
    # Unpack the list of columns as individual arguments to F.coalesce()
    return F.coalesce(column1, column2)


def time_between_timestamps_hms(end_time, start_time):
    """
    Takes 2 timestamps and determines the number of hours, minutes and seconds between them (hh:mm:ss).
    Handles negative durations by prefixing with '-'.

    Args:
        end_time: The end time (tmstmp) in 'YYYY-MM-DDTHH:MM:SS+/-0/1' format
        start_time: The start time (tmstmp) in 'YYYY-MM-DDTHH:MM:SS+/-0/1' format

    Returns:
       Duration in hours, minutes and seconds
    """

    seconds_diff = F.when(
        end_time.isNotNull() & start_time.isNotNull(),
        F.unix_timestamp(end_time) - F.unix_timestamp(start_time),
    ).otherwise(None)

    abs_seconds = F.abs(seconds_diff)
    sign = F.when(seconds_diff < 0, F.lit("-")).otherwise(F.lit(""))

    duration = F.when(
        seconds_diff.isNotNull(),
        F.concat(
            sign,
            F.format_string("%02d", (abs_seconds / 3600).cast("integer")),  # Hours
            F.lit(":"),
            F.format_string(
                "%02d", ((abs_seconds % 3600) / 60).cast("integer")
            ),  # Minutes
            F.lit(":"),
            F.format_string("%02d", (abs_seconds % 60).cast("integer")),  # Seconds
        ),
    ).otherwise(None)
    return duration


def timestamp_to_date(timestamp_col):
    """
    Takes a timestamp and converts to a date.

    Args:
        timestamp_col: The column (tmstmp) in 'YYYY-MM-DDTHH:MM:SS+/-0/1' format

    Returns:
       Date in YYYY-MM-DD format
    """

    return F.to_date(timestamp_col)
