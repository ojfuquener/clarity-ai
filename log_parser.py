import argparse
import datetime
import polars as pl


class LogParser:
    def __init__(self, file_path: str):
        """
        Initializes a LogParser object.

        Args:
            file_path (str): The path to the log file.

        Returns:
            LogParser: An instance of the LogParser class.
        """
        self.file_path = file_path
        self.df = self._parse_log_file()

    def _parse_log_file(self) -> pl.DataFrame:
        """
        Parses the log file using polars scan_csv function.

        Returns:
            pl.DataFrame: A polars DataFrame containing the log data.
        """
        df_log_file = pl.scan_csv(
            self.file_path,
            has_header=False,
            separator=" ",
            new_columns=["timestamp", "source_host", "target_host"],
        )
        return df_log_file

    def filter_connections_by_target_host_and_time_range(
        self,
        init_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
        hostname: str,
    ) -> pl.DataFrame:
        """
        Filters the log data for connections within the given time range and
        with the specified target hostname.

        Args:
            init_datetime (datetime.datetime): The initial datetime for filtering connections.
            end_datetime (datetime.datetime): The end datetime for filtering connections.
            hostname (str): The target hostname to filter connections.

        Returns:
            pl.DataFrame: A polars DataFrame containing the 'source_hosts' connected to
            the 'target_host' within the specified given period.
        """
        df_with_datetime = self.df.with_columns(
            pl.from_epoch("timestamp", time_unit="ms").alias("connected_at")
        )
        result = df_with_datetime.filter(
            (pl.col("connected_at").is_between(init_datetime, end_datetime)) &
            (pl.col("target_host") == hostname)
        )
        return result.select("source_host", "target_host", "connected_at").collect()


def parse_datetime(datetime_str: str) -> datetime.datetime:
    """
    Parses a datetime string into a datetime.datetime object.

    Args:
        datetime_str (str): The datetime string in format "YYYY-MM-DD HH:MM:SS.f".

    Returns:
        datetime.datetime: A datetime object representing the parsed datetime.
    """
    return datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")


def main():
    parser = argparse.ArgumentParser(
        description="Parse log files and filter connections."
    )
    parser.add_argument("--file_path", type=str, help="Path to the log file")
    parser.add_argument(
        "--init_datetime",
        type=parse_datetime,
        help='Initial datetime in format "YYYY-MM-DD HH:MM:SS.f"',
    )
    parser.add_argument(
        "--end_datetime",
        type=parse_datetime,
        help='End datetime in format "YYYY-MM-DD HH:MM:SS.f"',
    )
    parser.add_argument("--hostname", type=str, help="Hostname to filter connections")

    args = parser.parse_args()

    if not (args.init_datetime and args.end_datetime and args.hostname):
        print("Please provide init_datetime, end_datetime, and hostname.")
        return

    log_parser = LogParser(args.file_path)
    connected_hosts = log_parser.filter_connections_by_target_host_and_time_range(
        args.init_datetime, args.end_datetime, args.hostname
    )

    print(
        f"Hostnames connected to '{args.hostname}' during the given period: {args.init_datetime} - {args.end_datetime}:"
    )
    print(connected_hosts)


if __name__ == "__main__":
    main()
