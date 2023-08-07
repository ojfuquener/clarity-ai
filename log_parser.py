from datetime import datetime, timedelta, timezone
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

        """
        Add new column 'connected_at' transforming the timestamp to datetime to
        make it easy filter by dates.
        """
        df_logs_with_datetime = df_log_file.with_columns(
            pl.from_epoch("timestamp", time_unit="ms").alias("connected_at")
        )

        return df_logs_with_datetime

    def filter_connections_by_target_host_and_time_range(
        self,
        init_datetime: datetime,
        end_datetime: datetime,
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
        result = self.df.filter(
            (pl.col("connected_at").is_between(init_datetime, end_datetime)) &
            (pl.col("target_host") == hostname)
        )
        return result.select("source_host", "target_host", "connected_at").collect()

    def get_period_datetime(self, period_in_minutes: int) -> datetime:
        return datetime.now(timezone.utc) - timedelta(minutes=period_in_minutes)

    def get_hostames_connected_to_target_host(
        self, target_host: str, period_to_seek_logs: datetime
    ) -> pl.DataFrame:

        df_logs_into_period = self.df.filter(
            pl.col("connected_at") >= period_to_seek_logs.replace(tzinfo=None)
        )

        df_list_hosts_connected_to = df_logs_into_period\
            .filter(pl.col("target_host") == target_host)\
                .sort(["source_host"], descending=False)

        return df_list_hosts_connected_to.select(pl.col('source_host')).collect()

    def get_hostames_received_connections_from_source_host(
        self, source_host: str, period_to_seek_logs: datetime
    ) -> pl.DataFrame:

        df_logs_into_period = self.df.filter(
            pl.col("connected_at") >= period_to_seek_logs.replace(tzinfo=None)
        )

        df_list_hosts_connected_from = df_logs_into_period\
            .filter(pl.col("source_host") == source_host)\
                .sort(["target_host"], descending=False)

        return df_list_hosts_connected_from.select(pl.col('target_host')).collect()

    def get_hostames_with_more_connections_within_period(
        self, period_to_seek_logs: datetime
    ) -> pl.DataFrame:

        df_logs_into_period = self.df.filter(
            pl.col("connected_at") >= period_to_seek_logs.replace(tzinfo=None)
        )

        df_host_more_connections = df_logs_into_period.groupby(
            pl.col("source_host"))\
                .agg(pl.count().alias("max_connections"))\
                    .sort(["source_host", "max_connections"], descending=[False, True])\
                        .limit(1)

        return df_host_more_connections.collect()