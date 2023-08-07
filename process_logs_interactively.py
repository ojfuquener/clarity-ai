import argparse
import datetime
import logging

from log_parser import LogParser


logging.basicConfig(format='Clarity-AI :: %(asctime)s :: %(levelname)s :: %(message)s', level=logging.INFO)


def parse_datetime(datetime_str: str, date_format: str="%Y-%m-%d %H:%M:%S.%f") -> datetime.datetime:
    """
    Parses a datetime string into a datetime.datetime object.

    Args:
        datetime_str (str): The datetime string in specified format.
        date_format (str): The datetime format, by default "YYYY-MM-DD HH:MM:SS.f".

    Returns:
        datetime.datetime: A datetime object representing the parsed datetime.
    """
    return datetime.datetime.strptime(datetime_str, date_format)


def main():
    parser = argparse.ArgumentParser(
        description="Parse log files and filter connections."
    )
    parser.add_argument(
        "--file_path", 
        type=str, 
        help="Path to the log file")
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

    if not (args.file_path and args.init_datetime and args.end_datetime and args.hostname):
        logging.warning("Please provide file_path, init_datetime, end_datetime and hostname.")
        return

    log_parser = LogParser(args.file_path)
    connected_hosts = log_parser.filter_connections_by_target_host_and_time_range(
        args.init_datetime, args.end_datetime, args.hostname
    )

    logging.info(
        f"Hostnames connected to '{args.hostname}' during the given period: \
            {args.init_datetime} - {args.end_datetime}: {connected_hosts}"
    )


if __name__ == "__main__":
    main()