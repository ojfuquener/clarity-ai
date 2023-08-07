import os
import datetime
import logging
import configparser
import time

from log_parser import LogParser


logging.basicConfig(
    format="Clarity-AI :: %(asctime)s :: %(levelname)s :: %(message)s",
    level=logging.INFO,
)


def parse_datetime(
    datetime_str: str, date_format: str = "%Y-%m-%d %H:%M:%S.%f"
) -> datetime.datetime:
    """
    Parses a datetime string into a datetime.datetime object.

    Args:
        datetime_str (str): The datetime string in specified format.
        date_format (str): The datetime format, by default "YYYY-MM-DD HH:MM:SS.f".

    Returns:
        datetime.datetime: A datetime object representing the parsed datetime.
    """
    return datetime.datetime.strptime(datetime_str, date_format)


def get_config(config_path: str):
    """
    Read the configuration file where background log processing parameter are defined.

    Args:
        config_path (str): The path to the configuration file.

    Returns:
        config: A dict with configuration parameters.
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def get_insights_from_log_file(log_filename: str):
    """
    Parses the given log file to extract insights and logs the results.

    Args:
        log_filename (str): The name of the log file to be processed.

    Returns:
        None
    """
    log_parser = LogParser(log_filename)

    period_to_seek_logs = log_parser.get_period_datetime(PERIOD_IN_MINUTES_TO_SEEK_LOGS)
    logging.info(period_to_seek_logs)
    list_hosts_connected_to = log_parser.get_hostames_connected_to_target_host(
        HOSTNAME_CONNECTED_TO, period_to_seek_logs
    )
    logging.info(
        f"A list of hostnames connected to a {HOSTNAME_CONNECTED_TO} \
        host during the last {PERIOD_IN_MINUTES_TO_SEEK_LOGS} minutes: {list_hosts_connected_to}"
    )

    list_hosts_connected_from = (
        log_parser.get_hostames_received_connections_from_source_host(
            HOSTNAME_CONNECTED_FROM, period_to_seek_logs
        )
    )
    logging.info(
        f"A list of hostnames connected from {HOSTNAME_CONNECTED_TO} \
        host during the last {PERIOD_IN_MINUTES_TO_SEEK_LOGS} minutes: {list_hosts_connected_from}"
    )

    host_with_more_connections = (
        log_parser.get_hostames_with_more_connections_within_period(period_to_seek_logs)
    )
    logging.info(
        f"Hostname with more connections in the last {PERIOD_IN_MINUTES_TO_SEEK_LOGS} \
        minutes: {host_with_more_connections}"
    )
    

def read_logs(directory, tracking_file):
    """
    Reads log files generated continuously in the configured directory, processes new log files,
    and extracts insights from each new log file using the `get_insights_from_log_file` function.
    The processed filenames are tracked in the given tracking file to avoid processing duplicates.

    Args:
        directory (str): The directory path where log files are generated continuously.
        tracking_file (str): The name of the file to track the processed filenames.

    Returns:
        None
    """
    processed_log_files = set()

    # Check if the processed file exists, and read its content
    if os.path.exists(tracking_file):
        with open(tracking_file, "r") as f:
            processed_log_files = set(line.strip() for line in f)

    # Read and process only new log files
    for log_filename in os.listdir(directory):
        if log_filename not in processed_log_files:
            # Process the new log file and get insights
            get_insights_from_log_file(os.path.join(directory, log_filename))
            # Add the filename to the processed set
            processed_log_files.add(os.path.join(directory, log_filename))

    # Write the updated set of processed filenames back to the tracking file
    with open(tracking_file, "w") as f:
        f.write("\n".join(processed_log_files))
    

if __name__ == "__main__":
    process_config = get_config("./config/background_processing.ini")
    PERIOD_IN_MINUTES_TO_SEEK_LOGS = int(
        process_config["log_processing"]["PERIOD_IN_MINUTES_TO_SEEK_LOGS"]
    )
    HOSTNAME_CONNECTED_TO = process_config["log_processing"]["HOST_NAME_CONNECTED_TO"]
    HOSTNAME_CONNECTED_FROM = process_config["log_processing"]["HOST_NAME_CONNECTED_FROM"]
    LOGS_DIRECTORY = process_config["log_processing"]["LOGS_DIRECTORY"]
    TRACKING_FILE = process_config["log_processing"]["LOGS_PROCESSED_TRACKING_FILE"]

    logging.info(PERIOD_IN_MINUTES_TO_SEEK_LOGS)
    logging.info(HOSTNAME_CONNECTED_TO)
    logging.info(HOSTNAME_CONNECTED_FROM)
    logging.info(LOGS_DIRECTORY)
    logging.info(TRACKING_FILE)

    while True:
        read_logs(LOGS_DIRECTORY, TRACKING_FILE)
        time.sleep(PERIOD_IN_MINUTES_TO_SEEK_LOGS)
