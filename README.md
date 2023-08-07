# How to Execute a Python Scripts

To execute a Python script that uses the Polars library, follow the steps below:

There are 2 python scripts:

1. process_logs_interactively.py: This script lets to the user gets a list of hostnames connected to the given host during the given period in an interactive way.
2. process_logs_background.py: Once this scripts it's executed in background, lets to the user get the insights described below based on a configured parameters: 
    - A list of hostnames connected to a given (configurable) host during the last hour.
    - A list of hostnames received connections from a given (configurable) host during the last hour.
    - The hostname that generated most connections in the last hour.

To execute any of the aformentioned scripts, follow the step described below:

## Step 1: Create and activate Python virtual environment

Make sure you have Python installed on your system. Python 3.10 has been used to implement the script's code and virtual environ has been created via `venv`.

Command to create Virtual environ:

```
python3 -m venv /path/to/new/virtual/environment
```

Command to activate Virtual environ:

```
source /path/to/new/virtual/environment/bin/activate
```


## Step 2: Install Polars Library

Once the virtual environ has been activated you can install Polars and its required dependencies using the `pip` package manager executing the following command:

```
pip install -r requirements.txt
```

This will download and install the Polars library along with its dependencies.

## Step 3: Execute the Python Script

Once you have the Python script ready and Polars library installed, you can execute the script. Open a terminal or command prompt and navigate to the directory where your Python script is located.

To run the Python script, follow the steps described below:

### 3.1. Execute the script Interactively

```
python3 process_logs_interactively.py --file_path <log filepath> --init_datetime <Initial date in format YYYY-MM-DD HH:MM:SS.f> --end_datetime <End date in format YYYY-MM-DD HH:MM:SS.f> --hostname <host name>
```

For instance:

```
python3 process_logs_interactively.py --file_path ./input-file-10000.txt --init_datetime '2019-08-10 21:00:04.351' --end_datetime '2019-08-14 22:01:00.341' --hostname 'Mayarose'
```


### 3.2. Execute the script in background

```
python3 process_logs_background.py
```

This script reads a configuration file which has the below parameters:

- PERIOD_IN_MINUTES_TO_SEEK_LOGS = Number of minutes that comprise the window of time to lookup the logs records. It takes the current date in GMT and subtracts the configured minutes. By default: 60 min.
- HOST_NAME_CONNECTED_TO = Name of the target host that receives connections from the source hosts.
- HOST_NAME_CONNECTED_FROM = Name of the source host that it's connected to the target hosts. 
- LOGS_DIRECTORY = The path of the directory where the log files are stored continuously. By default: ./logs_directory/
- LOGS_PROCESSED_TRACKING_FILE = The file path where the names of the processed log files are stored to avoid reading them again. By default: ./track_processed_files/processed_files.txt

### Recommendation: In both ways to execute the scripts, be aware that the log files must have records that have been generated within the datetime range specified in the parameters.

## Step 5: Observe the Output

The Python script will be executed, and you should see the output or results (if any) in the terminal or command prompt.

## Additional Notes

- Ensure that you have the necessary permissions to execute the Python script and that it's located in a directory where you have appropriate access.

- If you encounter any issues during the installation of the Polars library or while executing the Python script, double-check that Python and Polars are installed correctly, and refer to the official documentation for troubleshooting.


That's it! You should now be able to execute your Python scripts that uses the Polars library and observe the results of the logs processing.