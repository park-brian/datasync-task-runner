#!/usr/bin/python3

import importlib.util
import os.path
import logging
import time
import boto3
from pprint import pformat
from botocore.exceptions import InvalidConfigError
from botocore.utils import InvalidArnException

sns = boto3.resource("sns")
datasync_client = boto3.client("datasync")

logging.basicConfig(filename="output.log", encoding="utf-8", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


def load_module(module_name, filepath):
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def update_location(options: dict):
    location_arn = options.get("LocationArn")

    if location_arn is None:
        raise InvalidConfigError("Please specify a location ARN")

    type = options["Type"]
    config = options["Config"]
    config.update(LocationArn=location_arn)

    if type == "nfs":
        datasync_client.update_location_nfs(**config)

    elif type == "object_storage":
        datasync_client.update_location_object_storage(**config)

    elif config["type"] == "smb":
        datasync_client.update_location_smb(**config)

    else:
        raise InvalidConfigError("Please specify a valid location type")

    return location_arn


def create_location(options):
    type = options["Type"]
    config = options["Config"]

    if type == "efs":
        return datasync_client.create_location_efs(**config)["LocationArn"]

    elif type == "fsx_windows":
        return datasync_client.create_location_fsx_windows(**config)["LocationArn"]

    elif type == "nfs":
        return datasync_client.create_location_nfs(**config)["LocationArn"]

    elif type == "object_storage":
        return datasync_client.create_location_object_storage(**config)["LocationArn"]

    elif type == "s3":
        return datasync_client.create_location_s3(**config)["LocationArn"]

    elif config["type"] == "smb":
        return datasync_client.create_location_smb(**config)["LocationArn"]

    else:
        raise InvalidConfigError("Please specify a valid location type")


def get_location(options):
    if options["LocationArn"]:
        return update_location(options)
    else:
        return create_location(options)


def create_task(config: dict):
    task_config = {
        key: config[key]
        for key in config.keys()
        & {
            "CloudWatchLogGroupArn",
            "Name",
            "Options",
            "Excludes",
            "Schedule",
            "Tags",
        }
    }

    task_config.update(
        SourceLocationArn=get_location(config["SourceLocation"]),
        DestinationLocationArn=get_location(config["DestinationLocation"]),
    )

    return datasync_client.create_task(**task_config).get("TaskArn")


def update_task(config: dict):
    task_config = {
        key: config[key]
        for key in config.keys()
        & {
            "TaskArn",
            "CloudWatchLogGroupArn",
            "Name",
            "Options",
            "Excludes",
            "Schedule",
        }
    }

    for location in [config.get("SourceLocation"), config.get("DestinationLocation")]:
        if location is not None:
            update_location(location)

    datasync_client.update_task(**task_config)
    return task_config.get("TaskArn")


def main(config_filepath, task_check_interval=5):
    # check if file exists
    if not os.path.isfile(config_filepath):
        raise FileNotFoundError(config_filepath)

    # load configuration
    config = load_module("config", config_filepath)
    task_config = config.configure_task()
    logging.info("loaded configuration")
    logging.info(pformat(task_config))

    # invoke before_task_configuration
    if not config.before_task_configuration():
        return False

    if config["TaskArn"]:
        task_arn = update_task(task_config)
        logging.info(f"updated task ({task_arn})")
    else:
        task_arn = create_task(task_config)
        logging.info(f"created task ({task_arn})")

    logging.info(f"waiting for task to become ready...")

    # wait until task has been created
    task_status = {}
    while task_status.get("Status") not in ["AVAILABLE", "UNAVAILABLE", "QUEUED"]:
        task_status = datasync_client.describe_task(TaskArn=task_arn)
        time.sleep(task_check_interval)

    if task_status["Status"] == "UNAVAILABLE":
        raise InvalidArnException("The DataSync agent is unable to create the task")

    # invoke before_task_execution
    if not config.before_task_execution(task_status):
        return False

    logging.info(f"starting task ({task_arn})...")

    # start/enqueue task
    task_execution_config = {"TaskArn": task_arn}

    if config.get("Includes"):
        task_execution_config["Includes"] = config["Includes"]

    task_execution_arn = datasync_client.start_task_execution(
        **task_execution_config
    ).get("TaskExecutionArn", None)

    task_execution_status = {}

    # check task status
    start = time.time()
    while task_execution_status.get("Status", None) not in ["SUCCESS", "ERROR"]:
        duration = time.time() - start
        task_execution_status = datasync_client.describe_task_execution(
            TaskExecutionArn=task_execution_arn
        )

        logging.info(
            "task execution status ({}s): {}".format(
                round(duration), task_execution_status["Status"]
            )
        )

        # invoke before_task_execution
        if not config.during_task_execution(task_status, task_execution_status):
            return False

        time.sleep(task_check_interval)

    # invoke after_task_execution
    config.after_task_execution(task_status, task_execution_status)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Execute a DataSync task with the given configuration"
    )
    parser.add_argument(
        "-c",
        "--config-file",
        required=True,
        help="A python configuration file for the DataSync task",
    )

    args = parser.parse_args()
    main(args.config_file)
