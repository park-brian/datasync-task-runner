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

logging.basicConfig(filename='output.log', encoding='utf-8', level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

def load_module(module_name, filepath):
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_location(config: dict):
    response = {}

    if config["type"] == "efs":
        response = datasync_client.create_location_efs(
            Subdirectory=config["subdirectory"],
            EfsFilesystemArn=config["arn"],
            Ec2Config={
                "SubnetArn": config["ec2_subnet_arn"],
                "SecurityGroupArns": config["ec2_security_group_arns"],
            },
            Tags=config.get("tags", []),
        )

    elif config["type"] == "fsx_windows":
        response = datasync_client.create_location_fsx_windows(
            Subdirectory=config["subdirectory"],
            FsxFilesystemArn=config["arn"],
            SecurityGroupArns=config["security_group_arns"],
            Domain=config.get("domain", None),
            User=config["user"],
            Password=config["password"],
            Tags=config.get("tags", []),
        )

    elif config["type"] == "nfs":
        response = datasync_client.create_location_nfs(
            Subdirectory=config["subdirectory"],
            ServerHostname=config["hostname"],
            OnPremConfig={"AgentArns": config.get("agent_arns", [])},
            MountOptions={"Version": config.get("nfs_version", "AUTOMATIC")},
            Tags=config.get("tags", []),
        )

    elif config["type"] == "object_storage":
        response = datasync_client.create_location_object_storage(
            ServerHostname=config["hostname"],
            ServerPort=config.get("port", 443),
            ServerProtocol=config.get("protocol", "HTTPS"),
            Subdirectory=config["subdirectory"],
            BucketName=config["bucket"],
            AccessKey=config.get("access_key", None),
            SecretKey=config.get("secret_key", None),
            AgentArns=config.get("agent_arns", []),
            Tags=config.get("tags", []),
        )

    elif config["type"] == "s3":
        response = datasync_client.create_location_s3(
            S3BucketArn=config["arn"],
            S3StorageClass=config.get("storage_class", "STANDARD"),
            S3Config={"BucketAccessRoleArn": config.get("access_role_arn", None)},
            Subdirectory=config["subdirectory"],
            Tags=config.get("tags", []),
        )

    elif config["type"] == "smb":
        response = datasync_client.create_location_smb(
            ServerHostname=config["hostname"],
            Subdirectory=config["subdirectory"],
            Domain=config.get("domain", None),
            User=config["user"],
            Password=config["password"],
            AgentArns=config.get("agent_arns", []),
            MountOptions=config.get("smb_version", "AUTOMATIC"),
            Tags=config.get("tags", []),
        )

    return response.get("LocationArn", None)


def main(config_filepath):
    # check if file exists
    if not os.path.isfile(config_filepath):
        raise FileNotFoundError(config_filepath)

    # load configuration
    config = load_module("config", config_filepath).config()
    logging.info("loaded configuration")
    logging.info(pformat(config))

    # create resources if needed
    source_arn = config.get("source_arn", create_location(config["source"]))
    destination_arn = config.get(
        "destination_arn", create_location(config["destination"])
    )

    if source_arn is None or destination_arn is None:
        raise InvalidConfigError()

    # create task
    task_arn = datasync_client.create_task(
        SourceLocationArn=source_arn,
        DestinationLocationArn=destination_arn,
        CloudWatchLogGroupArn=config["cloudwatch_log_group_arn"],
        Excludes=config.get("excludes", [])
    ).get("TaskArn", None)
    logging.info(f"created task ({task_arn})")
    logging.info(f"waiting for task to become ready...")

    # wait until task has been created
    task_status = {}
    while task_status.get("Status", None) not in ["AVAILABLE", "UNAVAILABLE", "QUEUED"]:
        task_status = datasync_client.describe_task(TaskArn=task_arn)
        time.sleep(5)

    if task_status["Status"] == "UNAVAILABLE":
        raise InvalidArnException("The DataSync agent is unable to create the task")

    logging.info(f"starting task ({task_arn})...")

    # start/enqueue task
    task_execution_arn = datasync_client.start_task_execution(
        TaskArn=task_arn, Includes=config.get("includes", [])
    ).get("TaskExecutionArn", None)

    task_execution_status = {}

    # check task status
    start = time.time()
    while task_execution_status.get("Status", None) not in ["SUCCESS", "ERROR"]:
        duration = time.time() - start
        task_execution_status = datasync_client.describe_task_execution(
            TaskExecutionArn=task_execution_arn
        )
        logging.info("task execution status ({}s): {}".format(
            round(duration),
            task_execution_status["Status"])
        )
        time.sleep(5)

    # clean up locations
    logging.info("cleaning up...")
    datasync_client.delete_location(LocationArn=source_arn)
    datasync_client.delete_location(LocationArn=destination_arn)

    # send sns notification if topic is specified
    sns_topic_arn = config.get("sns_topic_arn", None)

    if sns_topic_arn is not None:
        # initialize sns topic
        sns_topic = sns.Topic(sns_topic_arn)
        start_time = task_execution_status["StartTime"].strftime("%Y-%m-%d %H:%M:%S")
        end_time = time.strftime("%Y-%m-%d %H:%M:%S")

        # send success notification
        if task_execution_status["Status"] == "SUCCESS":
            logging.info(f"sending success notification to sns topic ({sns_topic_arn})")
            with open("templates/success.txt") as f:
                message = f.read().format(
                    count=task_execution_status["FilesTransferred"],
                    source=source_arn,
                    destination=destination_arn,
                    start_time=start_time,
                    end_time=end_time,
                )

                sns_topic.publish(Subject="DataSync Success", Message=message)

        # send error notification
        elif task_execution_status["Status"] == "ERROR":
            logging.info(f"sending failure notification to sns topic ({sns_topic_arn})")
            with open("templates/failure.txt") as f:
                message = f.read().format(
                    count=task_execution_status["FilesTransferred"],
                    source=source_arn,
                    destination=destination_arn,
                    start_time=start_time,
                    end_time=end_time,
                    error=task_execution_status["Result"]["ErrorDetail"],
                )

                sns_topic.publish(Subject="DataSync Failure", Message=message)


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
