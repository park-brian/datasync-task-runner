def config():
    return {
        "cloudwatch_log_group_arn": "my-cloudwatch-log-group-arn",
        "sns_topic_arn": "my-sns-topic-arn",
        "source": {
            "type": "nfs",
            "hostname": "my-nfs-hostname",
            "subdirectory": "/source/directory",
            "agent_arns": [
                "my-nfs-agent-arn"
            ]
        }, 
        "destination": {
            "type": "s3",
            "arn": "my-s3-bucket-arn",
            "access_role_arn": "my-s3-bucket-access-role-arn",
            "subdirectory": "/target/directory"
        }
    }
