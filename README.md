# datasync-task-runner

### Prerequisites
- DataSync Agent
- python 3.6+

### Usage
```sh
pip3 install -r requirements.txt

python3 run-task.py --config-file configure-task.py

# optional: add daily job to crontab (preferably, use absolute paths)
0 23 * * * python3 run-task.py --config-file configure-task.py

# optional: run within a container
docker build -t datasync-task-runner .
docker run \
  -v $PWD/configure-task.example.py:/config.py:ro \
  -v ~/.aws:/root/.aws:ro \
  datasync-task-runner \
  ./run-task.py -c /config.py
```

### Example Configuration File (`configure-task.py`)

```python

def config():
    # assuming run-task.py is run once daily, create a task
    # which syncs a folder created using the YYYY-MM-DD naming convention
    yesterday = date.today() - timedelta(days=1)
    key = yesterday.strftime("%Y-%m-%d")

    return {
        "cloudwatch_log_group_arn": "my-cloudwatch-log-group-arn",
        "sns_topic_arn": "my-sns-topic-arn",
        "source": {
            "type": "nfs",
            "hostname": "my-nfs-hostname",
            "subdirectory": f"/source/directory/{key}",
            "agent_arns": [
                "my-nfs-agent-arn"
            ]
        }, 
        "destination": {
            "type": "s3",
            "arn": "my-s3-bucket-arn",
            "access_role_arn": "my-s3-bucket-access-role-arn",
            "subdirectory": f"/target/directory/{key}"
        }
    }

```

