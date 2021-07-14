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

```

### Example Configuration File (`configure-task.py`)

```python

def config():
    yesterday = date.today() - timedelta(days=1)
    key = yesterday.strftime("%Y-%m-%d")

    return {
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

