from datetime import date, timedelta
from os import path

"""
Sample configuration to:
- Update the metadata for an existing nfs location
- Update a task's metadata
- Execute a task which syncs the source files to the destination
"""


def configure_task():
    """Updates the """
    return {
        "TaskArn": "arn:aws:datasync:<region>:<account>:task/<task-id>",
        "SourceLocation": {
            "LocationArn": "arn:aws:datasync:<region>:<account>:location/<location-id>",
            "Type": "nfs",
            "Config": {"Subdirectory": "/new/source/subdirectory/"},
        },
        "Options": {"PreserveDeletedFiles": "REMOVE"},
    }
