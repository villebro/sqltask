import logging
import os

logger = logging.getLogger(__name__)


@property
def is_developer_mode():
    return os.getenv("SQLTASK_DEVELOPER_MODE", False)
