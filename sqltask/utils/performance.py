import logging
import os

logger = logging.getLogger(__name__)


is_developer_mode = os.getenv("SQLTASK_DEVELOPER_MODE", False)
if is_developer_mode:
    logger.info("Developer mode enabled")
