import os


def is_developer_mode() -> bool:
    """
    Check if developer mode is activated.

    :return: True if developer mode is active, otherwise False
    """
    return False if os.getenv("SQLTASK_DEVELOPER_MODE") is None else True
