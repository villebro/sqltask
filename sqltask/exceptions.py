class SqlTaskException(Exception):
    pass


class TooFewRowsException(SqlTaskException):
    pass


class ExecutionArgumentException(SqlTaskException):
    pass
