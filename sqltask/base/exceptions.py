class SqlTaskException(Exception):
    pass


class MandatoryValueMissingException(SqlTaskException):
    pass


class TooFewRowsException(SqlTaskException):
    pass


class ExecutionArgumentException(SqlTaskException):
    pass
