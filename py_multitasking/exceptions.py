class NotSubmittedError(RuntimeError):
    pass


class AlreadySubmittedError(RuntimeError):
    pass


class NoSuchObjectError(RuntimeError):
    pass


class NotAvailableError(ValueError):
    pass


class AlreadyDestroyedError(RuntimeError):
    pass
