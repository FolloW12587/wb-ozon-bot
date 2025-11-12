class OzonProductExistsError(Exception):
    pass


class OzonAPICrashError(Exception):
    pass


class OzonAPIAttemptsExceeded(Exception):
    pass


class OzonAPIParseError(Exception):
    pass


class WbAPICrashError(Exception):
    pass


class WbProductExistsError(Exception):
    pass


class NotEnoughGraphicData(Exception):
    pass


class Forbidden(Exception):
    pass
