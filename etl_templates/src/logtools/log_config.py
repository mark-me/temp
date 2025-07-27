LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "format": "%(asctime)s %(levelname)s %(message)s %(module)s %(funcName)s %(process)d",
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
        },
        "stdout": {
            "format": "%(levelname)s | %(message)s | bestand: '%(module)s' | functie: '%(funcName)s'",
            #"class": "pythonjsonlogger.jsonlogger.JsonFormatter",
        },
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "json",
            "filename": "log.json",
            "maxBytes": 204800,
            "backupCount": 10,
        },
    },
    "loggers": {"": {"handlers": ["stdout", "file"], "level": "WARNING"}},
}
