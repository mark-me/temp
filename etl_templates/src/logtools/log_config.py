LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "format": "%(asctime)s %(levelname)s %(message)s %(module)s %(funcName)s %(process)d",
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
        },
        "colored": {
            "format": "\033[1m%(levelname)s\033[0m: %(message)s | \033[1mBestand:\033[0m '%(module)s' | \033[1mFunctie:\033[0m '%(funcName)s'",
            "()": "logtools.color_formatter.ColorFormatter",
        },
    },
    "handlers": {
        "tqdm_stdout": {
            "class": "logtools.tqdm_logging.TqdmLoggingHandler",  # Gebruik het juiste pad
            "formatter": "colored",
            "level": "DEBUG",  # of een andere drempel
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "json",
            "filename": "log.json",
            "maxBytes": 204800,
            "backupCount": 10,
        },
    },
    "loggers": {"": {"handlers": ["tqdm_stdout", "file"], "level": "WARNING"}},
}
