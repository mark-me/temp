# log_manager.py
import logging
import logging.config
from .log_config import LOGGING
from .issue_tracking import IssueTrackingHandler

# Apply the logging config once
logging.config.dictConfig(LOGGING)

# Set up issue tracker handler (shared across all modules)
issue_tracker = IssueTrackingHandler()
logging.getLogger().addHandler(issue_tracker)


def get_logger(name: str) -> logging.Logger:
    """Haalt een logger-instantie op aan de hand van een naam.

    Deze functie vereenvoudigt het ophalen van een logger en zorgt voor
    een consistente configuratie volgens de logginginstellingen van het project.

    Args:
        name: De naam van de logger die opgehaald moet worden.

    Retourneert:
        Een logger-instantie met de opgegeven naam.
    """
    return logging.getLogger(name)
