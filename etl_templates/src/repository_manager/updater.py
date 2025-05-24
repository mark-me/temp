from pathlib import Path
import shutil

from .publisher import DDLPublisher  # FIXME: Should probably be part of this?

class RepoUpdater:
    """Adds generated scripts, post deployment and data to repository
    """
    def __init__(self):
        pass