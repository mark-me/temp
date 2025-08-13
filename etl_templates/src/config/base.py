import os
from pathlib import Path


class ConfigFileError(Exception):
    """Exception raised for configuration file errors."""

    def __init__(self, message, error_code):
        """
        Initialiseert een ConfigFileError met een foutmelding en foutcode.
        Deze exceptie wordt gebruikt om fouten in het configuratiebestand te signaleren.

        Args:
            message (str): De foutmelding.
            error_code (int): De bijbehorende foutcode.
        """
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

    def __str__(self):
        """
        Retourneert de string-representatie van de ConfigFileError.
        Geeft de foutmelding samen met de foutcode terug.

        Returns:
            str: De foutmelding en foutcode als string.
        """
        return f"{self.message} (Error Code: {self.error_code})"


class BaseConfigComponent:
    def __init__(self, config):
        self._data = config

    def create_dir(self, path: Path) -> None:
        """
        Maakt de opgegeven directory aan als deze nog niet bestaat.
        Controleert of het pad een bestand is en converteert het naar een director-ypad indien nodig.

        Args:
            dir_path (Path): Het pad naar de directory die aangemaakt moet worden.
        """
        if path.is_file():
            path = os.path.dirname(path)
        Path(path).mkdir(parents=True, exist_ok=True)
