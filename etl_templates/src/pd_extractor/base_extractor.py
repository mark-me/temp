from typing import Any

from logtools import get_logger

logger = get_logger(__name__)


class BaseExtractor:
    def __init__(self, file_pd_ldm: str):
        self.file_pd_ldm = file_pd_ldm

    def _get_nested(self, data: dict, keys: list[str], default=None) -> Any:
        """Zoekt naar een geneste waarde in een dictionary op basis van een lijst van sleutels.

        Deze functie doorloopt de opgegeven sleutels en retourneert de bijbehorende geneste waarde als deze bestaat, anders de default waarde.

        Args:
            data (dict): De dictionary waarin gezocht wordt.
            keys (list[str]): Lijst van sleutels die de geneste structuur aangeven.
            default (Any, optional): Waarde die wordt geretourneerd als de sleutels niet gevonden worden. Standaard None.

        Returns:
            Any: De gevonden geneste waarde of de default waarde.
        """
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    def _has_nested(self, data: dict, keys: list[str]) -> bool:
        """Controleert of een reeks geneste sleutels aanwezig is in een dictionary.

        Deze functie retourneert True als alle opgegeven sleutels bestaan in de geneste structuur van de dictionary, anders False.

        Args:
            data (dict): De dictionary waarin gezocht wordt.
            keys (list[str]): Lijst van sleutels die genest aanwezig moeten zijn.

        Returns:
            bool: True als alle sleutels aanwezig zijn, anders False.
        """
        return self._get_nested(data, keys, default=object()) is not object()
