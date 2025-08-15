from dataclasses import dataclass
from pathlib import Path

from .base import BaseConfigComponent

@dataclass
class IntegratorConfigData:
    """Configuration settings for the Integrator.

    Specifies the folder for Integrator output.
    """

    folder_output: str = "RETW"


class IntegratorConfig(BaseConfigComponent):
    """
    Beheert de Integrator configuratie en outputpad voor extractieresultaten.
    Biedt toegang tot de relevante paden en instellingen voor extractie op basis van de configuratie.
    """

    def __init__(self, config: IntegratorConfigData, path_intermediate: Path):
        """
        Initialiseert een IntegratorConfig met de opgegeven configuratie en tussenliggende pad.
        Slaat de configuratie en het pad op voor gebruik bij het bepalen van outputlocaties voor extractie.

        Args:
            config (IntegratorConfigData): De Integrator configuratiegegevens.
            path_intermediate (Path): Het pad naar de tussenliggende outputfolder.
        """
        super().__init__(config)
        self.path_intermediate = path_intermediate

    @property
    def path_output(self) -> Path:
        """
        Geeft het pad naar de extractie-outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de tussenliggende outputfolder en de Integrator folder uit de configuratie.

        Returns:
            Path: Het pad naar de extractie-outputfolder.
        """
        folder = self.path_intermediate / self._data.folder_output
        self.create_dir(folder)
        return folder