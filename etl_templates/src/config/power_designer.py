from dataclasses import dataclass, field
from pathlib import Path

from .base import BaseConfigComponent, ConfigFileError

@dataclass
class PowerDesignerConfigData:
    """
    Bevat configuratiegegevens voor PowerDesigner-bestanden.
    Geeft de map en de lijst met PowerDesigner-bestanden die gebruikt worden in de applicatie.

    """

    folder: str
    files: list[str] = field(default_factory=list)


class PowerDesignerConfig(BaseConfigComponent):
    """
    Beheert de Power Designer configuratie en controleert de aanwezigheid van opgegeven bestanden.
    Biedt toegang tot de relevante paden en instellingen voor PowerDesigner-bestanden op basis van de configuratie.
    """

    def __init__(self, config: PowerDesignerConfigData):
        """
        Initialiseert een PowerDesignerConfig met de opgegeven configuratie.
        Slaat de configuratie op voor gebruik bij het controleren en ophalen van PowerDesigner-bestanden.

        Args:
            config (PowerDesignerConfigData): De PowerDesigner configuratiegegevens.
        """
        super().__init__(config)

    @property
    def files(self) -> list[Path]:
        """
        Geeft een lijst van paden naar de PowerDesigner-bestanden die in de configuratie zijn opgegeven.
        Controleert of alle opgegeven bestanden bestaan en geeft anders een foutmelding.

        Returns:
            list[Path]: Een lijst van Path-objecten naar de PowerDesigner-bestanden.

        Raises:
            ConfigFileError: Als een of meer PowerDesigner-bestanden ontbreken.
        """
        lst_pd_files = self._data.files
        folder_path = Path(self._data.folder)
        lst_pd_files = [
            folder_path / pd_file
            for pd_file in lst_pd_files
        ]
        if lst_missing := [str(file) for file in lst_pd_files if not file.exists()]:
            msg = f"Power Designer bestanden ontbreken: {', '.join(lst_missing)}"
            raise ConfigFileError(msg, 404)
        return lst_pd_files
