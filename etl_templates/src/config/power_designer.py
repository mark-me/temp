import os
from dataclasses import dataclass, field
from pathlib import Path

from .base import BaseConfigComponent, ConfigFileError

@dataclass
class PowerDesignerConfigData:
    """Configuration settings for PowerDesigner.

    Holds the folder path and a list of PowerDesigner file names.
    """

    folder: str
    files: list[str] = field(default_factory=list)


class PowerDesignerConfig(BaseConfigComponent):
    def __init__(self, config: PowerDesignerConfigData):
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
        lst_pd_files = [
            Path(
                os.path.join(
                    self._data.folder,
                    pd_file,
                )
            )
            for pd_file in lst_pd_files
        ]
        if lst_missing := [str(file) for file in lst_pd_files if not file.exists()]:
            msg = f"Power Designer bestanden ontbreken: {', '.join(lst_missing)}"
            raise ConfigFileError(msg, 404)
        return lst_pd_files
