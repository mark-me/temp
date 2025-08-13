from dataclasses import dataclass
from pathlib import Path

from .base import BaseConfigComponent

@dataclass
class ExtractorConfigData:
    """Configuration settings for the Extractor.

    Specifies the folder for extractor output.
    """

    folder_output: str = "RETW"


class ExtractorConfig(BaseConfigComponent):
    def __init__(self, config: ExtractorConfigData, path_intermediate: Path):
        super().__init__(config)
        self.path_intermediate = path_intermediate

    @property
    def path_output(self) -> Path:
        """
        Geeft het pad naar de extractie-outputfolder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de tussenliggende outputfolder en de extractor folder uit de configuratie.

        Returns:
            Path: Het pad naar de extractie-outputfolder.
        """
        folder = self.path_intermediate / self._data.folder_output
        self.create_dir(folder)
        return folder