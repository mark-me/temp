import os
from dataclasses import dataclass
from pathlib import Path

from .base import BaseConfigComponent

@dataclass
class GeneratorConfigData:
    """Configuration settings for the Generator.

    Specifies the output folder, platform templates, and JSON file for created DDLs.
    """

    templates_platform: str
    folder_output: str = "Generator"

    @property
    def dir_templates(self) -> Path:
        """
        Geeft het pad naar de map met templates voor de generator.
        Bepaalt het pad op basis van de standaardlocatie en het gekozen platform.

        Returns:
            Path: Het pad naar de map met templates.
        """
        root = Path("./etl_templates/src/generator/templates")
        dir_templates = root / self.templates_platform
        return dir_templates

    @property
    def dir_scripts_mdde(self) -> Path:
        """
        Geeft het pad naar de map met MDDE scripts voor de generator.
        Bepaalt het pad op basis van de standaardlocatie voor MDDE scripts.

        Returns:
            Path: Het pad naar de map met MDDE scripts.
        """
        root = "./etl_templates/src/generator/mdde_scripts"
        dir_scripts_mdde = Path(root)
        return dir_scripts_mdde

class GeneratorConfig(BaseConfigComponent):
    """
    Beheert de generator configuratie en outputpad voor gegenereerde scripts.
    Biedt toegang tot de relevante paden en instellingen voor scriptgeneratie op basis van de configuratie.
    """

    def __init__(self, config: GeneratorConfigData, path_intermediate: Path):
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

    @property
    def template_platform(self) -> str:
        """
        Geeft het platform voor de generator templates.
        Haalt de platformnaam op uit de configuratie.

        Returns:
            str: De naam van het platform voor de templates.
        """
        return self._data.templates_platform