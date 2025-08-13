from dataclasses import dataclass, field
from pathlib import Path

from .base import BaseConfigComponent


@dataclass
class DeploymentMDDEConfigData:
    """Configuration settings MDDE deployment settings

    Specifies the output folder, input folder for creating Codelists .
    """

    folder_data: str = "etl_templates/input/codeList"
    schema: str = "MDDE"
    folder_output: str = "DA_MDDE"
    schemas_datamart: list[str] = field(default_factory=list)


class DeploymentMDDEConfig(BaseConfigComponent):
    """
    Beheert de MDDE deployment configuratie en paden voor output en input.
    Biedt toegang tot de relevante paden en instellingen voor MDDE deployment op basis van de configuratie.

    """

    def __init__(self, config: DeploymentMDDEConfigData, path_intermediate: Path):
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
    def schema(self) -> str:
        """
        Geeft de naam van het schema voor MDDE deployment.
        Haalt de schemawaarde op uit de configuratie.

        Returns:
            str: De naam van het schema.
        """
        return self._data.schema

    @property
    def path_data_input(self) -> Path:
        """
        Geeft het pad naar de inputfolder voor codelists voor MDDE deployment.
        Haalt het pad op uit de configuratie.

        Returns:
            Path: Het pad naar de inputfolder voor codelists.
        """
        return Path(self._data.folder_data)

    @property
    def schemas_datamart(self) -> list[str]:
        """
        Geeft de lijst van datamart schemas voor MDDE deployment.
        Haalt de lijst van schemas op uit de configuratie.

        Returns:
            list[str]: De lijst van datamart schemas.
        """
        return self._data.schemas_datamart
