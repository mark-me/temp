import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


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


@dataclass
class PowerDesignerConfigData:
    """Configuration settings for PowerDesigner.

    Holds the folder path and a list of PowerDesigner file names.
    """

    folder: str
    files: List[str] = field(default_factory=list)


@dataclass
class ExtractorConfigData:
    """Configuration settings for the Extractor.

    Specifies the folder for extractor output.
    """

    folder_output: str = "RETW"


@dataclass
class DeploymentMDDEConfigData:
    """Configuration settings MDDE deployment settings

    Specifies the output folder, input folder for creating Codelists .
    """

    folder_data: str = "etl_templates/input/codeList"
    schema: str = "MDDE"
    folder_output: str = "DA_MDDE"
    schemas_datamart: List[str] = field(default_factory=list)


@dataclass
class GeneratorConfigData:
    """Configuration settings for the Generator.

    Specifies the output folder, platform templates, and JSON file for created DDLs.
    """

    templates_platform: str
    folder_output: str = "Generator"

    @property
    def dir_templates(self) -> Path:
        root = "./etl_templates/src/generator/templates"
        dir_templates = Path(os.path.join(root, self.templates_platform))
        return dir_templates

    @property
    def dir_scripts_mdde(self) -> Path:
        root = "./etl_templates/src/generator/mdde_scripts"
        dir_scripts_mdde = Path(root)
        return dir_scripts_mdde


@dataclass
class DevOpsConfigData:
    """Configuration settings for DevOps.

    Specifies details for DevOps integration, including organization, project, repository, branch, and work item information.
    """

    organisation: str
    project: str
    repo: str
    branch: str
    work_item: str
    work_item_description: str
    file_sql_project: str
    folder: str = "GIT_repo"


@dataclass
class ConfigData:
    """Overall configuration settings.

    Combines all configuration settings for different components of the application.
    """

    title: str
    folder_intermediate_root: str
    ignore_warnings: bool = False
    power_designer: PowerDesignerConfigData = field(default_factory=PowerDesignerConfigData)
    extractor: ExtractorConfigData = field(default_factory=ExtractorConfigData)
    generator: GeneratorConfigData = field(default_factory=GeneratorConfigData)
    devops: DevOpsConfigData = field(default_factory=DevOpsConfigData)
    deployment_mdde: DeploymentMDDEConfigData = field(default_factory=DeploymentMDDEConfigData)
