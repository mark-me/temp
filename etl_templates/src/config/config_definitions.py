import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DeploymentMDDEConfigData:
    """Configuration settings MDDE deployment settings

    Specifies the output folder, input folder for creating Codelists .
    """

    folder_data: str = "etl_templates/input/codeList"
    schema: str = "MDDE"
    folder_output: str = "DA_MDDE"
    schemas_datamart: list[str] = field(default_factory=list)


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
