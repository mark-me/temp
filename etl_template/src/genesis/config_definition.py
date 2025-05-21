import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class PowerDesignerConfig:
    """Configuration settings for PowerDesigner.

    Holds the folder path and a list of PowerDesigner file names.
    """

    folder: str
    files: List[str] = field(default_factory=list)


@dataclass
class ExtractorConfig:
    """Configuration settings for the Extractor.

    Specifies the folder for extractor output.
    """

    folder: str = "RETW"


@dataclass
class CodelistConfig:
    """Configuration settings for the Codelist.

    Specifies the output folder, input folder, and JSON file for created Codelists .
    """

    input_folder: str
    codeList_json: str
    folder: str = "CodeList"


@dataclass
class GeneratorConfig:
    """Configuration settings for the Generator.

    Specifies the output folder, platform templates, and JSON file for created DDLs.
    """

    templates_platform: str
    created_ddls_json: str
    folder_mdde_scripts: str

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

    folder: str = "Generator"

@dataclass
class PublisherConfig:
    """Configuration settings for the Publisher.

    Specifies various paths and settings related to publishing, including Visual Studio project details, code lists, and MDDE scripts.
    """

    vs_project_folder: str
    vs_project_file: str
    codeList_json: str
    codeList_folder: str
    
    folder: str = "GIT_repo"


@dataclass
class DevOpsConfig:
    """Configuration settings for DevOps.

    Specifies details for DevOps integration, including organization, project, repository, branch, and work item information.
    """

    organisation: str
    project: str
    repo: str
    branch: str
    work_item: str
    work_item_description: str

    @property
    def featurebranch(self) -> str:
        return (
            f"feature/{self.work_item}_{self.work_item_description.replace(' ', '_')}_{os.getlogin().replace(' ', '_')}"
        )

    @property
    def url(self) -> str:
        return f"https://{self.organisation}@dev.azure.com/{self.organisation}/{self.project}/_git/{self.repo}"

    @property
    def url_check(self) -> str:
        return (
            f"https://dev.azure.com/{self.organisation}/{self.project}/_git/{self.repo}"
        )

    @property
    def url_branch(self) -> str:
        """De URL van de repository branch waar de wijzigingen in worden doorgevoerd"""
        return f"https://dev.azure.com/{self.organisation}/{self.project}/_git/{self.repo}?version=GBfeature%2F{self.work_item}_{self.work_item_description.replace(' ', '_')}_{os.getlogin().replace(' ', '_')}"

    folder: str = "GIT_repo"


@dataclass
class ConfigData:
    """Overall configuration settings.

    Combines all configuration settings for different components of the application.
    """

    title: str
    folder_intermediate_root: str
    power_designer: PowerDesignerConfig = field(default_factory=PowerDesignerConfig)
    extractor: ExtractorConfig = field(default_factory=ExtractorConfig)
    generator: GeneratorConfig = field(default_factory=GeneratorConfig)
    publisher: PublisherConfig = field(default_factory=PublisherConfig)
    devops: DevOpsConfig = field(default_factory=DevOpsConfig)
    codelist: CodelistConfig = field(default_factory=CodelistConfig)
