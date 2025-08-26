import os
from dataclasses import dataclass
from pathlib import Path

from .base import BaseConfigComponent


@dataclass
class DevOpsConfigData:
    """Configuration settings for DevOps.

    Specifies details for DevOps integration, including organization, project, repository, branch, and work item information.
    """

    organisation: str
    project: str
    repo: str
    branch: str
    work_item: str = ""
    work_item_description: str = ""
    file_sql_project: str = ""
    folder: str = "GIT_repo"


class DevOpsConfig(BaseConfigComponent):
    """
    Beheert de DevOps configuratie en paden voor repository en branches.
    Biedt toegang tot DevOps instellingen zoals repository-URL's, branchnamen, werkitems en lokale paden op basis van de configuratie.
    """

    def __init__(self, config: DevOpsConfigData, path_output_root: Path):
        super().__init__(config)
        self._path_output_root = path_output_root

    @property
    def path_local(self) -> Path:
        """
        Geeft het pad naar de DevOps repository-folder voor deze configuratie.
        Bepaalt en maakt de directory aan op basis van de root en de devops folder uit de configuratie.

        Returns:
            Path: Het pad naar de repository-folder.
        """
        folder = Path(self._path_output_root) / self._data.folder
        self.create_dir(folder)
        return folder

    @property
    def branch(self) -> str:
        """
        Geeft de naam van de branch voor deze DevOps configuratie.
        Haalt de branchnaam op uit de configuratie.

        Returns:
            str: De naam van de branch.
        """
        return self._data.branch

    @property
    def feature_branch(self) -> str:
        user_login = os.getlogin().replace(' ', '_')
        descr_work_item = self._data.work_item_description.replace(' ', '_')
        return f"feature/{self._data.work_item}_{descr_work_item}_{user_login}"

    @property
    def url(self) -> str:
        return f"https://{self._data.organisation}@dev.azure.com/{self._data.organisation}/{self._data.project}/_git/{self._data.repo}"

    @property
    def url_check(self) -> str:
        return f"https://dev.azure.com/{self._data.organisation}/{self._data.project}/_git/{self._data.repo}"

    @property
    def url_branch(self) -> str:
        """De URL van de repository branch waar de wijzigingen in worden doorgevoerd"""
        user_login = os.getlogin().replace(' ', '_')
        descr_work_item = self._data.work_item_description.replace(' ', '_')
        return f"{self.url_check}?version=GBfeature%2F{self._data.work_item}_{descr_work_item}_{user_login}"

    @property
    def path_file_sql_project(self) -> Path:
        return Path(self._data.file_sql_project)

    @property
    def work_item_description(self):
        return self._data.work_item_description.replace(' ', '_')

    @property
    def work_item(self):
        return self._data.work_item
