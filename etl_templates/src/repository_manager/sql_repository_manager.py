import os
import stat
from pathlib import Path
from shutil import copytree, rmtree

from logtools import get_logger
from config import DevOpsConfig

from .file_sql_project import SqlProjEditor
from .repository_manager import RepositoryManager

logger = get_logger(__name__)


class SqlRepositoryManager(RepositoryManager):
    """Specialisatie van RepositoryManager voor SQL-projecten."""

    def __init__(self, config: DevOpsConfig):
        """
        Initialiseert een SqlRepositoryManager voor SQL-projecten.

        Deze constructor stelt de configuratie en het pad naar het SQL projectbestand in.

        Args:
            config: Configuratieobject voor de repository.
        """
        super().__init__(config)
        self._path_file_sql_project = config.path_file_sql_project
        self._work_item_description = config.work_item_description
        self._work_item = config.work_item

    def clean_target_dir_in_repo(self, target: str = "CentralLayer") -> None:
        """
        Verwijdert de opgegeven doeldirectory uit de repository.

        Deze methode probeert de opgegeven directory en alle inhoud te verwijderen.
        Indien nodig worden de rechten aangepast en fouten worden gelogd.

        Args:
            target (str, optional): Naam van de directory die verwijderd moet worden. Standaard "CentralLayer".

        Returns:
            None
        """
        dir_to_clean = self._path_local / target

        def onerror(func, path, _):
            try:
                os.chmod(path, stat.S_IWRITE)
                func(path)
            except Exception as e:
                logger.error(f"Failed to remove {path}: {e}")

        if dir_to_clean.exists():
            rmtree(dir_to_clean, onexc=onerror)

    def add_directory_to_repo(self, path_source: Path, target: str = "CentralLayer") -> None:
        """
        Voegt een directory met nieuwe bestanden toe aan de repository en werkt het SQL projectbestand bij.

        Deze methode kopieert alle bestanden uit de bronmap naar de doeldirectory in de repository,
        werkt het SQL projectbestand bij en logt de actie.

        Args:
            path_source (Path): De bronmap met toe te voegen bestanden.
            target (str, optional): Naam van de doeldirectory in de repository. Standaard "CentralLayer".

        Returns:
            None
        """
        path_sqlproj = self._path_local / self._path_file_sql_project

        if not path_source.exists() or not path_source.is_dir():
            raise ValueError(f"path_source '{path_source}' does not exist or is not a directory.")

        copytree(src=path_source, dst=self._path_local / target, dirs_exist_ok=True)

        project_editor = SqlProjEditor(path_sqlproj=path_sqlproj)
        project_editor.add_new_files(folder=path_source)
        project_editor.remove_missing_files()
        project_editor.save()
        logger.info("Added files to repository")

    def publish(self):
        commit_message = f"Commit: {self._work_item_description} #{int(self._work_item)}"
        return super().publish(commit_message)
