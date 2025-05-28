import os
from shutil import copytree
import subprocess
import time
import webbrowser
from pathlib import Path

from logtools import get_logger

from .project_file import ProjectFile

logger = get_logger(__name__)


class RepositoryManager:
    """Handelt repository acties af zoals klonen, feature branches aanmaken, comitten en pushen naar de remote"""

    def __init__(self, config: dict):
        """
        Initialiseert de RepositoryHandler met repository parameters en een doel-directory.

        Args:
            params (dict): Dictionary die de repository parameters bevat zoals URL, branch, etc.
            dir_repository (str): Pad naar de locale repository directory.
        """
        self._config = config
        self._path_local = config.path_local.resolve()

    def clone(self):
        """
        Kloont de repository, maakt een feature-branch aan en schakelt hiernaar over.

        Deze functie verwijdert eerst een bestaande repository, kloont vervolgens de opgegeven repository,
        maakt een nieuwe feature-branch aan en schakelt hiernaar over. Indien nodig wordt de gebruiker gevraagd
        om in te loggen op DevOps.

        Returns:
            None
        """
        logger.info("Kloon van repository '{self.params.url}'.")
        dir_current = Path("./").resolve()
        self._remove_old_repo()  # deletes a directory and all its contents.
        # time.sleep(5)
        lst_command = [
            "git",
            "clone",
            self._config.url,
            "-b",
            self._config.branch,
            str(self._path_local),
        ]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command)
        logger.info(f"chdir to: {self._path_local}")
        os.chdir(self._path_local)
        lst_command = [
            "git",
            "branch",
            self._config.feature_branch,
            self._config.branch,
        ]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command)
        lst_command = ["git", "switch", self._config.feature_branch]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command)
        # Relocate to org root folder
        os.chdir(dir_current)

    def clone2(self):
        """
        Kloont de repository, maakt een feature-branch aan en schakelt hiernaar over.

        Deze functie verwijdert eerst een bestaande repository, kloont vervolgens de opgegeven repository,
        maakt een nieuwe feature-branch aan en schakelt hiernaar over. Indien nodig wordt de gebruiker gevraagd
        om in te loggen op DevOps.

        Returns:
            None
        """
        logger.info("Kloon van repository '{self.params.url}'.")
        dir_current = Path("./").resolve()
        self._remove_old_repo()  # deletes a directory and all its contents.
        time.sleep(5)
        for i in range(2):
            try:
                lst_command = [
                    "git",
                    "clone",
                    self._config.url,
                    "-b",
                    self._config.branch,
                    str(self._path_local),
                ]
                logger.info(f"Executed: {' '.join(lst_command)}")
                subprocess.run(lst_command)
                logger.info(f"chdir to: {self._path_local}")
                os.chdir(self._path_local)
                lst_command = [
                    "git",
                    "branch",
                    self._config.featurebranch,
                    self._config.branch,
                ]
                logger.info(f"Executed: {' '.join(lst_command)}")
                subprocess.run(lst_command)
                lst_command = ["git", "switch", self._config.featurebranch]
                logger.info(f"Executed: {' '.join(lst_command)}")
                subprocess.run(lst_command)
                i += 99
            except:
                logger.error(
                    "Er is wat mis gegaan. Waarschijnlijk moet je eerst inloggen op Devops. "
                )
                webbrowser.open(self._config.url_check, new=0, autoraise=True)
                logger.info(
                    "Wait timer for 15 seconds, to allow user to log in to DevOps"
                )
                time.sleep(15)
                continue
            else:
                break
        # Relocate to org root folder
        os.chdir(dir_current)

    def _remove_old_repo(self) -> None:
        """
        Verwijdert de opgegeven repository map en al zijn inhoud als deze bestaat voor een verse start.

        Deze functie zorgt ervoor dat alle bestanden en submappen in de repository map verwijderbaar zijn
        door de rechten aan te passen, en verwijdert vervolgens de volledige map.

        Returns:
            None
        """
        if not self._path_local.is_dir():
            return
        # change owner of file .idx, else we get an error
        for root, dirs, files in self._path_local.walk(top_down=False):
            for d in dirs:
                os.chmod((root / d), 0o777)
                (root / d).rmdir()
            for f in files:
                os.chmod((root / f), 0o777)
                (root / f).unlink()
        self._path_local.rmdir()
        logger.info(f"Delete existing folder: {self._path_local}")

    def publish(self) -> None:
        """
        Voert een commit en push uit naar de DevOps repository en opent de branch in de browser.

        Deze functie voegt alle wijzigingen toe, maakt een commit met een werkitem-omschrijving,
        pusht naar de feature-branch en opent de branch-URL in de browser.

        Returns:
            None
        """
        os.chdir(self._path_local)
        lst_command = [
            "git",
            "add",
            "-A",
        ]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command)
        lst_command = [
            "git",
            "commit",
            "-m"
            f"Commit: {self._config.work_item_description.replace(' ', '_')} #{int(self._config.work_item)}",
        ]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command)
        lst_command = ["git", "push", "origin", self._config.feature_branch]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command)

        # Open browser to check Commit tot DevOps
        webbrowser.open(self._config.url_branch, new=0, autoraise=True)

    def add_directory_to_repo(self, path_source: Path):
        # Add files not, currently found in the repository, to the project file
        lst_files_new = self._find_files_new(path_source=path_source)
        project_file = ProjectFile(
            dir,
            path_repository=self.path_repository,
            path_file_project=self.config.path_vs_project_file,
        )
        project_file.publish()
        # Copy all files to repository
        copytree(src=path_source, dst=self._path_local, dirs_exist_ok=True)

    def _find_files_new(self, path_source: Path) -> list:
        """
        Zoekt naar bestanden die wel in de bronmap staan, maar nog niet in de repository.

        Deze functie vergelijkt de bestanden in de bronmap met die in de repository en retourneert een lijst van nieuwe bestanden.

        Args:
            path_source (Path): De bronmap waarin gezocht wordt naar nieuwe bestanden.

        Returns:
            list: Een lijst met bestandsnamen die nieuw zijn in de bronmap en nog niet in de repository staan.
        """
        lst_files_new = []
        lst_generated = list(path_source.rglob("*"))
        lst_generated = [str(file) for file in lst_generated]
        lst_generated = [file.replace(f"{path_source}/", '') for file in lst_generated]

        lst_repository = self._path_local.rglob("*")
        lst_repository = [str(file) for file in lst_repository]
        lst_repository = [file.replace(f"{self._path_local}/", '') for file in lst_repository]

        lst_files_new = [file for file in lst_generated if file not in lst_repository]
        return lst_files_new

