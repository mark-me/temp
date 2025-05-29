import os
from shutil import copytree
import subprocess
import time
import webbrowser
from pathlib import Path

from logtools import get_logger

from .file_sql_project import SqlProjEditor

logger = get_logger(__name__)


class RepositoryManager:
    """Handelt repository acties af zoals klonen, feature branches aanmaken, comitten en pushen naar de remote"""

    def __init__(self, config: dict):
        """Initialiseert de RepositoryManager met de opgegeven configuratie.

        Stelt de lokale repositorypad en configuratie in op basis van de meegegeven parameters.

        Args:
            config (dict): Configuratieobject met repository-instellingen.
        """
        self._config = config
        self._path_local = config.path_local.resolve()

    def clone(self) -> None:
        """
        Kloont de repository, maakt een feature-branch aan en schakelt hiernaar over.

        Deze functie verwijdert eerst een bestaande repository, kloont vervolgens de opgegeven repository,
        maakt een nieuwe feature-branch aan en schakelt hiernaar over. Indien nodig wordt de gebruiker gevraagd
        om in te loggen op DevOps.

        Returns:
            None
        """
        logger.info(f"Kloon van repository '{self._config.url}'.")
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
        try:
            subprocess.run(lst_command, check=True)
        except subprocess.CalledProcessError:
            logger.error(f"Failed to clone repository: {self._config.url}")
            raise
        logger.info(f"chdir to: {self._path_local}")
        os.chdir(self._path_local)
        lst_command = [
            "git",
            "branch",
            self._config.feature_branch,
            self._config.branch,
        ]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command, check=True)
        lst_command = ["git", "switch", self._config.feature_branch]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command, check=True)
        # Relocate to org root folder
        os.chdir(dir_current)

    def clone2(self) -> None:
        """
        Kloont de repository, maakt een feature-branch aan en schakelt hiernaar over.

        Deze functie verwijdert eerst een bestaande repository, kloont vervolgens de opgegeven repository,
        maakt een nieuwe feature-branch aan en schakelt hiernaar over. Indien nodig wordt de gebruiker gevraagd
        om in te loggen op DevOps.

        Returns:
            None
        """
        logger.info(f"Kloon van repository '{self.params.url}'.")
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
                subprocess.run(lst_command, check=True)
                logger.info(f"chdir to: {self._path_local}")
                os.chdir(self._path_local)
                lst_command = [
                    "git",
                    "branch",
                    self._config.feature_branch,
                    self._config.branch,
                ]
                logger.info(f"Executed: {' '.join(lst_command)}")
                subprocess.run(lst_command, check=True)
                lst_command = ["git", "switch", self._config.feature_branch]
                logger.info(f"Executed: {' '.join(lst_command)}")
                subprocess.run(lst_command, check=True)
                i += 99
            except OSError as e:
                logger.error(
                    f"Er is wat mis gegaan. Waarschijnlijk moet je eerst inloggen op Devops. Foutmelding: {e}"
                )
                webbrowser.open(self._config.url_check, new=0, autoraise=True)
                logger.info(
                    "Wacht 15 seconden, zodat de gebruiker kan inloggen op DevOps"
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

    def add_directory_to_repo(self, path_source: Path, paths_post_deployment: list[Path]) -> None:
        """
        Voegt een directory met nieuwe bestanden en post-deployment scripts toe aan de repository.

        Zoekt naar nieuwe bestanden in de bronmap, werkt het projectbestand bij en kopieert alle bestanden naar de lokale repository.

        Args:
            path_source (Path): De bronmap met te publiceren bestanden.
            paths_post_deployment (list[Path]): Lijst met paden naar post-deployment scripts die toegevoegd moeten worden.

        Returns:
            None
        """
        lst_files_new = self._find_files_new(path_source=path_source)
        lst_files_build = [file for file in lst_files_new if file not in paths_post_deployment]
        project_editor = SqlProjEditor(path_sqlproj=self.config.path_vs_project_file)

        project_editor.publish()
        # Copy all files to repository
        copytree(src=path_source, dst=self._path_local, dirs_exist_ok=True)

    def _find_files_new(self, path_source: Path) -> list[Path]:
        """
        Zoekt naar bestanden die wel in de bronmap staan, maar nog niet in de repository.

        Deze functie vergelijkt de relatieve paden van bestanden in de bronmap met die in de repository
        en retourneert een lijst van bestanden die nog niet aanwezig zijn in de repository.

        Args:
            path_source (Path): De bronmap waarin gezocht wordt naar nieuwe bestanden.

        Returns:
            list[Path]: Een lijst met relatieve paden van bestanden die nieuw zijn.
        """
        # Genereer relatieve paden van bestanden in de bronmap
        files_in_source = {
            file.relative_to(path_source)
            for file in path_source.rglob("*")
            if file.is_file()
        }

        # Genereer relatieve paden van bestanden in de repository
        files_in_repo = {
            file.relative_to(self._path_local)
            for file in self._path_local.rglob("*")
            if file.is_file()
        }

        # Bepaal welke bestanden nog niet in de repository staan
        new_files = list(files_in_source - files_in_repo)
        return new_files

    def publish(self) -> None:
        """
        Voert een commit en push uit naar de DevOps repository en opent de branch in de browser.

        Deze functie voegt alle wijzigingen toe, maakt een commit met een werkitem-omschrijving,
        pusht naar de feature-branch en opent de branch-URL in de browser.

        Returns:
            None
        """
        os.chdir(self._path_local)
        self._git_add_all()
        self._git_commit()
        self._git_push()
        self._open_branch_in_browser()

    def _git_add_all(self):
        """
        Voegt alle gewijzigde, nieuwe en verwijderde bestanden toe aan de git staging area.

        Deze functie voert een 'git add -A' uit om alle wijzigingen voor commit voor te bereiden.

        Returns:
            None
        """
        lst_command = [
            "git",
            "add",
            "-A",
        ]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command, check=True)

    def _git_commit(self):
        """
        Voert een git commit uit met een werkitem-omschrijving als commit message.

        Deze functie maakt een commit van alle toegevoegde wijzigingen met een beschrijving van het werkitem.

        Returns:
            None
        """
        lst_command = [
            "git",
            "commit",
            "-m"
            f"Commit: {self._config.work_item_description.replace(' ', '_')} #{int(self._config.work_item)}",
        ]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command, check=True)

    def _git_push(self):
        """
        Voert een git push uit naar de feature-branch van de remote repository.

        Deze functie pusht de lokale wijzigingen naar de opgegeven feature-branch op de remote repository.

        Returns:
            None
        """
        lst_command = ["git", "push", "origin", self._config.feature_branch]
        logger.info(f"Executed: {' '.join(lst_command)}")
        subprocess.run(lst_command, check=True)

    def _open_branch_in_browser(self):
        """
        Opent de branch-URL in de browser om de commit in DevOps te controleren.

        Deze functie opent de URL van de subprocess.run(lst_command, cwd=self._path_local)
        feature-branch in de standaard webbrowser.

        Returns:
            None
        """
        webbrowser.open(self._config.url_branch, new=0, autoraise=True)
