import os
import stat
import subprocess
import webbrowser
from pathlib import Path
from shutil import rmtree
from logtools import get_logger

logger = get_logger(__name__)


class RepositoryError(Exception):
    """Exception die wordt opgegooid bij repository fouten."""

    def __init__(self, message: str, path_repo: Path):
        super().__init__(message)
        self.message = message
        self.path_repo = path_repo

    def __str__(self):
        return f"{self.message} voor {self.path_repo}"


class RepositoryManager:
    """Generieke repository manager met git-acties."""

    def __init__(
        self, path_local: Path, url: str, branch: str, feature_branch: str = None
    ):
        self._path_local = Path(path_local).resolve()
        self._url = url
        self._branch = branch
        self._feature_branch = feature_branch

    def pull(self) -> None:
        """
        Haalt de laatste wijzigingen van de remote repository op of kloont de repository indien nodig.

        Deze methode voert een 'git pull' uit als de lokale repository geldig is, of kloont de repository als deze nog niet aanwezig is of ongeldig is.

        Returns:
            None
        """
        if self._status_repo_folder() == "ok":
            self._execute(
                ["git", "-C", str(self._path_local), "pull", "origin", self._branch]
            )
        else:
            self.clone()

    def clone(self) -> None:
        """
        Kloont de repository naar het lokale pad en verwijdert indien nodig een bestaande repository.

        Deze methode controleert of er al een geldige repository aanwezig is en verwijdert deze indien nodig.
        Vervolgens wordt de opgegeven repository gekloond naar het lokale pad.

        Returns:
            None
        """
        if self._status_repo_folder():
            self._remove_old_repo()
        self._execute(
            ["git", "clone", self._url, "-b", self._branch, str(self._path_local)]
        )

    def create_branch(self) -> None:
        """
        Maakt een nieuwe feature branch aan en schakelt hiernaar over.

        Deze methode verwijdert eerst een bestaande remote branch met dezelfde naam,
        maakt vervolgens een nieuwe branch aan vanaf de huidige branch en schakelt naar deze feature branch.
        Als er geen feature branch is ingesteld, wordt een RepositoryError opgegooid.

        Raises:
            RepositoryError: Als er geen feature branch is ingesteld.
        """
        if not self._feature_branch:
            raise RepositoryError("Geen feature branch ingesteld", self._path_local)
        self._remove_remote_branch()
        self._execute(
            [
                "git",
                "-C",
                str(self._path_local),
                "branch",
                self._feature_branch,
                self._branch,
            ]
        )
        self.switch_branch()

    def switch_branch(self) -> None:
        """
        Schakelt over naar de ingestelde feature branch als deze niet leeg is.

        Deze methode voert een 'git switch' uit naar de feature branch indien deze is opgegeven.

        Returns:
            None
        """
        if self._feature_branch != "":
            self._execute(
                ["git", "-C", str(self._path_local), "switch", self._feature_branch]
            )

    def publish(self, commit_message: str, open_url: str | None = None) -> None:
        """
        Voert een commit en push uit naar de repository en opent optioneel een URL in de browser.

        Deze methode voegt alle wijzigingen toe, maakt een commit met het opgegeven bericht, pusht naar
        de (feature-)branch en opent optioneel een URL in de webbrowser.

        Args:
            commit_message (str): Het commitbericht voor de commit.
            open_url (str | None, optional): Een URL die na het pushen in de browser wordt geopend. Standaard None.

        Returns:
            None
        """
        self._execute(["git", "-C", str(self._path_local), "add", "-A"])
        self._execute(
            ["git", "-C", str(self._path_local), "commit", "-m", commit_message]
        )
        self._execute(
            [
                "git",
                "-C",
                str(self._path_local),
                "push",
                "origin",
                self._feature_branch or self._branch,
            ]
        )
        if open_url:
            webbrowser.open(open_url, new=0, autoraise=True)

    def _remove_old_repo(self) -> None:
        """
        Verwijdert de bestaande lokale repository map en al zijn inhoud.

        Deze methode controleert of de lokale repository directory bestaat. Indien aanwezig, worden alle bestanden en mappen verwijderd, waarbij de rechten indien nodig worden aangepast. Fouten bij het verwijderen worden gelogd.

        Returns:
            None
        """
        if not self._path_local.is_dir():
            return

        def onerror(func, path, _):
            try:
                os.chmod(path, stat.S_IWRITE)
                func(path)
            except Exception as e:
                logger.error(f"Failed to remove {path}: {e}")

        rmtree(self._path_local, onexc=onerror)
        logger.info(f"Deleted folder: {self._path_local}")

    def _remove_remote_branch(self) -> None:
        """
        Verwijdert de remote feature branch uit de repository indien deze bestaat.

        Deze methode probeert de remote feature branch te verwijderen. Als de branch niet bestaat, wordt dit gelogd.

        Returns:
            None
        """
        try:
            self._execute(
                [
                    "git",
                    "-C",
                    str(self._path_local),
                    "push",
                    "origin",
                    "--delete",
                    self._feature_branch,
                ]
            )
        except subprocess.CalledProcessError:
            logger.info("Remote branch bestaat nog niet")

    def _status_repo_folder(self) -> str:
        """
        Controleert de status van de lokale repository directory en remote.

        Deze methode controleert of de lokale directory bestaat, of het een geldige git repository is,
        en of de remote origin overeenkomt met de verwachte URL. Geeft een string terug die de status beschrijft.

        Returns:
            str: "no_directory" als de directory niet bestaat, "no_repository" als het geen geldige repository is,
                 "no_remote" als de remote origin niet gevonden kan worden, "ok" als alles overeenkomt,
                 of "not_same_repo" als de remote origin niet overeenkomt met de verwachte URL.
        """
        if not self._path_local.is_dir():
            return "no_directory"
        try:
            self._execute(["git", "-C", str(self._path_local), "rev-parse"])
        except subprocess.CalledProcessError:
            return "no_repository"
        try:
            origin = (
                subprocess.check_output(
                    [
                        "git",
                        "-C",
                        str(self._path_local),
                        "config",
                        "--get",
                        "remote.origin.url",
                    ]
                )
                .strip()
                .decode()
            )
        except subprocess.CalledProcessError:
            return "no_remote"
        return "ok" if self._url == origin else "not_same_repo"

    def _execute(self, cmd: list[str]) -> None:
        """
        Voert een shell-commando uit en logt het uitgevoerde commando.

        Deze methode logt het commando en voert het uit met subprocess.run. Als het commando faalt, wordt een CalledProcessError opgegooid.

        Args:
            cmd (list[str]): De lijst met commando-argumenten die uitgevoerd moeten worden.

        Returns:
            None

        Raises:
            subprocess.CalledProcessError: Als het commando niet succesvol wordt uitgevoerd.
        """
        logger.info(f"Executing: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
