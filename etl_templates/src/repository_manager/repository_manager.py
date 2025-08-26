import os
import stat
import subprocess
import webbrowser
from pathlib import Path
from shutil import rmtree

from config import DevOpsConfig
from logtools import get_logger

logger = get_logger(__name__)


class RepositoryError(Exception):
    """
    Exception die wordt opgegooid bij repository fouten.

    Deze exceptie wordt gebruikt om fouten te signaleren die optreden bij repository-operaties,
    zoals het ontbreken van een geldige repository of een mismatch in remote origin.
    """

    def __init__(self, message: str, path_repo: Path | None = None):
        super().__init__(message)
        self.message = message
        self.path_repo = path_repo

    def __str__(self):
        if self.path_repo:
            return f"{self.message} voor {self._path_local}"
        else:
            return self.message


class RepositoryManager:
    """Generieke repository manager voor git-acties."""

    def __init__(self, config: DevOpsConfig):
        self._path_local = config.path_local.resolve()
        self._url = config.url
        self._branch = config.branch
        self._feature_branch = config.feature_branch

    def pull(self) -> None:
        """
        Haalt de laatste wijzigingen van de remote repository op of kloont de repository indien nodig.

        Deze methode voert een 'git pull' uit als de lokale repository geldig is, of kloont de repository als
        deze nog niet aanwezig is of ongeldig is.

        Returns:
            None
        """
        if self._has_local_repo():
            self._execute(
                ["git", "-C", str(self._path_local), "pull", "origin", self._branch]
            )
        else:
            self.clone()

    def clone(self) -> None:
        """
        Kloont de repository naar het lokale pad of voert een pull uit als de repository nog niet aanwezig is.

        Deze methode controleert of er al een geldige repository aanwezig is en kloont deze indien nodig.
        Als er geen geldige repository is, wordt een pull uitgevoerd om de repository op te halen.

        Returns:
            None
        """
        if self._has_local_repo():
            self._execute(
                ["git", "clone", self._url, "-b", self._branch, str(self._path_local)]
            )
        else:
            self.pull()

    def create_feature_branch(self) -> None:
        """
        Maakt een nieuwe feature branch aan en schakelt hiernaar over.

        Deze methode verwijdert eerst een bestaande remote branch met dezelfde naam,
        maakt vervolgens een nieuwe branch aan vanaf de huidige branch en schakelt naar deze feature branch.
        Als er geen feature branch is ingesteld, wordt een RepositoryError opgegooid.

        Raises:
            RepositoryError: Als er geen feature branch is ingesteld.
        """
        if not self._feature_branch:
            raise RepositoryError("Geen feature branch ingesteld")
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
        self.switch_branch(branch="feature")

    def switch_branch(self, branch: str) -> None:
        """
        Schakelt over naar de opgegeven branch in de repository.

        Deze methode voert een 'git switch' uit naar de base branch of feature branch, afhankelijk van de parameter.
        Als de branch niet bestaat of niet is opgegeven, wordt een RepositoryError opgegooid.

        Args:
            branch (str): De naam van de branch om naar over te schakelen ("base" of "feature").

        Returns:
            None

        Raises:
            RepositoryError: Als de branch niet bestaat of niet is opgegeven.
        """
        if branch == "base":
            self._execute(
                ["git", "-C", str(self._path_local), "switch", self._branch]
            )
        elif branch == "feature":
            if self._feature_branch:
                self._execute(
                    ["git", "-C", str(self._path_local), "switch", self._feature_branch]
                )
            else:
                raise RepositoryError(
                    message=f"Branch '{self._feature_branch}' is niet gevonden",
                    path_repo=self._path_local,
                )
        else:
            raise RepositoryError(
                message="Geen branch gekozen (feature of base)."
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

    def remove_old_repo(self) -> None:
        """
        Verwijdert de bestaande lokale repository map en al zijn inhoud.

        Deze methode controleert of de lokale repository directory bestaat. Indien aanwezig,
        worden alle bestanden en mappen verwijderd, waarbij de rechten indien nodig worden aangepast.
        Fouten bij het verwijderen worden gelogd.

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

    def _has_local_repo(self) -> bool:
        """
        Controleert of de lokale directory een geldige git repository is met de juiste remote origin.

        Deze methode controleert of de directory bestaat, of het een geldige git repository is, en of
        de remote origin overeenkomt met de verwachte URL.
        Gooit een RepositoryError als een van deze checks faalt.

        Returns:
            bool: True als de directory een geldige repository is met de juiste remote origin,
            anders wordt een exceptie opgegooid of False geretourneerd als de directory niet bestaat.

        Raises:
            RepositoryError: Als de directory geen geldige git repository is, de remote niet gevonden kan worden,
            of de remote origin niet overeenkomt.
        """
        if not self._path_local.is_dir():
            return False
        try:
            self._execute(["git", "-C", str(self._path_local), "rev-parse"])
        except subprocess.CalledProcessError as e:
            raise RepositoryError(
                message="De opgegeven directory is geen git repository",
                path_repo=self._path_local,
            ) from e
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
        except subprocess.CalledProcessError as e:
            raise RepositoryError(
                message="Kan de remote niet vinden van de git repository",
                path_repo=self._path_local,
            ) from e
        if self._url != origin:
            raise RepositoryError(
                message=f"De remote origin komt niet overeen, is '{origin}' in plaats van '{self._url}'",
                path_repo=self._path_local,
            )
        return True

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
