import os
import subprocess
import time
from pathlib import Path
import webbrowser
from log_config import logging

from .publisher import DDLPublisher # FIXME: Should probably be part of this?

logger = logging.getLogger(__name__)


class DevOpsHandler:
    """Nog te doen"""

    def __init__(self, params: dict, dir_repository: str):
        logger.info("Initializing Class: 'DevOpsHandler'.")
        self.params = params
        self.dir_repository = Path(dir_repository).resolve

    def get_repo(self):
        """ """
        logger.info("Initializing Function: 'devopsgetrepo'.")
        dir_current = Path("./").resolve()
        self._remove_old_repo()  # deletes a directory and all its contents.
        time.sleep(5)
        for i in range(2):
            try:
                lst_command = [
                    "git",
                    "clone",
                    self.params.url,
                    "-b",
                    self.params.branch,
                    str(self.dir_repository),
                ]
                logger.info(" ".join(lst_command))
                subprocess.run(lst_command)
                logger.info(f"chdir to: {self.dir_repository}")
                os.chdir(self.dir_repository)
                lst_command = [
                    "git",
                    "branch",
                    self.params.featurebranch,
                    self.params.branch,
                ]
                logger.info(" ".join(lst_command))
                subprocess.run(lst_command)
                lst_command = ["git", "switch", self.params.featurebranch]
                logger.info(" ".join(lst_command))
                subprocess.run(lst_command)
                i += 99
            except:
                logger.warning(
                    "Er is wat mis gegaan. Waarschijnlijk moet je eerst inloggen op Devops. "
                )
                webbrowser.open(self.params.url_check, new=0, autoraise=True)
                logger.info("Wait timer for 15 seconds, to allow user to log in to DevOps")
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
        if not self.dir_repository.is_dir():
            return
        # change owner of file .idx, else we get an error
        for root, dirs, files in self.dir_repository.walk(top_down=False):
            for d in dirs:
                os.chmod((root / d), 0o777)
                (root / d).rmdir()
            for f in files:
                os.chmod((root / f), 0o777)
                (root / f).unlink()
        self.dir_repository.rmdir()
        logger.info(f"Delete existing folder: {self.dir_repository}")

    def publish_repo(self) -> None:
        """
        Voert een commit en push uit naar de DevOps repository en opent de branch in de browser.

        Deze functie voegt alle wijzigingen toe, maakt een commit met een werkitem-omschrijving,
        pusht naar de featurebranch en opent de branch-URL in de browser.

        Returns:
            None
        """
        os.chdir(self.dir_repository)
        lst_command = [
            "git",
            "add",
            "-A",
        ]
        logger.info(" ".join(lst_command))
        subprocess.run(lst_command)
        lst_command = [
            "git",
            "commit",
            "-m"
            f"Commit: {self.params.work_item_description.replace(' ', '_')} #{int(self.params.work_item)}",
        ]
        logger.info(" ".join(lst_command))
        subprocess.run(lst_command)
        lst_command = ["git", "push", "origin", self.params.featurebranch]
        logger.info(" ".join(lst_command))
        subprocess.run(lst_command)

        # Open browser to check Commit tot DevOps
        webbrowser.open(self.params.url_branch, new=0, autoraise=True)
