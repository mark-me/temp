import os
import subprocess
import time
from pathlib import Path
import shutil
import webbrowser
from log_config import logging

from generator import DDLGenerator
from generator import DDLPublisher

logger = logging.getLogger(__name__)


class DevOpsHandler:
    """Nog te doen"""

    def __init__(self, params: dict, dir_repository: str):
        logger.info("Initializing Class: 'DevOpsHandler'.")
        self.params = params
        self.dir_repository = Path(dir_repository)

    def get_repo(self):
        """ """
        logger.info("Initializing Function: 'devopsgetrepo'.")
        currentFolder = Path("./").resolve()
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
                logger.info(f"chdir to: {self.dir_repository.resolve()}")
                os.chdir(self.dir_repository.resolve())
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
        os.chdir(currentFolder.resolve())

    def _remove_old_repo(self) -> None:
        """
        Verwijdert de opgegeven repositorymap en al zijn inhoud als deze bestaat voor een verse start.

        Deze functie zorgt ervoor dat alle bestanden en submappen in de repositorymap verwijderbaar zijn
        door de rechten aan te passen, en verwijdert vervolgens de volledige map.

        Returns:
            None
        """
        if os.path.isdir(self.dir_repository.resolve()):
            # change owner of file .idx, else we get an error
            for root, dirs, files in os.walk(self.dir_repository.resolve()):
                for d in dirs:
                    os.chmod(os.path.join(root, d), 0o777)
                for f in files:
                    os.chmod(os.path.join(root, f), 0o777)
            logger.info(f"Delete existing folder: {self.dir_repository.resolve()}")
            shutil.rmtree(self.dir_repository.resolve())

    def publish_repo(self):
        os.chdir(self.dir_repository.resolve())
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
