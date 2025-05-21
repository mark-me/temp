import os
import time
from pathlib import Path
import shutil
import webbrowser
from log_config import logging

from generator import DDLGenerator
from generator import DDLPublisher

logger = logging.getLogger(__name__)


class DevOpsHandler:
    """Nog te doen
    """
    def __init__(self, params: dict, dir_repository: str):
        logger.info("Initializing Class: 'DevOpsHandler'.")
        self.params = params
        self.dir_repository = dir_repository

    def get_repo(self):
        """

        """
        logger.info("Initializing Function: 'devopsgetrepo'.")
        currentFolder = Path("./").resolve()
        if os.path.isdir(self.dir_repository.resolve()):
            # change owner of file .idx, else we get an error
            for root, dirs, files in os.walk(self.dir_repository.resolve()):
                for d in dirs:
                    os.chmod(os.path.join(root, d), 0o777)
                for f in files:
                    os.chmod(os.path.join(root, f), 0o777)
            logger.info(
                f"Delete existing folder: {self.dir_repository.resolve()}"
            )
            shutil.rmtree(
                self.dir_repository.resolve()
            )  # deletes a directory and all its contents.
        time.sleep(5)
        for i in range(0, 2):
            try:
                logger.info(
                    f"git clone {self.params.url} -b {self.params.branch} {str(self.dir_repository)}"
                )
                os.system(
                    f"git clone {self.params.url} -b {self.params.branch} {str(self.dir_repository)}"
                )
                logger.info(f"chdir to: {self.dir_repository.resolve()}")
                os.chdir(self.dir_repository.resolve())
                logger.info(f"git branch {self.params.featurebranch} {self.params.branch}")
                os.system(f"git branch {self.params.featurebranch} {self.params.branch}")
                logger.info(f"git switch {self.params.featurebranch}")
                os.system(f"git switch {self.params.featurebranch}")
                # Create all DDL and ETL Files and store them in the new repo folder
                i += 99
            except:
                print(
                    "Er is wat mis gegaan. Waarschijnlijk moet je eerst inloggen op Devops. "
                )
                webbrowser.open(self.params.url_check, new=0, autoraise=True)
                print("Wait timer for 15 seconds, to allow user to log in to DevOps")
                time.sleep(15)
                continue
            else:
                break
        # Relocate to org root folder
        os.chdir(currentFolder.resolve())

    def publish_repo(self):
        """

        """
        logger.info(
            f"""git add -A && git commit -m "Commit: {self.params.work_item_description.replace(' ', '_')} #{int(self.params.work_item)}" """
        )
        os.chdir(self.dir_repository.resolve())
        os.system(
            f"""git add -A && git commit -m "Commit: {self.params.work_item_description.replace(' ', '_')} #{int(self.params.work_item)}" """
        )
        logger.info(f"git push origin {self.params.featurebranch}")
        os.system(f"git push origin {self.params.featurebranch}")

        # Open browser to check Commit tot DevOps
        webbrowser.open(self.params.url_branch, new=0, autoraise=True)


# Run Current Class
if __name__ == "__main__":
    print("Done")
