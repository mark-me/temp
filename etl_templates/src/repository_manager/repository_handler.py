import os
import shutil
import subprocess
import time
import webbrowser
from pathlib import Path

from logtools import get_logger

from .project_file import ProjectFile

logger = get_logger(__name__)

class RepositoryHandler:
    """Handelt repository acties af zoals clonen, feature branches aanmaken, comitten en pushen naar de remote"""

    def __init__(self, params: dict, dir_repository: str):
        """
        Initialiseert de RepositoryHandler met repository parameters en een doel-directory.

        Args:
            params (dict): Dictionary die de repository parameters bevat zoals URL, branch, etc.
            dir_repository (str): Pad naar de locale repository directory.
        """
        self.params = params
        self.dir_repository = Path(dir_repository).resolve

    def clone(self):
        """
        Clonet de repository, maakt een featurebranch aan en schakelt hiernaar over.

        Deze functie verwijdert eerst een bestaande repository, clonet vervolgens de opgegeven repository,
        maakt een nieuwe featurebranch aan en schakelt hiernaar over. Indien nodig wordt de gebruiker gevraagd
        om in te loggen op DevOps.

        Returns:
            None
        """
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

    def push(self) -> None:
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


    def add_directory(self, path_source: Path):
        lst_files_new = self._find_files_new()
        project_file = ProjectFile(dir, path_repository=self.path_repository, )

    def _find_files_new(self) -> list:
        # Find files which are not in the repository
        project_file = ProjectFile()


    def _copy_file(self, file_source: str, file_destination: str):
        dest_folder = Path(file_destination).parent
        dest_folder.mkdir(parents=True, exist_ok=True)
        shutil.copy2(
            Path(file_source),
            Path(file_destination),
        )

    def _copy_mdde_scripts(self):
        """
        Kopieer de MDDE scripts naar een Visual Studio Project repository folder
        """
        logger.info("Start copy of MDDE scripts to vs Project repo folder.")
        # dir_root = f"{self.params.dir_repository}\\"
        dir_output = "CentralLayer/DA_MDDE"
        dir_scripts_mdde = self.params.generator_config.dir_scripts_mdde
        for platform in [d for d in Path(dir_scripts_mdde).iterdir() if d.is_dir()]:
            logger.info(f"Found platform folder: {dir_scripts_mdde}/{platform.name}.")
            for schema in [d for d in platform.iterdir() if d.is_dir()]:
                logger.info(
                    f"Found schema folder: {dir_scripts_mdde}/{platform.name}/{schema.name}."
                )
                for object_type in [d for d in schema.iterdir() if d.is_dir()]:
                    logger.info(
                        f"Found object type folder: {dir_scripts_mdde}\\{platform.name}\\{schema.name}\\{object_type.name}."
                    )
                    for file in [f for f in object_type.iterdir() if f.is_file()]:
                        # Add used folders to dict_created_ddls to be later used to add to the VS Project file
                        self.__add_object_to_ddl(
                            code_model=schema.name,
                            type_objects=object_type,
                            file_output=file.name,
                        )
                        dir_output_type = f"{dir_output}/{object_type.name}/"
                        Path(os.path.join(self.dir_root, dir_output_type.name)).mkdir(
                            parents=True, exist_ok=True
                        )
                        dest = Path(
                            os.path.join(self.dir_root, dir_output_type, file.name)
                        )
                        logger.info(f"Copy {file} to: {dest.resolve()}")
                        dest.write_text(file.read_text())
                        # Create a copy of the new file to the intermediate folder
                        cp_folder = Path(
                            f"{self.params.dir_generate}/{dir_output_type}/"
                        )
                        cp_folder.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(
                            Path(dest),
                            Path(f"{cp_folder}/{file.name}"),
                        )