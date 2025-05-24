import json
import os
from enum import Enum
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template
from logtools import get_logger

logger = get_logger(__name__)

class TemplateType(Enum):
    POST_DEPLOY_CONFIG = "Create MDDE_PostDeployScript_Config.sql"
    POST_DEPLOY_CODELIST = "Create MDDE_PostDeployScript_CodeList.sql"

class PostDeployment:
    def __init__(self, dir_output: str, schema_post_deploy: str):
        self.schema = schema_post_deploy #"DA_MDDE"
        self.dir_output = dir_output

    def __template(self, type_template: TemplateType) -> Template:
        """
        Haal alle templates op uit de template folder. De locatie van deze folder is opgeslagen in de config.yml

        Return:
            dict_templates (dict): Bevat alle beschikbare templates en de locatie waar de templates te vinden zijn
        """
        # Loading templates
        environment = Environment(
            loader=FileSystemLoader(self.dir_templates),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return environment.get_template(type_template.value)

    def generate_ddl_Config(self, mapping_order: list):
        """
        Creëert het post deploy script voor alle mappings opgenomen in de modellen. Voor elke mapping wordt een insert statement aangemaakt
        waarmee een record aangemaakt wordt in de tabel [DA_MDDE].[Config].
        de basis hiervoor is de DAG functie mapping_order

        Args:
            mapping_order (list) bevat alle mappingen en de volgorde van laden.
        """
        dir_output = f"{self.dir_output}/CentralLayer/{self.schema}/PostDeployment/"
        file_output = "PostDeploy_MetaData_Config_MappingOrder.sql"
        file_output_master = "PostDeploy.sql"
        path_output_master = Path(
            f"{self.dir_output}/CentralLayer/PostDeployment/{file_output_master}"
        )

        # Add used folders to self.dict_created_ddls to be later used to add to the VS Project file
        self.__add_post_deploy_to_ddl(
            file_output=file_output, file_output_master=file_output_master
        )

        # Fill Path with the destination directory. Path is used for file system operations
        directory = Path(dir_output)
        # Make directory if not exist.
        directory.mkdir(parents=True, exist_ok=True)
        content = self.templates["PostDeploy_Config"].render(config=mapping_order)
        with open(f"{dir_output}{file_output}", mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(
            f"Written MDDE PostDeploy_Config file {Path(dir_output + file_output).resolve()}"
        )

        # Add file to master file.
        if not path_output_master.is_file():
            with open(path_output_master, "a") as f:
                f.write("/* Post deploy master file. */\n")
        else:
            # Opening a file located at the path specified by the variable
            # `path_output_master` in read mode. It then checks if a specific string `":r
            # ..\DA_MDDE\PostDeployment\{file_output}\n"` is present in the contents of the file.
            fr = open(path_output_master, "r")
            if f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n" not in fr.read():
                fr.close()
                with open(path_output_master, "a") as f:
                    f.write(
                        f"\nPRINT N'Running PostDeploy: ..\\DA_MDDE\\PostDeployment\\{file_output}'\n"
                    )
                    f.write(f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n")

    def generate_ddl_CodeTable(self):
        """
        Creëert het post deploy script voor de CodeTable. Voor elk record in de CodeList wordt een select
        statement gemaakt waarmee de data geladen kan worden in [DA_MDDE].[CodeList]

        Args:
            templates (dict): Bevat alle beschikbare templates en de locatie waar de templates te vinden zijn
        """
        # Opening JSON file
        file_codelist = Path(
            f"{self.params.dir_codelist}/{self.params.codelist_config.codeList_json}"
        )
        if not file_codelist.exists():
            logger.error(f"Kon codelist bestand niet vinden: '{file_codelist}'")
            return
        with open(file_codelist) as json_file:
            codeList = json.load(json_file)

        dir_output = f"{self.params.dir_repository}/CentralLayer/DA_MDDE"
        dir_output_type = f"{dir_output}/PostDeployment/"
        file_output = "PostDeploy_MetaData_Config_CodeList.sql"
        file_output_full = Path(os.path.join(dir_output_type, file_output))
        file_output_master = "PostDeploy.sql"
        file_output_master_full = Path(
            f"{self.params.dir_repository}/CentralLayer/PostDeployment/{file_output_master}"
        )

        self.__add_post_deploy_to_ddl(
            file_output=file_output, file_output_master=file_output_master
        )

        content = self.templates["PostDeploy_CodeList"].render(codeList=codeList)

        Path(dir_output_type).mkdir(parents=True, exist_ok=True)
        with open(file_output_full, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(
            f"Written CodeTable Post deploy script: {file_output_full.resolve()}"
        )

        # Add file to master file.
        if not file_output_master_full.is_file():
            with open(file_output_master_full, "a+") as f:
                f.write("/* Post deploy master file. */\n")
        if file_output_master_full.is_file():
            fr = open(file_output_master_full, "r")
            if f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n" not in fr.read():
                fr.close()
                with open(file_output_master_full, "a") as f:
                    f.write(
                        f"\nPRINT N'Running PostDeploy: ..\\DA_MDDE\\PostDeployment\\{file_output}\n"
                    )
                    f.write(f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n")
