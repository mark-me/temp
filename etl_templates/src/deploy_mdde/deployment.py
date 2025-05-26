import json
from enum import Enum
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template
from logtools import get_logger

from .data_code_lists import CodeList

logger = get_logger(__name__)


class TemplateType(Enum):
    POST_DEPLOY_CONFIG = "PostDeployScript_Config.sql"
    POST_DEPLOY_CODELIST = "PostDeployScript_CodeList.sql"


class Deployment:
    def __init__(self, path_output: Path, schema_post_deploy: str, path_data: Path):
        self.schema = schema_post_deploy  # "DA_MDDE"
        self.path_output = path_output
        self.path_data = path_data

    def _get_template(self, type_template: TemplateType) -> Template:
        """
        Haal alle templates op uit de template folder. De locatie van deze folder is opgeslagen in de config.yml

        Return:
            dict_templates (dict): Bevat alle beschikbare templates en de locatie waar de templates te vinden zijn
        """
        dir_templates = Path(__file__).parent / "templates"
        if not dir_templates.is_dir():
            logger.error(
                f"Directory for post deployment templates not found '{dir_templates}'"
            )
            raise FileNotFoundError

        # Loading templates
        environment = Environment(
            loader=FileSystemLoader(dir_templates),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return environment.get_template(type_template.value)

    def generate_load_config(self, mapping_order: list):
        """
        Creëert het post deploy script voor alle mappings opgenomen in de modellen. Voor elke mapping wordt een insert statement aangemaakt
        waarmee een record aangemaakt wordt in de tabel [DA_MDDE].[Config].
        de basis hiervoor is de DAG functie mapping_order

        Args:
            mapping_order (list) bevat alle mappingen en de volgorde van laden.
        """
        template = self._get_template(TemplateType.POST_DEPLOY_CONFIG)
        content = template.render(config=mapping_order)

        dir_output = self.dir_output / "CentralLayer" / self.schema / "PostDeployment"

        dir_output.mkdir(parents=True, exist_ok=True)
        file_output = dir_output / "PostDeploy_MetaData_Config_MappingOrder.sql"
        with open(str(file_output), mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(f"Written MDDE PostDeploy_Config file {file_output.resolve()}")

        file_output_master = "PostDeploy.sql"
        path_output_master = (
            self.dir_output / "CentralLayer/PostDeployment" / file_output_master
        )

        # Add file to master file.
        if not path_output_master.is_file():
            with open(path_output_master, "a") as f:
                f.write("/* Post deploy master file. */\n")
        else:
            # Opening a file located at the path specified by the variable
            # `path_output_master` in read mode. It then checks if a specific string
            # `":r..\DA_MDDE\PostDeployment\{file_output}\n"` is present in the contents of the file.
            fr = open(path_output_master, "r")
            if f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n" not in fr.read():
                fr.close()
                with open(path_output_master, "a") as f:
                    f.write(
                        f"\nPRINT N'Running PostDeploy: ..\\DA_MDDE\\PostDeployment\\{file_output}'\n"
                    )
                    f.write(f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n")

    def generate_load_CodeList(self):
        """
        Creëert het post deploy script voor de CodeTable. Voor elk record in de CodeList wordt een select
        statement gemaakt waarmee de data geladen kan worden in [DA_MDDE].[CodeList]

        Args:
            templates (dict): Bevat alle beschikbare templates en de locatie waar de templates te vinden zijn
        """

        read_code_list = CodeList(dir_input=)
        data_codeList = CodeList

        content = self.templates["PostDeploy_CodeList"].render(codeList=codeList)


        dir_output = self.params.dir_repository / "CentralLayer" / self.schema
        dir_output_type = dir_output / "PostDeployment"
        file_output = dir_output_type / "PostDeploy_MetaData_Config_CodeList.sql"
        file_output_master = (
            self.params.dir_repository
            / "CentralLayer/PostDeployment"
            / "PostDeploy.sql"
        )
        dir_output_type.mkdir(parents=True, exist_ok=True)
        with open(file_output, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(f"Written CodeTable Post deploy script: {file_output.resolve()}")

        # Add file to master file.
        if not file_output_master.is_file():
            with open(file_output_master, "a+") as f:
                f.write("/* Post deploy master file. */\n")
        if file_output_master.is_file():
            fr = open(file_output_master, "r")
            if f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n" not in fr.read():
                fr.close()
                with open(file_output_master, "a") as f:
                    f.write(
                        f"\nPRINT N'Running PostDeploy: ..\\DA_MDDE\\PostDeployment\\{file_output}\n"
                    )
                    f.write(f":r ..\\DA_MDDE\\PostDeployment\\{file_output}\n")
