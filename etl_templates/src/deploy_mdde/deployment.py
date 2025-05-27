from enum import Enum
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template
from logtools import get_logger

from .data_code_lists import CodeListReader

logger = get_logger(__name__)


class TemplateType(Enum):
    """
    Enum die de verschillende types post-deployment templates specificeert.
    Wordt gebruikt om het juiste templatebestand te selecteren voor het genereren van scripts.
    """
    POST_DEPLOY_CONFIG = "PostDeployScript_Config.sql"
    POST_DEPLOY_CODELIST = "PostDeployScript_CodeList.sql"


class DeploymentMDDE:
    def __init__(self, path_data: Path, schema: str, path_output: Path):
        """
        Initialiseert het DeploymentMDDE object met de opgegeven data directory, schema en outputpad.
        Slaat de paden en het schema op voor gebruik bij het genereren van post-deployment scripts.

        Args:
            path_data (Path): Het pad naar de directory met data voor codelijsten.
            schema (str): De naam van het database schema.
            path_output (Path): Het pad naar de output directory voor de scripts.
        """
        self.schema = schema
        self.path_output = path_output
        self.path_data = path_data

    def _get_template(self, type_template: TemplateType) -> Template:
        """
        Haalt het Jinja2-template op voor het opgegeven type post-deployment script.
        Controleert of de template directory bestaat en laadt het juiste templatebestand.

        Args:
            type_template (TemplateType): Het type template dat opgehaald moet worden.

        Returns:
            Template: Het geladen Jinja2-template.

        Raises:
            FileNotFoundError: Als de template directory niet gevonden kan worden.
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
        Genereert het post-deploy script voor de mapping order configuratie.
        Rendert het template met de mapping order en schrijft het resultaat naar het juiste outputbestand.

        Args:
            mapping_order (list): De mapping order configuratie die in het script verwerkt moet worden.
        """
        template = self._get_template(TemplateType.POST_DEPLOY_CONFIG)
        content = template.render(config=mapping_order)

        path_output = self.dir_output / "CentralLayer" / self.schema / "PostDeployment"
        path_output.mkdir(parents=True, exist_ok=True)
        path_file_output = path_output / "PostDeploy_MetaData_Config_MappingOrder.sql"
        with open(str(path_file_output), mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(f"Written MDDE PostDeploy_Config file {path_file_output.resolve()}")

        self._add_to_post_deploy_master(path_file_output)

    def generate_load_code_list(self):
        """
        Genereert het post-deploy script voor alle codelijsten in de data directory.
        Leest de codelijsten, rendert het template en schrijft het resultaat naar het juiste outputbestand.

        """
        code_list_reader = CodeListReader(dir_input=self.path_data)
        code_list = code_list_reader.read()
        template = self._get_template(TemplateType.POST_DEPLOY_CODELIST)
        content = template.render(codeList=code_list)

        path_output = self.path_output / "CentralLayer" / self.schema / "PostDeployment"
        path_file_output = path_output / "PostDeploy_MetaData_Config_CodeList.sql"
        path_output.mkdir(parents=True, exist_ok=True)
        with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(
            f"Written CodeTable Post deploy script: {path_file_output.resolve()}"
        )
        self._add_to_post_deploy_master(path_file_output)

    def _add_to_post_deploy_master(self, path_file_output: Path):
        """
        Voegt een post-deploy scriptbestand toe aan het masterbestand voor post-deployment scripts.
        Controleert of het bestand al is opgenomen en voegt het toe indien nodig.

        Args:
            path_file_output (Path): Het pad naar het toe te voegen post-deploy scriptbestand.
        """
        path_output_master = (
            self.path_output
            / "CentralLayer/PostDeployment"
            / "PostDeploy.sql"
        )
        # Add file to master file.
        if not path_output_master.is_file():
            with open(path_output_master, "a+") as f:
                f.write("/* Post deploy master file. */\n")
        else:
            with open(path_output_master, "r") as fr:
                if f":r ..\\DA_MDDE\\PostDeployment\\{path_file_output}\n" not in fr.read():
                    fr.close()
                    with open(path_output_master, "a") as f:
                        f.write(
                            f"\nPRINT N'Running PostDeploy: ..\\DA_MDDE\\PostDeployment\\{path_file_output}\n"
                        )
                        f.write(f":r ..\\DA_MDDE\\PostDeployment\\{path_file_output}\n")
