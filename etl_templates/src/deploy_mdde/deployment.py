from shutil import copytree
from enum import Enum
from pathlib import Path, WindowsPath
import os

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
        self.post_deployment_scripts = []

    def process(self, mapping_order: list):
        """
        Voert het genereren van alle post-deployment scripts uit voor het opgegeven mapping order.
        Roept interne methoden aan om codelijst- en configuratiescripts te genereren, database objecten te kopiëren
        en het masterbestand bij te werken.

        Args:
            mapping_order (list): De mapping order configuratie die in het script verwerkt moet worden.
        """
        self._generate_load_code_list()
        self._generate_load_config(mapping_order=mapping_order)
        self._copy_db_objects()
        self._generate_post_deploy_master()

    def _copy_db_objects(self):
        """
        Kopieert de database objecten van de bronmap naar de outputmap.
        Zorgt ervoor dat alle benodigde database objecten beschikbaar zijn in de output directory.

        """
        path_source = Path(__file__).parent / "db_objects"
        copytree(path_source, self.path_output, dirs_exist_ok=True)

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
        path_templates = Path(__file__).parent / "templates"
        if not path_templates.is_dir():
            logger.error(
                f"Directory for post deployment templates not found '{path_templates}'"
            )
            raise FileNotFoundError

        # Loading templates
        environment = Environment(
            loader=FileSystemLoader(path_templates),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return environment.get_template(type_template.value)

    def _generate_load_config(self, mapping_order: list):
        """
        Genereert het post-deploy script voor de mapping order configuratie.
        Rendert het template met de mapping order en schrijft het resultaat naar het juiste outputbestand.

        Args:
            mapping_order (list): De mapping order configuratie die in het script verwerkt moet worden.
        """
        template = self._get_template(TemplateType.POST_DEPLOY_CONFIG)
        content = template.render(config=mapping_order)

        path_output = self.path_output / "PostDeployment"
        path_output.mkdir(parents=True, exist_ok=True)
        path_file_output = path_output / "PostDeploy_MetaData_Config_MappingOrder.sql"
        with open(str(path_file_output), mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(
            f"Created MDDE Config post deployment script '{path_file_output.resolve()}'"
        )

        self.post_deployment_scripts.append(path_file_output)

    def _generate_load_code_list(self):
        """
        Genereert het post-deploy script voor alle codelijsten in de data directory.
        Leest de codelijsten, rendert het template en schrijft het resultaat naar het juiste outputbestand.

        """
        code_list_reader = CodeListReader(dir_input=self.path_data)
        code_list = code_list_reader.read()
        template = self._get_template(TemplateType.POST_DEPLOY_CODELIST)
        content = template.render(codeList=code_list)

        path_file_output = (
            self.path_output
            / "PostDeployment"
            / "PostDeploy_MetaData_Config_CodeList.sql"
        )
        path_file_output.parent.mkdir(parents=True, exist_ok=True)
        with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(
            f"Created CodeTable post deployment script '{path_file_output.resolve()}'"
        )
        self.post_deployment_scripts.append(path_file_output)

    def _generate_post_deploy_master(self):
        """
        Voegt een post-deploy scriptbestand toe aan het masterbestand voor post-deployment scripts.
        Controleert of het bestand al is opgenomen en voegt het toe indien nodig.

        Args:
            path_file_output (Path): Het pad naar het toe te voegen post-deploy scriptbestand.
        """
        path_output_master = (
            self.path_output.parent / "PostDeployment" / "PostDeploy.sql"
        )
        path_output_master.parent.mkdir(parents=True, exist_ok=True)
        with open(path_output_master, "w") as file:
            for script in self.post_deployment_scripts:
                script_path = self._get_relative_path(
                    path_base=path_output_master, path_relative=Path(script)
                )
                script_entries = [
                    f"PRINT N'Running PostDeploy: {script_path}'",
                    f":r {script_path}",
                ]
                file.writelines(line + "\n" for line in script_entries)

    def _get_relative_path(self, path_base: Path, path_relative: Path) -> str:
        parts_compare = list(path_base.parts)
        parts_output = list(path_relative.parts)

        is_part_in_common = True
        i = 0
        while is_part_in_common and i < len(parts_output) and len(parts_compare):
            if parts_compare[i] != parts_output[i]:
                is_part_in_common = False
            i += 1

        parts_new = parts_output[i - 1 :]
        dir_output = os.path.join(*parts_new)
        dir_output = "..\\" + dir_output
        return dir_output
