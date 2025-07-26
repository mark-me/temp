import os
from enum import Enum
from pathlib import Path
from shutil import copytree

from jinja2 import Environment, FileSystemLoader, Template
from logtools import get_logger

from .data_code_lists import CodeListReader

logger = get_logger(__name__)


class TemplateType(Enum):
    """
    Enum die de verschillende types post-deployment templates specificeert.
    Wordt gebruikt om het juiste templatebestand te selecteren voor het genereren van scripts.
    """

    CONFIG_MODEL_INFO = "ConfigModelInfo.sql"
    CONFIG_RUN_ORDER = "ConfigRunOrder.sql"
    CONFIG_MAPPING_CLUSTERS = "ConfigMappingClusters.sql"
    CONFIG_LOAD_DEPENDENCIES = "ConfigLoadDependencies.sql"
    CODELIST = "CodeList.sql"


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
        self._schema = schema
        self._path_output = path_output
        self._path_data = path_data
        self.post_deployment_scripts = []

    def process(
        self,
        info_models: list[dict],
        mapping_order: list[dict],
        mapping_dependencies: list[dict],
        datamart_clusters: list[dict],
    ) -> None:
        """
        Voert het volledige post-deployment proces uit voor MDDE.
        Genereert en schrijft alle benodigde scripts en kopieert database objecten naar de output directory.

        Args:
            info_models (list[dict]): Modelinformatie voor het genereren van het model info script.
            mapping_order (list[dict]): Mapping order configuratie voor het genereren van het mapping order script.
            mapping_dependencies (list[dict]): Afhankelijkheden voor het genereren van het dependencies script.
            datamart_clusters (list[dict]): Clusters voor het genereren van het mapping clusters script.
        """
        self._generate_load_model_info(info_models=info_models)
        self._generate_load_config(mapping_order=mapping_order)
        self._generate_load_dependencies(mapping_dependencies=mapping_dependencies)
        self._generate_load_config_mapping_clusters(mapping_clusters=datamart_clusters)
        self._generate_load_code_list()
        self._generate_load_dates()
        self._copy_db_objects()
        self._generate_post_deploy_master()

    def _copy_db_objects(self):
        """
        Kopieert de database objecten van de bronmap naar de outputmap.
        Zorgt ervoor dat alle benodigde database objecten beschikbaar zijn in de output directory.

        """
        path_source = Path(__file__).parent / "db_objects"
        copytree(path_source, self._path_output.parent, dirs_exist_ok=True)

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

    def _generate_load_model_info(self, info_models: list[dict]) -> None:
        """
        Genereert het post-deploy script voor de modelinformatie.
        Rendert het template met de modelinformatie en schrijft het resultaat naar het juiste outputbestand.

        Args:
            info_models (list[dict]): De modelinformatie die in het script verwerkt moet worden.
        """
        template = self._get_template(TemplateType.CONFIG_MODEL_INFO)
        content = template.render(info_models=info_models)
        file_output = TemplateType.CONFIG_MODEL_INFO.value
        self._write_generated_code(content, file_output)

    def _write_generated_code(self, content, file_output):
        """
        Schrijft de gegenereerde code naar een bestand in de PostDeployment directory.
        Maakt de directory aan indien nodig en voegt het script toe aan de lijst van post-deployment scripts.

        Args:
            content (str): De inhoud van het te schrijven scriptbestand.
            file_output (str): De naam van het outputbestand.
        """
        path_output = self._path_output / "PostDeployment"
        path_output.mkdir(parents=True, exist_ok=True)
        path_file_output = path_output / file_output
        with open(str(path_file_output), mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(
            f"Created MDDE post deployment script '{path_file_output.resolve()}'"
        )
        self.post_deployment_scripts.append(path_file_output)

    def _generate_load_config(self, mapping_order: list[dict]) -> None:
        """
        Genereert het post-deploy script voor de mapping order configuratie.
        Rendert het template met de mapping order en schrijft het resultaat naar het juiste outputbestand.

        Args:
            mapping_order (list): De mapping order configuratie die in het script verwerkt moet worden.
        """
        template = self._get_template(TemplateType.CONFIG_RUN_ORDER)
        content = template.render(config=mapping_order)
        file_output = TemplateType.CONFIG_RUN_ORDER.value
        self._write_generated_code(content, file_output)

    def _generate_load_dependencies(self, mapping_dependencies: list[dict]) -> None:
        """
        Genereert het post-deploy script voor de mapping dependencies voor conditioneel laden van entiteiten.
        Rendert het template met de mapping dependencies en schrijft het resultaat naar het juiste outputbestand.

        Args:
            mapping_dependencies (list): De afhankelijkheden tussen de mappings die in het script verwerkt moet worden.
        """
        template = self._get_template(TemplateType.CONFIG_LOAD_DEPENDENCIES)
        content = template.render(mapping_dependencies=mapping_dependencies)
        file_output = TemplateType.CONFIG_LOAD_DEPENDENCIES.value
        self._write_generated_code(content, file_output)

    def _generate_load_config_mapping_clusters(
        self, mapping_clusters: list[dict]
    ) -> None:
        """
        Genereert het post-deploy script voor de mapping clusters voor het uitwisselen van feiten in dimensies bij het laden van datamarts.
        Rendert het template met de mapping clusters en schrijft het resultaat naar het juiste outputbestand.

        Args:
            mapping_clusters (list[dict]): _description_
        """
        template = self._get_template(TemplateType.CONFIG_MAPPING_CLUSTERS)
        content = template.render(mapping_clusters=mapping_clusters)
        file_output = TemplateType.CONFIG_MAPPING_CLUSTERS.value
        self._write_generated_code(content, file_output)

    def _generate_load_code_list(self) -> None:
        """
        Genereert het post-deploy script voor alle codelijsten in de data directory.
        Leest de codelijsten, rendert het template en schrijft het resultaat naar het juiste outputbestand.

        """
        code_list_reader = CodeListReader(dir_input=self._path_data)
        code_list = code_list_reader.read()
        template = self._get_template(TemplateType.CODELIST)
        content = template.render(codeList=code_list)
        file_output = TemplateType.CODELIST.value
        self._write_generated_code(content, file_output)

    def _generate_load_dates(self) -> None:
        """
        Genereert het post-deploy script voor het laden van datums in de database.
        Schrijft een SQL-opdracht naar een bestand om de stored procedure voor het laden van datums uit te voeren.

        """
        content = "EXEC [DA_MDDE].[sp_LoadDates]"

        path_file_output = self._path_output / "PostDeployment" / "Dates.sql"
        path_file_output.parent.mkdir(parents=True, exist_ok=True)
        with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
        logger.info(
            f"Created Dates post deployment script '{path_file_output.resolve()}'"
        )
        self.post_deployment_scripts.append(path_file_output)

    def _generate_post_deploy_master(self) -> None:
        """
        Voegt een post-deploy scriptbestand toe aan het masterbestand voor post-deployment scripts.
        Controleert of het bestand al is opgenomen en voegt het toe indien nodig.

        Args:
            path_file_output (Path): Het pad naar het toe te voegen post-deploy scriptbestand.
        """
        path_output_master = (
            self._path_output.parent / "PostDeployment" / "PostDeploy.sql"
        )
        path_output_master.parent.mkdir(parents=True, exist_ok=True)
        with open(path_output_master, "w", encoding="utf-8") as file:
            for script in self.post_deployment_scripts:
                script_path = self._get_relative_path(
                    path_base=path_output_master, path_relative=script
                )
                script_path = str(script_path).replace("/", "\\")
                script_entries = [
                    f"PRINT N'Running PostDeploy: {script_path}'",
                    f":r {script_path}",
                ]
                file.writelines(line + "\n" for line in script_entries)

    def _get_relative_path(self, path_base: Path, path_relative: Path) -> str:
        """
        Bepaalt het relatieve pad van een scriptbestand ten opzichte van het masterbestand.
        Vergelijkt de padonderdelen en construeert een relatief pad voor gebruik in het master post-deploy script.

        Args:
            path_base (Path): Het basispad (meestal het masterbestand).
            path_relative (Path): Het pad van het scriptbestand waarvoor het relatieve pad bepaald moet worden.

        Returns:
            str: Het relatieve pad van het scriptbestand ten opzichte van het masterbestand.
        """
        parts_compare = list(path_base.parts)
        parts_output = list(path_relative.parts)

        is_part_in_common = True
        i = 0
        while is_part_in_common and i < len(parts_output) and i < len(parts_compare):
            if parts_compare[i] != parts_output[i]:
                is_part_in_common = False
            i += 1

        parts_new = parts_output[i - 1 :]
        dir_output = os.path.join(*parts_new)
        dir_output = "..\\" + dir_output
        return dir_output
