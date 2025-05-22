import json
import os
from enum import Enum
from pathlib import Path

import sqlparse
from jinja2 import Environment, FileSystemLoader, Template
from log_config import logging

from .entities import DDLEntities
from .views import DDLSourceViews

logger = logging.getLogger(__name__)


class TemplateType(Enum):
    """Enumerates the types of vertices in the graph.

    Provides distinct identifiers for each type of node in the graph, including entities, mappings, and files.
    """

    SCHEMA = "create_schema.sql"
    TABLE = "create_table.sql"
    ENTITY = "create_entity.sql"
    VIEW = "create_view.sql"
    PROCEDURE = "create_procedure.sql"
    SOURCE_VIEW = "create_source_view.sql"
    SOURCE_VIEW_AGGR = "create_source_view_agg.sql"
    POST_DEPLOY_CONFIG = "Create MDDE_PostDeployScript_Config.sql"
    POST_DEPLOY_CODELIST = "Create MDDE_PostDeployScript_CodeList.sql"


class DDLGenerator:
    """Class DDLGenerator genereert DDL en ETL vanuit de door RETW gemaakte Json."""

    def __init__(self, params: dict):
        """Initialiseren van de Class DDLGenerator. Hiervoor wordt de config.yml uitgelezen om parameters
        mee te kunnen geven. Ook wordt de flow georkestreerd waarmee het Json-bestand uitgelezen wordt
        en omgezet kan worden naar DDL en ETL bestanden

        Args:
            params (dict): Bevat alle parameters vanuit config.yml
        """
        logger.info("Initializing Class: 'DDLGenerator'.")
        self.params = params
        self.dir_generator = self.params.dir_generate
        self.dir_templates = params.dir_templates
        self.source_layer_prefix = "SL_"
        self.schema_post_deploy = "DA_MDDE"
        self.templates = self.__template(dir_templates=params.dir_templates)
        self.generator_entities = DDLEntities(
            dir_output=self.dir_generate,
            ddl_template=self.__template(TemplateType.ENTITY),
        )
        self.generator_views = DDLSourceViews(
            dir_output=self.dir_generate,
            ddl_template=self.__template(TemplateType.SOURCE_VIEW),
        )

    def __read_model_file(self, file_RETW: str) -> dict:
        """Leest het in  de config opgegeven Json-bestand in en slaat de informatie op in een dictionary

        Returns:
            dict_models (dict): De JSON (RETW Output) geconverteerd naar een dictionary
        """
        p = Path(file_RETW).resolve()
        logger.info(f"Filepath MDDE Json file: {p}")
        # Function not yet used, but candidate for reading XML file
        with open(file_RETW) as json_file:
            dict_model = json.load(json_file)
        return dict_model

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

    def generate_ddls(self, file_RETW: dict, mapping_order: list):
        """
        Overkoepelende functie waarin alle functions worden gestart om de diverse objecttypes op te bouwen

        Args:
            model (dict): De JSON (RETW Output) geconverteerd naar een dictionary
            templates (dict): Bevat alle beschikbare templates en de locatie waar de templates te vinden zijn
        """
        # self.__copy_mdde_scripts()\
        dict_RETW = self.__read_model_file(file_RETW=file_RETW)
        identifiers = {}
        if "Mappings" in dict_RETW:
            mappings = dict_RETW["Mappings"]
            identifiers = self.__select_identifiers(mappings=mappings)
        self.generator_entities.generate_ddl_entities(
            models=dict_RETW["Models"], identifiers=identifiers
        )
        if "Mappings" in dict_RETW:
            self.__write_ddl_source_view_aggr(mappings=mappings)
            self.__write_ddl_source_view(mappings=mappings, identifiers=identifiers)
        self.__write_ddl_MDDE_PostDeploy_Config(mapping_order=mapping_order)
        self.__write_ddl_MDDE_PostDeploy_CodeTable()

    def __select_identifiers(self, mappings: dict) -> dict:
        """
        Haalt alle identifiers op uit het model ten behoeve van de aanmaken van BKeys in de entiteiten en DDL's

        Args:
            models (dict): de JSON (RETW Output) geconverteerd naar een dictionary
        Returns:
            identifiers (dict): een dictionary met daarin alle informatie van de identifier benodigd voor het aanmaken van BKeys
        """
        # TODO: in __select_identifiers zit nu opbouw van strings die platform specifiek zijn (SSMS). Om de generator ook platform onafhankelijk te maken kijken of we dit wellicht in een template kunnen gieten.
        identifiers = {}

        def get_name_business_key(identifier):
            return (
                identifier["EntityCode"]
                if identifier["IsPrimary"]
                else identifier["Code"]
            )

        def get_identifier_def_primary(name_business_key):
            return f"[{name_business_key}BKey] nvarchar(200) NOT NULL"

        def get_identifier_def(name_business_key, mapping, attr_map):
            if "AttributesSource" in attr_map:
                id_entity = attr_map["AttributesSource"]["IdEntity"]
                attribute_source = attr_map["AttributesSource"]["Code"]
                return f"[{name_business_key}BKey] = '{mapping['DataSource']}'+ '-' + CAST({id_entity}.[{attribute_source}] AS NVARCHAR(50))"
            else:
                return f"[{name_business_key}BKey] = '{mapping['DataSource']}'+  '-' + {attr_map['Expression']}"

        for mapping in mappings:
            if "Identifiers" not in mapping["EntityTarget"]:
                logger.error(
                    f"Geen identifiers aanwezig voor entitytarget {mapping['EntityTarget']['Name']}"
                )
                continue
            if "AttributeMapping" not in mapping:
                logger.error(
                    f"Geen attribute mapping aanwezig voor entity {mapping['EntityTarget']['Name']}"
                )
                continue
            for identifier in mapping["EntityTarget"]["Identifiers"]:
                for attr_map in mapping["AttributeMapping"]:
                    if (
                        attr_map["AttributeTarget"]["IdEntity"]
                        == identifier["EntityID"]
                        and attr_map["AttributeTarget"]["Code"] == identifier["Name"]
                    ):
                        name_business_key = get_name_business_key(identifier)
                        identifier_def_primary = get_identifier_def_primary(
                            name_business_key
                        )
                        identifier_def = get_identifier_def(
                            name_business_key, mapping, attr_map
                        )

                        identifiers[identifier["Id"]] = {
                            "IdentifierID": identifier["Id"],
                            "IdentifierName": identifier["Name"],
                            "IdentifierCode": identifier["Code"],
                            "EntityId": identifier["EntityID"],
                            "EntityCode": identifier["EntityCode"],
                            "IsPrimary": identifier["IsPrimary"],
                            "IdentifierStringEntity": identifier_def_primary,
                            "IdentifierStringSourceView": identifier_def,
                        }
        return identifiers

    def __write_ddl_MDDE_PostDeploy_Config(self, mapping_order: list):
        """
        Creëert het post deploy script voor alle mappings opgenomen in de modellen. Voor elke mapping wordt een insert statement aangemaakt
        waarmee een record aangemaakt wordt in de tabel [DA_MDDE].[Config].
        de basis hiervoor is de DAG functie mapping_order

        Args:
            mapping_order (list) bevat alle mappingen en de volgorde van laden.
        """
        dir_output = f"{self.dir_generator}/CentralLayer/{self.schema_post_deploy}/PostDeployment/"
        file_output = "PostDeploy_MetaData_Config_MappingOrder.sql"
        file_output_master = "PostDeploy.sql"
        path_output_master = Path(
            f"{self.dir_generator}/CentralLayer/PostDeployment/{file_output_master}"
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

    def __write_ddl_MDDE_PostDeploy_CodeTable(self):
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
