import json
from enum import Enum
from pathlib import Path

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

        self.generator_entities = DDLEntities(
            dir_output=self.dir_generator,
            ddl_template=self.__template(TemplateType.ENTITY),
        )
        self.generator_views = DDLSourceViews(
            dir_output=self.dir_generator,
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
            self.generator_views.generate_ddl_source_view(mappings=mappings, identifiers=identifiers)
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
