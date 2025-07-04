from enum import Enum
from pathlib import Path

from integrator import DagImplementation
from jinja2 import Environment, FileSystemLoader, Template
from logtools import get_logger

from .ddl_entities import DDLEntities
from .ddl_views_source import DDLSourceViews
from .ddl_views_source_aggr import DDLSourceViewsAggr

logger = get_logger(__name__)


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
    """
    Class DDLGenerator genereert DDL en ETL vanuit de door RETW gemaakte Json.

    Deze klasse leest parameters uit een configuratiebestand, orkestreert het inlezen van een JSON-modelbestand,
    en genereert DDL- en ETL-bestanden op basis van de ingelezen data en templates.
    """

    def __init__(self, params: dict):
        """Initialiseren van de Class DDLGenerator. Hiervoor wordt de config.yml uitgelezen om parameters
        mee te kunnen geven. Ook wordt de flow georkestreerd waarmee het Json-bestand uitgelezen wordt
        en omgezet kan worden naar DDL en ETL bestanden

        Args:
            params (dict): Bevat alle parameters vanuit config.yml
        """
        logger.info("Initializing Class: 'DDLGenerator'.")
        self.dir_generator = params.path_output
        self.dir_templates = (
            Path(__file__).parent / "templates" / params.template_platform
        )
        self.source_layer_prefix = "SL_"

        self.entities = DDLEntities(
            dir_output=self.dir_generator,
            ddl_template=self._get_template(TemplateType.ENTITY),
        )
        self.source_views = DDLSourceViews(
            dir_output=self.dir_generator,
            ddl_template=self._get_template(TemplateType.SOURCE_VIEW),
        )
        self.source_views_aggr = DDLSourceViewsAggr(
            dir_output=self.dir_generator,
            ddl_template=self._get_template(TemplateType.SOURCE_VIEW_AGGR),
        )

    def _get_template(self, type_template: TemplateType) -> Template:
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

    def generate_ddls(self, dag_etl: DagImplementation):
        """
        Genereert DDL- en ETL-bestanden op basis van een RETW JSON-modelbestand en een opgegeven mappingvolgorde.

        Deze methode leest het opgegeven JSON-modelbestand in, selecteert identifiers, en genereert DDL- en ETL-bestanden
        voor entiteiten en views. De gegenereerde bestanden worden aangemaakt op basis van de mappingvolgorde en de
        beschikbare templates.

        Args:
            dag_etl (DagImplementation): Een DAG waar implementatie details aan zijn toegevoegd
        """
        # self.__copy_mdde_scripts()\
        mappings = dag_etl.get_mappings()
        self.source_views.generate_ddls(mappings=mappings)
        self.source_views_aggr.generate_ddls(mappings=mappings)
        entities = dag_etl.get_entities()
        self.entities.generate_ddls(entities=entities)

