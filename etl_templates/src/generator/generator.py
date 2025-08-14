from integrator import DagImplementation

from logtools import get_logger

from .ddl_entities import DDLEntities
from .ddl_views_source import DDLSourceViews
from .ddl_views_source_aggr import DDLSourceViewsAggr

logger = get_logger(__name__)


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
        self.platform = params.template_platform
        self.path_output = params.path_output

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

        generator_source_views = DDLSourceViews(
            path_output=self.path_output, platform=self.platform
        )
        generator_source_views.generate_ddls(mappings=mappings)

        generator_source_views_aggr = DDLSourceViewsAggr(
            path_output=self.path_output, platform=self.platform
        )
        generator_source_views_aggr.generate_ddls(mappings=mappings)

        entities = dag_etl.get_entities()
        generator_entities = DDLEntities(
            path_output=self.path_output, platform=self.platform
        )
        generator_entities.generate_ddls(entities=entities)
