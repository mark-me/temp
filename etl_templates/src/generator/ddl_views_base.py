from jinja2 import Template

from log_config import logging
from .ddl_base import DDLGeneratorBase

logger = logging.getLogger(__name__)


class DDLViewBase(DDLGeneratorBase):
    def __init__(self, dir_output: str, ddl_template: Template):
        super().__init__(dir_output=dir_output, ddl_template=ddl_template)

    def __set_datasource_code(self, mapping: dict) -> dict:
        """
        Bepaalt en stelt de DataSourceCode in voor een mapping op basis van de DataSource.

        Deze methode controleert of een DataSource aanwezig is en stelt de juiste code in, of logt een waarschuwing als deze ontbreekt.

        Args:
            mapping (dict): De mapping waarvoor de DataSourceCode wordt bepaald.

        Returns:
            dict: De aangepaste mapping met eventueel toegevoegde DataSourceCode.
        """
        if "DataSource" in mapping:
            datasource = mapping["DataSource"]
            mapping["DataSourceCode"] = (
                datasource[3:]
                if datasource[:3] == self.source_layer_prefix
                else datasource
            )
        else:
            logger.warning(f"Geen datasource opgegeven voor mapping {mapping['Name']}")
        return mapping

