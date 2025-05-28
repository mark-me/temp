from jinja2 import Template
from logtools import get_logger

from .ddl_base import DDLGeneratorBase

logger = get_logger(__name__)


class DDLViewBase(DDLGeneratorBase):
    def __init__(self, dir_output: str, ddl_template: Template):
        super().__init__(dir_output=dir_output, ddl_template=ddl_template)
        self.source_layer_prefix = "SL_"

    def _set_datasource_code(self, mapping: dict) -> dict:
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
                datasource[len(self.source_layer_prefix):]
                if datasource[:len(self.source_layer_prefix)] == self.source_layer_prefix
                else datasource
            )
        else:
            logger.error(f"Geen datasource opgegeven voor mapping {mapping['Name']}")
        return mapping

