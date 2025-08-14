from pathlib import Path

import sqlparse
from logtools import get_logger

from .ddl_base import DDLGeneratorBase, DDLType

logger = get_logger(__name__)


class DDLViewBase(DDLGeneratorBase):
    def __init__(self, path_output: Path, platform: str, ddl_type: DDLType):
        super().__init__(path_output=path_output, platform=platform, ddl_type=ddl_type)
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
            mapping["Name"] =f"{mapping["Name"]}"
            datasource = mapping["DataSource"]
            mapping["DataSourceCode"] = (
                datasource[len(self.source_layer_prefix):]
                if datasource[:len(self.source_layer_prefix)] == self.source_layer_prefix
                else datasource
            )
        else:
            logger.error(f"Geen datasource opgegeven voor mapping {mapping['Name']}")
        return mapping

    def format_sql(self, sql_content: str) -> str:
        """Formatteert SQL statement(s)

        Args:
            sql_content (str): SQL statement(s) die geformatteerd dienen te worden

        Returns:
            str: Geformatteerd(e) SQL statement(s)
        """
        return sqlparse.format(
            sql_content,
            reindent=True,
            comma_first=True,
            keyword_case="upper"
        )