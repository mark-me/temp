from enum import Enum
from pathlib import Path
import re

import sqlparse
from jinja2 import Environment, FileSystemLoader
from logtools import get_logger

from .sql_formatter import SqlFormatter

logger = get_logger(__name__)


class DdlType(Enum):
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


class DDLGeneratorBase:
    def __init__(self, path_output: Path, platform: str, ddl_type: DdlType):
        """
        Initialiseert een DDLViews instantie voor het genereren van DDL-bestanden voor views.

        Deze constructor stelt de outputdirectory en de te gebruiken Jinja2-template in voor het genereren van DDL's.

        Args:
            path_output (Path): De directory waarin de DDL-bestanden worden opgeslagen.
            ddl_type (DdlType): De Jinja2-template die gebruikt wordt voor het renderen van de DDL.
        """
        self.platform = platform
        self.dir_templates = Path(__file__).parent / "templates" / platform
        self.ddl_type = ddl_type
        self.path_output = path_output
        self.template = self._get_template()
        self.files_generated = []

    def _get_template(self):
        environment = Environment(
            loader=FileSystemLoader(self.dir_templates),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return environment.get_template(self.ddl_type.value)

    def save_generated_object(self, content: str, path_file_output: str) -> None:
        """
        Slaat de gegenereerde source view DDL op in het opgegeven pad en registreert het bestand in de DDL-lijst.

        Deze methode schrijft de geformatteerde SQL naar een bestand en voegt het bestand toe aan de lijst van aangemaakte DDL's.

        Args:
            content (str): De te schrijven SQL-inhoud.
            path_file_output (str): Het volledige pad waar de source view wordt opgeslagen.
        """
        if self.platform in ["dedicated-pool"]:
            content = self.format_sql(sql_content=content)

        with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)

    def format_sql(self, sql_content: str) -> str:
        """Formatteert SQL statement(s)

        Args:
            sql_content (str): SQL statement(s) die geformatteerd dienen te worden

        Returns:
            str: Geformatteerd(e) SQL statement(s)
        """
        formatter = SqlFormatter()
        if self.ddl_type == DdlType.ENTITY:
            formatted = formatter.format(sql=sql_content)
        elif self.ddl_type in [DdlType.SOURCE_VIEW, DdlType.SOURCE_VIEW_AGGR]:
            # formatted = sqlparse.format(
            #     sql_content, reindent=True, comma_first=True, keyword_case="upper"
            # )
            formatted = formatter.format(sql=formatted)
        return formatted
