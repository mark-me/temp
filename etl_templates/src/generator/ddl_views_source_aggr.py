from pathlib import Path

import sqlparse
from jinja2 import Template
from log_config import logging

from .ddl_views_base import DDLViewBase

logger = logging.getLogger(__name__)



class DDLSourceViewsAggr(DDLViewBase):
    def __init__(self, dir_output: str, ddl_template: Template):
        super().__init__(dir_output=dir_output, ddl_template=ddl_template)

    def generate_ddl_source_view_aggr(self, mappings: dict):
        """
        Creëert alle source views van de verschillende aggregatie entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository.
        De source views bevatten de ETL om de doeltabel te vullen met data.

        Args:
            mappings (dict): Bevat alle mappings uit een RETW bestand
        """
        for mapping in mappings:
            if mapping["EntityTarget"]["Stereotype"] != "mdde_AggregateBusinessRule":
                continue

            self.__set_datasource_code(mapping)
            content = self.__render_source_view_aggr(mapping=mapping)
            dir_output, file_output, path_file_output = (
                self.__get_source_view_aggr_paths(mapping=mapping)
            )
            self.save_generated_object(content=content, path_file_output=path_file_output)
            logger.info(
                f"Written Source view aggregation DDL {Path(path_file_output).resolve()}"
            )

    def __get_source_view_aggr_paths(self, mapping: dict):
        dir_output = Path(
            f"{self.dir_generator}/CentralLayer/{mapping['EntityTarget']['CodeModel']}/Views/"
        )
        dir_output.mkdir(parents=True, exist_ok=True)
        file_output = f"vw_src_{mapping['Name']}.sql"
        path_file_output = f"{dir_output}/{file_output}"
        return dir_output, file_output, path_file_output

    def __render_source_view_aggr(self, mapping: dict) -> str:
        content = self.templates["SourceViewsaggr"].render(mapping=mapping)
        return sqlparse.format(content, reindent=True, keyword_case="upper")
