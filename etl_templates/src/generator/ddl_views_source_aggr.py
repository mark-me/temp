from pathlib import Path

import sqlparse
from jinja2 import Template
from logtools import get_logger
from tqdm import tqdm

from .ddl_views_base import DDLViewBase

logger = get_logger(__name__)


class DDLSourceViewsAggr(DDLViewBase):
    def __init__(self, dir_output: str, ddl_template: Template):
        super().__init__(dir_output=dir_output, ddl_template=ddl_template)

    def generate_ddls(self, mappings: dict):
        """
        Creëert alle source views van de verschillende aggregatie entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository.
        De source views bevatten de ETL om de doeltabel te vullen met data.

        Args:
            mappings (dict): Bevat alle mappings uit een RETW bestand
        """
        for mapping in tqdm(mappings, desc="Genereren Source Views Aggregates", colour="#6aa84f"):
            if mapping["EntityTarget"]["Stereotype"] != "mdde_AggregateBusinessRule":
                continue
            mapping["Name"] =f"{mapping["Name"]}"
            self._set_datasource_code(mapping)
            mapping = self._set_source_view_aggr_derived(mapping=mapping)
            content = self._render_source_view_aggr(mapping=mapping)
            dir_output, file_output, path_file_output = (
                self._get_source_view_aggr_paths(mapping=mapping)
            )
            self.save_generated_object(
                content=content, path_file_output=path_file_output
            )
            logger.info(
                f"Written Source view aggregation DDL {Path(path_file_output).resolve()}"
            )

    def _get_source_view_aggr_paths(self, mapping: dict):
        dir_output = Path(
            f"{self.dir_output}/{mapping['EntityTarget']['CodeModel']}/Views/"
        )
        dir_output.mkdir(parents=True, exist_ok=True)
        mapping["Name"] =f"{mapping["Name"]}"
        file_output = f"vw_src_{mapping['Name']}.sql"
        path_file_output = f"{dir_output}/{file_output}"
        return dir_output, file_output, path_file_output

    def _set_source_view_aggr_derived(self, mapping: dict) -> dict:
        """Stelt afgeleiden in voor de entiteit die gebruikt worden bij de implementatie

        Args:
            entity (dict): Entiteit waarvan de implementatiespecifieke afleidingen worden toegevoegd

        Returns:
            dict: Gewijzigde entiteitsdata
        """
        dict_aggr_functions = {
            "AVERAGE": "AVG",
            "COUNT": "COUNT",
            "MAXIMUM": "MAX",
            "MINIMUM": "MIN",
            "SUM": "SUM",
        }
        for attr_mapping in mapping["AttributeMapping"]:
            if "Expression" in attr_mapping:
                attr_mapping["Expression"] = dict_aggr_functions[attr_mapping["Expression"]]
        return mapping

    def _render_source_view_aggr(self, mapping: dict) -> str:
        content = self.template.render(mapping=mapping)
        return sqlparse.format(content, reindent=True, keyword_case="upper")
