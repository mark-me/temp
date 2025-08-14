from pathlib import Path

import sqlparse
from logtools import get_logger
from tqdm import tqdm

from .ddl_views_base import DDLViewBase, DDLType

logger = get_logger(__name__)


class DDLSourceViewsAggr(DDLViewBase):
    def __init__(self, path_output: Path, platform: str):
        super().__init__(
            path_output=path_output,
            platform=platform,
            ddl_type=DDLType.SOURCE_VIEW_AGGR,
        )

    def generate_ddls(self, mappings: dict) -> None:
        """
        Creëert alle source views van de verschillende aggregatie entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository.
        De source views bevatten de ETL om de doeltabel te vullen met data.

        Args:
            mappings (dict): Bevat alle mappings uit een RETW bestand
        """
        for mapping in tqdm(
            mappings, desc="Genereren Source Views Aggregates", colour="#6aa84f"
        ):
            if mapping["EntityTarget"]["Stereotype"] != "mdde_AggregateBusinessRule":
                continue
            mapping["Name"] = f"{mapping['Name']}"
            self._set_datasource_code(mapping)
            mapping = self._set_source_view_aggr_derived(mapping)
            content = self._render_source_view_aggr(mapping)
            path_file_output = self.get_output_file_path(mapping)
            self.save_generated_object(
                content=content, path_file_output=path_file_output
            )
            logger.info(
                f"Written Source view aggregation DDL {path_file_output.resolve()}"
            )

    def _get_source_view_aggr_paths(self, mapping: dict) -> Path:
        """
        Bepaalt het pad voor het opslaan van een source view aggregatie DDL-bestand.

        Deze methode maakt de benodigde directorystructuur aan en retourneert het volledige pad naar het DDL-bestand.

        Args:
            mapping (dict): Mappinginformatie van de entiteit.

        Returns:
            Path: Het volledige pad naar het DDL-bestand.
        """
        path_output = self.path_output / mapping["EntityTarget"]["CodeModel"] / "Views"
        path_output.mkdir(parents=True, exist_ok=True)
        file_output = f"vw_src_{mapping['Name']}.sql"
        path_file_output = path_output / file_output
        return path_file_output

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
                attr_mapping["Expression"] = dict_aggr_functions[
                    attr_mapping["Expression"]
                ]
        return mapping

    def _render_source_view_aggr(self, mapping: dict) -> str:
        """
        Genereert de SQL-inhoud voor een source view aggregatie op basis van de mapping.

        Deze methode rendert de Jinja2-template met de mapping en formatteert de SQL indien nodig voor specifieke platforms.

        Args:
            mapping (dict): Mappinginformatie van de entiteit.

        Returns:
            str: De gerenderde en eventueel geformatteerde SQL-inhoud.
        """
        content = self.template.render(mapping=mapping)
        if self.platform in ["dedicated-pool"]:
            content = sqlparse.format(content, reindent=True, keyword_case="upper")
        return content
