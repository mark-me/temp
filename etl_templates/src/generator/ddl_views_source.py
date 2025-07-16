from pathlib import Path

from jinja2 import Template
from logtools import get_logger
from tqdm import tqdm

from .ddl_views_base import DDLViewBase

logger = get_logger(__name__)


class DDLSourceViews(DDLViewBase):
    def __init__(self, dir_output: str, ddl_template: Template):
        super().__init__(dir_output=dir_output, ddl_template=ddl_template)

    def generate_ddls(self, mappings: list):
        """
        Creëert alle source views van de verschillende niet-aggregatie entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository.
        De source views bevatten de ETL om de doeltabel te vullen met data.

        Args:
            mappings (dict): Bevat alle mappings uit een RETW bestand
            identifiers (dict): De JSON (RETW Output) geconverteerd naar een dictionary
        """
        for mapping in tqdm(mappings, desc="Genereren Source Views", colour="#93c47d"):
            if mapping["EntityTarget"]["Stereotype"] == "mdde_AggregateBusinessRule":
                continue
            mapping["Name"] =f"{mapping["Name"]}"
            self._set_datasource_code(mapping)
            content = self._render_source_view(mapping)
            path_file_output = self._get_source_view_paths(
                mapping
            )
            self.save_generated_object(
                content=content, path_file_output=path_file_output
            )
            logger.info(f"Written Source view DDL {Path(path_file_output).resolve()}")

    def _render_source_view(self, mapping: dict) -> str:
        """
        Genereert en formatteert de SQL voor een source view op basis van de mapping.

        Deze methode rendert de source view met behulp van de Jinja2 template en formatteert de SQL met sqlparse.

        Args:
            mapping (dict): De mapping die gebruikt wordt om de source view te genereren.

        Returns:
            str: De geformatteerde SQL-string voor de source view.
        """
        mapping["Name"] =f"{mapping["Name"]}"
        content = self.template.render(mapping=mapping)
        #content = sqlparse.format(content, reindent=True, keyword_case="upper")
        return content

    def _get_source_view_paths(self, mapping: dict) -> tuple:
        """
        Bepaalt de outputpaden voor het opslaan van een gegenereerde source view DDL.

        Deze methode genereert het output directory-pad, de bestandsnaam en het volledige pad voor de source view op basis van de mapping.

        Args:
            mapping (dict): De mapping die gebruikt wordt om de paden te bepalen.

        Returns:
            tuple: (dir_output, file_output, path_file_output)
        """
        dir_output = Path(
            f"{self.dir_output}/{mapping['EntityTarget']['CodeModel']}/Views/"
        )
        dir_output.mkdir(parents=True, exist_ok=True)
        file_output = f"vw_src_{mapping['Name']}.sql"
        path_file_output = f"{dir_output}/{file_output}"
        return path_file_output
