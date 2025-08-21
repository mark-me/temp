from pathlib import Path

from logtools import get_logger
from tqdm import tqdm

from .ddl_views_base import DDLViewBase, DdlType

logger = get_logger(__name__)


class DDLSourceViews(DDLViewBase):
    def __init__(self, path_output: Path, platform: str):
        super().__init__(
            path_output=path_output, platform=platform, ddl_type=DdlType.SOURCE_VIEW
        )

    def generate_ddls(self, mappings: list) -> None:
        """
        Creëert alle source views van de verschillende niet-aggregatie entiteiten die in models zijn opgenomen en schrijft deze weg naar een folder in de repository.
        De source views bevatten de ETL om de doeltabel te vullen met data.

        Args:
            mappings (dict): Bevat alle mappings uit een RETW bestand
        """
        for mapping in tqdm(mappings, desc="Genereren Source Views", colour="#93c47d"):
            if mapping["EntityTarget"]["Stereotype"] == "mdde_AggregateBusinessRule":
                continue
            content = self._render_source_view(mapping)
            path_file_output = self.get_output_file_path(mapping)
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
        content = self.template.render(mapping=mapping)
        return content

