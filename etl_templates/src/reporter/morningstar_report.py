
from pathlib import Path
from datetime import date

from jinja2 import Environment, FileSystemLoader
from logtools import get_logger

logger = get_logger(__name__)

class MorningstarReport:
    def __init__(self, path_output: Path):
        """
        Initialiseert een DDLViews instantie voor het genereren van DDL-bestanden voor views.

        Deze constructor stelt de outputdirectory en de te gebruiken Jinja2-template in voor het genereren van DDL's.

        Args:
            path_output (Path): De directory waarin de DDL-bestanden worden opgeslagen.
            ddl_type (DdlType): De Jinja2-template die gebruikt wordt voor het renderen van de DDL.
        """
        self.dir_templates = Path(__file__).parent / "templates"
        self.path_output = path_output
        self.files_generated = []

    def create_report(self, failed_mappings: list, file_png: Path, impacted_mappings: list):
        """
        Genereert een Morningstar rapport op basis van mislukte en beïnvloede mappings.

        Deze methode stelt de benodigde attributen in, laadt de template en genereert het rapport.

        Args:
            failed_mappings (list): Lijst van mappings die zijn mislukt.
            file_png (Path): Pad naar het PNG-bestand dat in het rapport wordt opgenomen.
            impacted_mappings (list): Lijst van mappings die door fouten zijn beïnvloed.
        """
        self.file_png = file_png.resolve()
        self.reporting_date = date.today()
        self.impacted_mappings = impacted_mappings
        self.nr_failed_mappings = len(failed_mappings)
        self.template = self._get_template()
        self.report = self._generate_html_report()

    def _get_template(self):
        """
        Laadt en retourneert de Jinja2-template voor het Morningstar rapport.

        Deze methode initialiseert de Jinja2-omgeving en haalt de juiste template op.
        """
        environment = Environment (
            loader = FileSystemLoader(self.dir_templates),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        return environment.get_template("report.jinja")


    def _generate_html_report(self):
        """
        Genereert het Morningstar rapport en slaat het op als HTML-bestand.

        Deze methode rendert de rapportagegegevens en schrijft het resultaat naar een HTML-bestand.
        """
        content = self._render_source_view(self.reporting_date,self.nr_failed_mappings, self.file_png, self.impacted_mappings)
        self.save_generated_object(
            content=content, path_file_output=self.path_output / "Morningstar_report.html"
        )

    def _render_source_view(self, reporting_date: str, nr_failed_mappings: str, file_png, impacted_mappings):
        """
        Rendert de Morningstar rapportagegegevens naar HTML met behulp van de Jinja2-template.

        Deze methode vult de template met de opgegeven rapportagegegevens en retourneert de HTML-inhoud als string.

        Args:
            reporting_date (str): De datum van de rapportage.
            nr_failed_mappings (str): Het aantal mislukte mappings.
            file_png: Het PNG-bestand dat in het rapport wordt opgenomen.
            impacted_mappings: Lijst van mappings die door fouten zijn beïnvloed.

        Returns:
            str: De gerenderde HTML-inhoud van het rapport.
        """
        content = self.template.render(reporting_date=reporting_date, nr_failed_mappings=nr_failed_mappings, file_png=file_png, impacted_mappings=impacted_mappings)
        return content

    def save_generated_object(self, content: str, path_file_output: str) -> None:
        """
        Slaat de gegenereerde source view DDL op in het opgegeven pad en registreert het bestand in de DDL-lijst.

        Deze methode schrijft de geformatteerde SQL naar een bestand en voegt het bestand toe aan de lijst van aangemaakte DDL's.

        Args:
            content (str): De te schrijven SQL-inhoud.
            path_file_output (str): Het volledige pad waar de source view wordt opgeslagen.
        """

        with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)