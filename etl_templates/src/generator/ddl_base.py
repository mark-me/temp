from jinja2 import Template

class DDLGeneratorBase:
    def __init__(self, dir_output: str, ddl_template: Template):
        """
        Initialiseert een DDLViews instantie voor het genereren van DDL-bestanden voor views.

        Deze constructor stelt de outputdirectory en de te gebruiken Jinja2-template in voor het genereren van DDL's.

        Args:
            dir_output (str): De directory waarin de DDL-bestanden worden opgeslagen.
            ddl_template (Template): De Jinja2-template die gebruikt wordt voor het renderen van de DDL.
        """
        self.dir_output = dir_output
        self.template = ddl_template
        self.files_generated = []

    def save_generated_object(
        self, content: str, path_file_output: str
    ):
        """
        Slaat de gegenereerde source view DDL op in het opgegeven pad en registreert het bestand in de DDL-lijst.

        Deze methode schrijft de geformatteerde SQL naar een bestand en voegt het bestand toe aan de lijst van aangemaakte DDL's.

        Args:
            content (str): De te schrijven SQL-inhoud.
            dir_output (Path): De outputdirectory voor de source view.
            file_output (str): De bestandsnaam van de source view.
            path_file_output (str): Het volledige pad waar de source view wordt opgeslagen.
        """
        with open(path_file_output, mode="w", encoding="utf-8") as file_ddl:
            file_ddl.write(content)
