from pathlib import Path

from logtools import get_logger

from .ddl_base import DDLGeneratorBase, DdlType

logger = get_logger(__name__)


class DDLViewBase(DDLGeneratorBase):
    def __init__(self, path_output: Path, platform: str, ddl_type: DdlType):
        super().__init__(path_output=path_output, platform=platform, ddl_type=ddl_type)

    def get_output_file_path(self, mapping: dict)-> Path:
        """
        Bepaalt het pad voor het opslaan van een source view DDL-bestand.

        Deze methode maakt de benodigde directorystructuur aan en retourneert het volledige pad naar het DDL-bestand.

        Args:
            mapping (dict): Mappinginformatie van de entiteit.

        Returns:
            Path: Het volledige pad naar het DDL-bestand.
        """
        path_output = self.path_output / mapping["EntityTarget"]["CodeModel"] / "Views"
        path_output.mkdir(parents=True, exist_ok=True)
        file_output = f"vw_src_{mapping['Name']}.sql"
        return path_output / file_output
