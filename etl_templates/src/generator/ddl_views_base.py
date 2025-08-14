from pathlib import Path

from logtools import get_logger

from .ddl_base import DDLGeneratorBase, DDLType

logger = get_logger(__name__)


class DDLViewBase(DDLGeneratorBase):
    def __init__(self, path_output: Path, platform: str, ddl_type: DDLType):
        super().__init__(path_output=path_output, platform=platform, ddl_type=ddl_type)
        # FIXME: remove self.source_layer_prefix = "SL_"

    # FIXME: Remove def _set_datasource_code(self, mapping: dict) -> dict:
    #     """
    #     Bepaalt en stelt de DataSourceCode in voor een mapping op basis van de DataSource.

    #     Deze methode controleert of een DataSource aanwezig is en stelt de juiste code in, of logt een waarschuwing als deze ontbreekt.

    #     Args:
    #         mapping (dict): De mapping waarvoor de DataSourceCode wordt bepaald.

    #     Returns:
    #         dict: De aangepaste mapping met eventueel toegevoegde DataSourceCode.
    #     """
    #     if "DataSource" in mapping:
    #         datasource = mapping["DataSource"]
    #         mapping["DataSourceCode"] = (
    #             datasource[len(self.source_layer_prefix):]
    #             if datasource[:len(self.source_layer_prefix)] == self.source_layer_prefix
    #             else datasource
    #         )
    #     else:
    #         logger.error(f"Geen datasource opgegeven voor mapping {mapping['Name']}")
    #     return mapping

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
