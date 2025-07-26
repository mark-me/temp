from pathlib import Path

import polars as pl
from logtools import get_logger

logger = get_logger(__name__)


class CodeListReader():
    def __init__(self, dir_input: str):
        self.dir_input = dir_input
        self.lst_codes = []

    def read(self) -> list[dict]:
        """
        Leest en combineert code lists voor de systemen DMS en AGS.

        Roept de interne leesfunctie aan voor beide systemen, voegt de resultaten samen en retourneert de gecombineerde lijst.

        Returns:
            list[dict]: Een gecombineerde lijst van code lists voor DMS en AGS.
        """
        logger.info("read_CodeLists")
        lst_dms = self._read_system_list(system="DMS")
        self.lst_codes.extend(lst_dms)
        lst_ags = self._read_system_list(system="AGS")
        self.lst_codes.extend(lst_ags)
        return self.lst_codes

    def _read_system_list(self, system: str) -> list:
        """
        Leest de code list bestanden voor het opgegeven systeem en voegt deze toe aan de lijst met code lists.

        Deze functie zoekt naar .xls-bestanden in de systeem-map, verwerkt de inhoud en voegt de resultaten toe aan lst_codeList.

        Args:
            system (str): De naam van het systeem waarvan de code lists gelezen moeten worden.

        Returns:
            None: De functie wijzigt de lst_codeList in-place.
        """
        lst_codes = []
        logger.info(f"Lezen codeList bestand(en) voor {system}.")
        folder = self.dir_input / system
        if not folder.exists():
            logger.error(
                f"Kan de code directory niet vinden voor `{system}` in de directory '{self.dir_input}'"
            )
            return
        files_xlsx = self._get_xls_files(folder)
        if len(files_xlsx) > 1:
            logger.warning(
                f"CodeList directory voor {folder.name} bevat meer dan 1 bestand, dit kan dubbele codes tot gevolg hebben."
            )
        for file_xlsx in files_xlsx:
            df_dmsCodeList = self._read_and_process_xls(file_xlsx, folder)
            lst_codes.extend(df_dmsCodeList.to_dicts())
        return lst_codes

    def _get_xls_files(self, folder: Path) -> list[Path]:
        """
        Zoekt naar alle .xls-bestanden in de opgegeven folder.

        Doorloopt de folder en selecteert alle bestanden met de extensie .xls voor verdere verwerking.

        Args:
            folder (Path): De folder waarin gezocht wordt naar .xls-bestanden.

        Returns:
            list[Path]: Een lijst van paden naar gevonden .xls-bestanden.
        """
        return [
            file
            for file in folder.iterdir()
            if file.is_file() and file.suffix == ".xls"
        ]

    def _read_and_process_xls(self, file_xlsx: Path, folder: Path) -> pl.DataFrame:
        """
        Leest en verwerkt een .xls-bestand met code lists voor een specifiek systeem.

        Deze functie leest het Excel-bestand in, verwijdert overbodige kolommen, voegt de bron toe,
        hernoemt de kolommen naar gestandaardiseerde namen en vult lege waarden aan.

        Args:
            file_xlsx (Path): Het pad naar het .xls-bestand.
            folder (Path): De folder (bron) waartoe het bestand behoort.

        Returns:
            pl.DataFrame: Een Polars DataFrame met opgeschoonde en gestandaardiseerde code list data.
        """
        df_dmsCodeList = self._read_xls(file_xlsx)
        df_dmsCodeList = self._drop_unnecessary_columns(df_dmsCodeList)
        df_dmsCodeList = self._insert_source_system_column(df_dmsCodeList, folder)
        df_dmsCodeList = self._rename_columns(df_dmsCodeList)
        df_dmsCodeList = self._fill_nulls(df_dmsCodeList)
        return df_dmsCodeList

    def _read_xls(self, file_xlsx: Path) -> pl.DataFrame:
        """
        Leest een .xls-bestand in als een Polars DataFrame.

        Opent het opgegeven Excel-bestand en leest de sheet 'DMS.core Code List Elements' in voor verdere verwerking.

        Args:
            file_xlsx (Path): Het pad naar het .xls-bestand.

        Returns:
            pl.DataFrame: Een Polars DataFrame met de ingelezen data van het Excel-bestand.
        """
        return pl.read_excel(
            source=file_xlsx.resolve(),
            sheet_name="DMS.core Code List Elements",  # FIXME: Klopt dit? Is alles op deze sheet-naam of is deze variabel met systemen?
            infer_schema_length=None
        )

    def _drop_unnecessary_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Verwijdert overbodige kolommen uit het DataFrame met code list data.

        Deze functie verwijdert specifieke kolommen die niet nodig zijn voor verdere verwerking,
        zodat alleen relevante data overblijft.

        Args:
            df (pl.DataFrame): Het Polars DataFrame met de originele code list data.

        Returns:
            pl.DataFrame: Het DataFrame zonder de overbodige kolommen.
        """
        df = df.drop(df.columns[2])
        df = df.drop(df.columns[6])
        return df

    def _insert_source_system_column(self, df: pl.DataFrame, folder: Path) -> pl.DataFrame:
        """
        Voegt een kolom 'SourceSystem' toe aan het DataFrame met de naam van de bronfolder.

        Deze functie plaatst de naam van het systeem als eerste kolom in het DataFrame,
        zodat duidelijk is uit welk systeem de code list afkomstig is.

        Args:
            df (pl.DataFrame): Het Polars DataFrame waarin de kolom toegevoegd wordt.
            folder (Path): De folder waarvan de naam als bron wordt gebruikt.

        Returns:
            pl.DataFrame: Het DataFrame met toegevoegde 'SourceSystem' kolom.
        """
        df.insert_column(
            0, pl.lit(folder.name.upper()).alias("SourceSystem")
        )
        return df

    def _rename_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Hernoemt de kolommen van het DataFrame naar gestandaardiseerde namen voor code lists.

        Deze functie wijzigt de kolomnamen zodat ze overeenkomen met de verwachte structuur voor verdere verwerking.

        Args:
            df (pl.DataFrame): Het Polars DataFrame waarvan de kolommen hernoemd moeten worden.

        Returns:
            pl.DataFrame: Het DataFrame met hernoemde kolommen.
        """
        return df.rename(
            {
                df.columns[1]: "ElementName",
                df.columns[2]: "Code",
                df.columns[3]: "Label_EN",
                df.columns[4]: "Description_EN",
                df.columns[5]: "Label_NL",
                df.columns[6]: "Description_NL",
            }
        )

    def _fill_nulls(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Vult alle lege waarden (nulls) in het DataFrame met lege strings.

        Deze functie zorgt ervoor dat ontbrekende waarden in de code list data geen nulls zijn,
        maar lege strings, zodat verdere verwerking en export correct verloopt.

        Args:
            df (pl.DataFrame): Het Polars DataFrame waarin nulls vervangen moeten worden.

        Returns:
            pl.DataFrame: Het DataFrame met alle nulls vervangen door lege strings.
        """
        return df.fill_null("")
