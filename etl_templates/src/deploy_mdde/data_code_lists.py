import polars as pl
from logtools import get_logger

logger = get_logger(__name__)


class CodeList():
    def __init__(self, dir_input: str):
        self.dir_input = dir_input

    def read_CodeLists(self) -> list:
        """
        Leest de code list bestanden voor DMS en AGS en voegt deze toe aan de lijst met code lists.

        Deze functie roept de interne methoden aan om de code lists van beide systemen te verwerken en op te slaan.

        Returns:
            None: De functie wijzigt de lst_codeList in-place.
        """
        logger.info("read_CodeLists")
        self.__read_code_list(system="DMS")
        self.__read_code_list(system="AGS")
        return self.lst_codeList

    def __read_code_list(self, system: str):
        """
        Leest de code list bestanden voor het opgegeven systeem en voegt deze toe aan de lijst met code lists.

        Deze functie zoekt naar .xls-bestanden in de systeemmap, verwerkt de inhoud en voegt de resultaten toe aan lst_codeList.

        Args:
            system (str): De naam van het systeem waarvan de code lists gelezen moeten worden.

        Returns:
            None: De functie wijzigt de lst_codeList in-place.
        """
        logger.info(f"Lezen codeList bestand(en) voor {system}.")
        folder = self.dir_input / system
        if not folder.exists():
            logger.error(
                f"Kan de code directory niet vinden voor `{system}` in de directory '{self.dir_input}'"
            )
            return
        files_xlsx = [
            file
            for file in folder.iterdir()
            if file.is_file() and file.suffix == ".xls"
        ]
        if len(files_xlsx) > 1:
            logger.warning(
                f"CodeList directory voor {folder.name} bevat meer dan 1 bestand, dit kan dubbele codes tot gevolg hebben."
            )
        for file_xlsx in files_xlsx:
            df_dmsCodeList = pl.read_excel(
                source=file_xlsx.resolve(),
                sheet_name="DMS.core Code List Elements",  # FIXME: Klopt dit? Is alles op deze sheetnaam of is deze variabel met systemen?
            )
            df_dmsCodeList = df_dmsCodeList.drop(
                df_dmsCodeList.columns[2]
            )
            df_dmsCodeList = df_dmsCodeList.drop(df_dmsCodeList.columns[6])
            df_dmsCodeList.insert_column(
                0, pl.lit(folder.name.upper()).alias("SourceSystem")
            )
            df_dmsCodeList = df_dmsCodeList.rename(
                {
                    df_dmsCodeList.columns[1]: "ElementName",
                    df_dmsCodeList.columns[2]: "Code",
                    df_dmsCodeList.columns[3]: "Label_EN",
                    df_dmsCodeList.columns[4]: "Description_EN",
                    df_dmsCodeList.columns[5]: "Label_NL",
                    df_dmsCodeList.columns[6]: "Description_NL",
                }
            )
            # Replace NONE with Underscores
            df_dmsCodeList = df_dmsCodeList.fill_null("")
            self.lst_codeList = df_dmsCodeList.to_dicts()
