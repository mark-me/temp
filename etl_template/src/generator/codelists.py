import json
from pathlib import Path

import yaml
import polars as pl

from log_config import logging

logger = logging.getLogger(__name__)


class CodeList:
    """Class Get_Enumerations is checking if there are emun files for a source system, loads the files and transform then in a  uniform format.
    Format is:
    SourceSystem, ElementName, Code, Label_EN, Label_NL, Description_EN, Description_NL

    """

    def __init__(self, inputfolder: Path, outputfile: Path):
        """Init Class Create_DDL.\
        Reads config.yml
        Orchestrates flow

        """
        self.lst_codeList = []
        self.inputfolder = inputfolder
        self.outputfile = outputfile

    def read_CodeLists(self):
        """
        nnb
        """
        logger.info("read_CodeLists")
        self.__read_DMS_CodeList()
        self.__read_AGS_CodeList()
        pass

    def __read_DMS_CodeList(self):
        """
        nnb
        """
        logger.info("Start reading codeList file for DMS.")
        for folder in self.inputfolder.iterdir():
            if folder.is_dir() and folder.name.upper() == "DMS":
                if sum(1 for x in folder.glob("*") if x.is_file()) > 1:
                    logger.warning(
                        f"CodeList folder for source {folder.name} has more than 1 file. This folder contains more than one file, this may lead to duplicates"
                    )
                for file in folder.iterdir():
                    if file.is_file() and file.suffix == ".xls":
                        df_dmsCodeList = pl.read_excel(
                            source=file.resolve(),
                            sheet_name="DMS.core Code List Elements",
                        )
                        df_dmsCodeList = df_dmsCodeList.drop(df_dmsCodeList.columns[2])
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
                                df_dmsCodeList.columns[6]: "Description_NL"
                            }
                        )

                        # Replace NONE with Underscores
                        df_dmsCodeList = df_dmsCodeList.fill_null("")
                        self.lst_codeList = df_dmsCodeList.to_dicts()
                        pass

    def __read_AGS_CodeList(self):
        """
        nnb
        """
        logger.info("Start reading codeList file for AGS.")
        for folder in self.inputfolder.iterdir():
            if folder.is_dir() and folder.name.upper() == "AGS":
                if sum(1 for x in folder.glob("*") if x.is_file()) > 1:
                    logger.warning(
                        f"CodeList folder for source {folder.name} has more than 1 file. This folder contains more than one file, this may lead to duplicates"
                    )
                for file in folder.iterdir():
                    if file.is_file() and file.suffix == ".xls":
                        df_agsCodeList = pl.read_excel(
                            source=file.resolve(),
                            sheet_name="DMS.core Code List Elements",
                        )
                        df_agsCodeList = df_agsCodeList.drop(df_agsCodeList.columns[2])
                        df_agsCodeList = df_agsCodeList.drop(df_agsCodeList.columns[6])
                        df_agsCodeList.insert_column(
                            0, pl.lit(folder.name.upper()).alias("SourceSystem")
                        )
                        df_agsCodeList = df_agsCodeList.rename(
                            {
                                df_agsCodeList.columns[1]: "ElementName",
                                df_agsCodeList.columns[2]: "Code",
                                df_agsCodeList.columns[3]: "Label_EN",
                                df_agsCodeList.columns[4]: "Description_EN",
                                df_agsCodeList.columns[5]: "Label_NL",
                                df_agsCodeList.columns[6]: "Description_NL"
                            }
                        )

                        # Replace NONE with Underscores
                        df_agsCodeList = df_agsCodeList.fill_null("")
                        self.lst_codeList.extend(df_agsCodeList.to_dicts())
                        pass

    def write_CodeLists(self):
        """
        Function to create a Json file based on the lst_codeList.
        This file is needed for the publisher class
        """
        with open(self.outputfile, mode="w", encoding="utf-8") as file_codeList:
                    json.dump(self.lst_codeList, file_codeList, indent=4)
        logger.info(f"Written dict_created_ddls to JSON file: {self.outputfile.resolve()}")

# Run Current Class
if __name__ == "__main__":
    print("Done")
