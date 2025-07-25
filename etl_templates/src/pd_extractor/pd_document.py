from datetime import datetime
import json
from pathlib import Path

import xmltodict
from logtools import get_logger

from .pd_mapping_extractor import MappingExtractor
from .pd_model_extractor import ModelExtractor
from .pd_stereotype_extractor import StereotypeExtractor
from .pd_transform_object import ObjectTransformer

logger = get_logger(__name__)


class PDDocument:
    """Representeert een logisch datamodel bestand gemaakt in Power Designer
    Dit bestand biedt de mogelijkheid om model en mapping data te extraheren en herstructureert deze data naar een meer
    leesbare format. De output gemaakt op basis van dit bestand is input voor DDL- en ETL generatie.
    """

    def __init__(self, file_pd_ldm: str):
        """Extraheert data uit het Logisch datamodel uit Power Designer en zet dit om in een representatie van objecten

        Args:
            file_pd_ldm (str): Power Designer logisch data model document (.ldm)
        """
        self.file_pd_ldm = file_pd_ldm
        # Extracting data from the file
        self.content = self.read_file_model(file_pd_ldm=file_pd_ldm)
        self.document_info = {}
        self.lst_models = []
        self.lst_filters = []
        self.lst_scalars = []
        self.lst_aggregates = []
        self.lst_mappings = []
        self.transform_objects = ObjectTransformer()

    def get_document_info(self) -> dict:
        """Geeft metadata terug over het ingelezen Power Designer logisch datamodel.

        Deze functie retourneert een dictionary met informatie zoals bestandsnaam, maker, aanmaakdatum, en modelopties.

        Returns:
            dict: Metadata van het Power Designer LDM-bestand.
        """
        return {
            "Filename": str(self.file_pd_ldm),
            "FilenameRepo": self.content['a:RepositoryFilename'],
            "Creator": self.content["a:Creator"],
            "DateCreated": datetime.fromtimestamp(int(self.content['a:CreationDate'])),
            "Modifier": self.content['a:Modifier'],
            "DateModified": datetime.fromtimestamp(int(self.content['a:ModificationDate'])),
            "ModelOptions": self.content['a:ModelOptionsText'].split("\n"),
            "PackageOptions": self.content['a:PackageOptionsText'].split("\n"),
        }

    def get_filters(self) -> list:
        """Haalt alle filter objecten op uit het logisch data model

        Returns:
            list : Een lijst van dictionaries die de filters representeren
        """
        stereotype_input = "mdde_FilterBusinessRule"
        extractor = StereotypeExtractor(
            pd_content=self.content, stereotype_input=stereotype_input, file_pd_ldm=self.file_pd_ldm
        )
        logger.debug("Start filter extraction")
        lst_filters = extractor.objects()
        logger.debug("Finished filter extraction")
        self.lst_filters = lst_filters
        return lst_filters

    def get_scalars(self) -> list:
        """Haalt alle scalar objecten (berekende attributen) op uit het logisch data model

        Returns:
            list: Een lijst van dictionaries die de scalars representeren
        """
        stereotype_input = "mdde_ScalarBusinessRule"
        extractor = StereotypeExtractor(
            pd_content=self.content, stereotype_input=stereotype_input, file_pd_ldm=self.file_pd_ldm
        )
        logger.debug("Start scalar extraction")
        lst_scalars = extractor.objects()
        logger.debug("Finished scalar extraction")
        self.lst_scalars = lst_scalars
        return lst_scalars

    def get_aggregates(self) -> list:
        """Haalt alle aggregatie objecten op uit het logisch data model

        Returns:
            list: Een lijst van dictionaries die de aggregaten representeren
        """
        stereotype_input = "mdde_AggregateBusinessRule"
        extractor = StereotypeExtractor(
            pd_content=self.content, stereotype_input=stereotype_input, file_pd_ldm=self.file_pd_ldm
        )
        logger.debug("Start aggregate extraction")
        lst_aggregates = extractor.objects()
        logger.debug("Finished aggregate extraction")
        self.lst_aggregates = lst_aggregates
        return lst_aggregates

    def get_models(self) -> list:
        """Haalt model data, apart van de mappings, op uit het logisch data model

        Returns:
            list: The Power Designer modellen zonder enige mappings
        """
        extractor = ModelExtractor(pd_content=self.content)
        logger.debug("Start model extraction")
        lst_models = extractor.models(lst_aggregates=self.lst_aggregates)
        logger.debug("Finished model extraction")
        self.lst_models = lst_models
        return lst_models

    def get_mappings(self) -> list:
        """Haalt de mappings op die de ETL van het LDM vertegenwoordigen

        Returns:
            list: Een lijst van dictionaries die mapping objects vertegenwoordigen
        """
        if len(self.lst_models) == 0:
            self.get_models()

        extractor = MappingExtractor(pd_content=self.content)
        logger.debug("Start mapping extraction")
        dict_entities = self._all_entities()
        dict_filters = self._all_filters()
        dict_scalars = self._all_scalars()
        dict_aggregates = self._all_aggregates()
        dict_objects = dict_entities | dict_filters | dict_scalars | dict_aggregates
        dict_variables = self._all_variables()
        dict_attributes = self._all_attributes()
        dict_datasources = self._all_datasources()
        lst_mappings = extractor.mappings(
            dict_objects=dict_objects,
            dict_attributes=dict_attributes,
            dict_variables=dict_variables,
            dict_datasources= dict_datasources,
        )
        self.lst_mappings = lst_mappings
        return lst_mappings

    def read_file_model(self, file_pd_ldm: str) -> dict:
        """Leest de XML van het Power Designer LDM in een dictionary

        Args:
            file_pd_ldm (str): Het pad naar Power Designer LDM file

        Returns:
            dict: De Power Designer data geconverteerd naar een dictionary
        """
        # Function not yet used, but candidate for reading XML file
        with open(file_pd_ldm, encoding='utf8') as fd:
            doc = fd.read()
        dict_data = xmltodict.parse(doc)
        dict_data = dict_data["Model"]["o:RootObject"]["c:Children"]["o:Model"]
        return dict_data

    def _all_entities(self) -> dict:
        """Haalt alle entiteiten op ongeacht het model waartoe ze behoren. Daarnaast worden ook alle aggregaties die
        bij een intern model zijn gevonden toegevoegd.

        Returns:
            dict: Elke waarde in de dictionary representeert een entiteit, de sleutel is het interne ID
        """
        dict_result = {}
        for model in self.lst_models:
            lst_entities = model["Entities"]
            for entity in lst_entities:
                if "Stereotype" not in entity:
                    dict_result[entity["Id"]] = {
                        "Id": entity["Id"],
                        "Name": entity["Name"],
                        "Code": entity["Code"],
                        "IdModel": model["Id"],
                        "NameModel": model["Name"],
                        "CodeModel": model["Code"],
                        "IsDocumentModel": not model["IsDocumentModel"],
                        "Stereotype": None
                    }
        return dict_result

    def _all_filters(self) -> list:
        """Haalt alle filters op ongeacht het model waartoe ze behoren.

        Returns:
            list: Elke value uit de list representeert een stereotype, de sleutel is het interne ID
        """
        dict_result = {
            filter["Id"]: {
                "Id": filter["Id"],
                "Name": filter["Name"],
                "Code": filter["Code"],
                "CodeModel": filter["CodeModel"],
                "Variables": filter["Variables"],
                "Stereotype": filter["Stereotype"],
                "SqlVariable": filter["SqlVariable"],
                "SqlExpression": filter["SqlExpression"],
            }
            for filter in self.lst_filters
        }
        return dict_result

    def _all_scalars(self) -> list:
        """Haalt alle scalars op ongeacht het model waartoe ze behoren.

        Returns:
            list: Elke value uit de list representeert een stereotype, de sleutel is het interne ID
        """
        dict_result = {
            scalar["Id"]: {
                "Id": scalar["Id"],
                "Name": scalar["Name"],
                "Code": scalar["Code"],
                "CodeModel": scalar["CodeModel"],
                "Variables": scalar["Variables"],
                "Stereotype": scalar["Stereotype"],
                "SqlVariable": scalar["SqlVariable"],
                "SqlExpression": scalar["SqlExpression"],
                "SqlExpressionVariables": scalar["SqlExpressionVariables"],
            }
            for scalar in self.lst_scalars
        }
        return dict_result

    def _all_aggregates(self) -> list:
        """Haalt alle aggregaties op ongeacht het model waartoe ze behoren.

        Returns:
            list: Elke value uit de list representeert een stereotype, de sleutel is het interne ID
        """
        dict_result = {
            aggregates["Id"]: {
                "Id": aggregates["Id"],
                "Name": aggregates["Name"],
                "Code": aggregates["Code"],
                "CodeModel": aggregates["CodeModel"],
                "Variables": aggregates["Attributes"],
                "Stereotype": aggregates["Stereotype"],
            }
            for aggregates in self.lst_aggregates
        }
        return dict_result

    def _all_attributes(self) -> dict:
        """Haalt alle attributen op ongeacht tot welk model of entiteit zij behoren

        Returns:
            dict: Elke waarde in de dictionary representeert een attribuut, de sleutel is het interne ID
        """
        dict_result = {}
        for model in self.lst_models:
            lst_entities = model["Entities"]
            for entity in lst_entities:
                if "Attributes" in entity:
                    lst_attributes = entity["Attributes"]
                    for attr in lst_attributes:
                        dict_result[attr["Id"]] = {
                            "Id": attr["Id"],
                            "Name": attr["Name"],
                            "Code": attr["Code"],
                            "IdModel": model["Id"],
                            "NameModel": model["Name"],
                            "CodeModel": model["Code"],
                            "IsDocumentModel": not model["IsDocumentModel"],
                            "IdEntity": entity["Id"],
                            "NameEntity": entity["Name"],
                            "CodeEntity": entity["Code"],
                            "StereotypeEntity": None,
                        }
        return dict_result

    def _all_variables(self) -> dict:
        """Extraheert de variabelen van de filters, scalars en aggregaten

        Returns:
            dict: Gevonden variabelen met de waarden gebaseerd op hun eigen interne referentie ("o")
        """
        dict_result = {}
        lst_stereotypes = self.lst_filters + self.lst_scalars #+ self.lst_aggregates
        for stereotypes in lst_stereotypes:
            lst_variables = stereotypes["Variables"]
            for var in lst_variables:
                dict_result[var["Id"]] = {
                    "Id": var["Id"],
                    "Name": var["Name"],
                    "Code": var["Code"],
                    "CodeModel": stereotypes["Code"],
                    "IdEntity": stereotypes["Id"],
                    "NameEntity": stereotypes["Name"],
                    "CodeEntity": stereotypes["Code"],
                    "StereotypeEntity": stereotypes["Stereotype"]
                }
        return dict_result

    def _all_datasources(self) -> dict:
        dict_result = {}
        for model in self.lst_models:
            if "DataSources" in model:
                dict_result = model["DataSources"]
        return dict_result

    def _serialize_datetime(self, obj):
        """Haalt alle datetime voorkomens op en formatteert deze naar een ISO-format

        Args:
            obj (any): Object dat (indien mogelijk) geformatteerd wordt naar een correct ISO datum formaat

        Returns:
            Datetime: Geformatteerd in ISO-format
        """

        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable")

    def write_result(self, file_output: str):
        """Schrijft het geëxtraheerde en getransformeerde model, filters, scalars, aggregaten en mappings naar een outputbestand.

        Deze functie verzamelt alle relevante data uit het logisch datamodel en schrijft deze als JSON naar het opgegeven bestandspad.

        Args:
            file_output (str): Het pad waar het resultaatbestand wordt opgeslagen.
        """

        dict_document = {"Info": self.get_document_info()}
        lst_filters = self.get_filters()
        lst_scalars = self.get_scalars()
        lst_aggregates = self.get_aggregates()
        lst_models = self.get_models()
        if "c:Mappings" in self.content:
            lst_mappings = self.get_mappings()
        else:
            lst_mappings = []
            logger.warning("Geen mappings gevonden in het model")
        dict_document["Models"] = lst_models
        if not lst_filters:
            logger.debug(f"Geen filters geschreven naar  '{file_output}'")
        else:
            dict_document["Filters"] = lst_filters
        if not lst_scalars:
            logger.debug(f"No scalars to write to  '{file_output}'")
        else:
            dict_document["Scalars"] = lst_scalars
        if not lst_aggregates:
            logger.debug(f"No aggregates to write to  '{file_output}'")
        if not lst_mappings:
            logger.warning(f"Geen mappings om te schrijven in '{file_output}'")
        else:
            dict_document["Mappings"] = lst_mappings
        path = Path(file_output)
        Path(path.parent).mkdir(parents=True, exist_ok=True)
        with open(file_output, "w") as outfile:
            json.dump(
                dict_document, outfile, indent=4, default=self._serialize_datetime
            )
        logger.info(f"Document output is written to '{file_output}'")
