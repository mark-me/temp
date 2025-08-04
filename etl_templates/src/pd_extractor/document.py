from datetime import datetime
import json
from pathlib import Path

import xmltodict
from logtools import get_logger

from .base_extractor import ExtractorBase
from .domains_extractor import DomainsExtractor
from .mapping_extractor import MappingExtractor
from .model_extractor import ModelExtractor
from .stereotype_extractor import StereotypeExtractor

logger = get_logger(__name__)


class PDDocument(ExtractorBase):
    """Representeert een logisch datamodel bestand gemaakt in Power Designer
    Dit bestand biedt de mogelijkheid om model en mapping data te extraheren en herstructureert deze data naar een meer
    leesbare format. De output gemaakt op basis van dit bestand is input voor DDL- en ETL generatie.
    """

    def __init__(self, file_pd_ldm: str):
        """Extraheert data uit het Logisch datamodel uit Power Designer en zet dit om in een representatie van objecten

        Args:
            file_pd_ldm (str): Power Designer logisch data model document (.ldm)
        """
        super().__init__(file_pd_ldm=file_pd_ldm)

    def extract_to_json(self, file_output: str):
        """Schrijft het geëxtraheerde en getransformeerde model, filters, scalars, aggregaten en mappings naar een outputbestand.

        Deze functie verzamelt alle relevante data uit het logisch datamodel en schrijft deze als JSON naar het opgegeven bestandspad.

        Args:
            file_output (str): Het pad waar het resultaatbestand wordt opgeslagen.
        """
        pd_content = self._read_file_model()
        dict_document = {"Info": self._get_document_info(pd_content=pd_content)}
        domains = self._get_domains(pd_content=pd_content)
        if filters := self._get_filters(pd_content=pd_content, domains=domains):
            dict_document["Filters"] = filters
        else:
            logger.debug(f"Geen filters geschreven naar  '{file_output}'")
        if scalars := self._get_scalars(pd_content=pd_content, domains=domains):
            dict_document["Scalars"] = scalars
        else:
            logger.debug(f"No scalars to write to  '{file_output}'")
        aggregates = self._get_aggregates(pd_content=pd_content, domains=domains)
        if models := self._get_models(pd_content=pd_content, domains=domains):
            dict_document["Models"] = models
        else:
            logger.error(f"Geen mappings om te schrijven in '{self.file_pd_ldm}'")
        if mappings := self._get_mappings(
            pd_content=pd_content,
            models=models,
            filters=filters,
            scalars=scalars,
            aggregates=aggregates,
        ):
            dict_document["Mappings"] = mappings
        else:
            logger.warning(f"Geen mappings om te schrijven in '{file_output}'")
        self._write_json(file_output=file_output, dict_document=dict_document)

    def _read_file_model(self) -> dict:
        """Leest de XML van het Power Designer LDM in een dictionary

        Args:
            file_pd_ldm (str): Het pad naar Power Designer LDM file

        Returns:
            dict: De Power Designer data geconverteerd naar een dictionary
        """
        try:
            with open(self.file_pd_ldm, encoding="utf8") as file_pd:
                doc = file_pd.read()
            dict_data = xmltodict.parse(doc)
            dict_data = dict_data["Model"]["o:RootObject"]["c:Children"]["o:Model"]
        except (KeyError, TypeError) as e:
            logger.error(f"Onverwachte XML structuur in {self.file_pd_ldm}: {e}")
            return None
        return dict_data

    def _get_document_info(self, pd_content: dict) -> dict:
        """Geeft metadata terug over het ingelezen Power Designer logisch datamodel.

        Deze functie retourneert een dictionary met informatie zoals bestandsnaam, maker, aanmaakdatum, en modelopties.

        Returns:
            dict: Metadata van het Power Designer LDM-bestand.
        """
        return {
            "Filename": str(self.file_pd_ldm),
            "FilenameRepo": pd_content.get("a:RepositoryFilename"),
            "Creator": pd_content.get("a:Creator"),
            "DateCreated": datetime.fromtimestamp(
                int(pd_content.get("a:CreationDate", 0))
            ),
            "Modifier": pd_content.get("a:Modifier"),
            "DateModified": datetime.fromtimestamp(
                int(pd_content.get("a:ModificationDate", 0))
            ),
            "ModelOptions": pd_content.get("a:ModelOptionsText", "").split("\n"),
            "PackageOptions": pd_content.get("a:PackageOptionsText", "").split("\n"),
        }

    def _get_domains(self, pd_content: dict) -> list[dict]:
        extractor = DomainsExtractor(
            pd_content=pd_content, file_pd_ldm=self.file_pd_ldm
        )
        return extractor.get_domains()

    def _get_filters(self, pd_content: dict, domains: dict) -> list[dict]:
        """Haalt alle filter objecten op uit het logisch data model.

        Deze functie verwerkt het opgegeven Power Designer model en retourneert een lijst van filter dictionaries.

        Args:
            pd_content (dict): De inhoud van het Power Designer LDM-bestand.
            dict_domains (dict): Dictionary met domeininformatie.

        Returns:
            list[dict]: Een lijst van dictionaries die de filters representeren.
        """
        extractor = StereotypeExtractor(
            pd_content=pd_content,
            stereotype_input="mdde_FilterBusinessRule",
            file_pd_ldm=self.file_pd_ldm,
        )
        logger.debug("Start filter extraction")
        filters = extractor.get_objects(dict_domains=domains)
        logger.debug("Finished filter extraction")
        return filters

    def _get_scalars(self, pd_content: dict, domains: dict) -> list[dict]:
        """Haalt alle scalar objecten op uit het logisch data model.

        Deze functie verwerkt het opgegeven Power Designer model en retourneert een lijst van scalar dictionaries.

        Args:
            pd_content (dict): De inhoud van het Power Designer LDM-bestand.
            dict_domains (dict): Dictionary met domeininformatie.

        Returns:
            list[dict]: Een lijst van dictionaries die de scalars representeren.
        """
        extractor = StereotypeExtractor(
            pd_content=pd_content,
            stereotype_input="mdde_ScalarBusinessRule",
            file_pd_ldm=self.file_pd_ldm,
        )
        logger.debug("Start scalar extraction")
        lst_scalars = extractor.get_objects(dict_domains=domains)
        logger.debug("Finished scalar extraction")
        return lst_scalars

    def _get_aggregates(self, pd_content: dict, domains: dict) -> list[dict]:
        """Haalt alle aggregate objecten op uit het logisch data model.

        Deze functie verwerkt het opgegeven Power Designer model en retourneert een lijst van aggregate dictionaries.

        Args:
            pd_content (dict): De inhoud van het Power Designer LDM-bestand.
            dict_domains (dict): Dictionary met domeininformatie.

        Returns:
            list[dict]: Een lijst van dictionaries die de aggregates representeren.
        """
        extractor = StereotypeExtractor(
            pd_content=pd_content,
            stereotype_input="mdde_AggregateBusinessRule",
            file_pd_ldm=self.file_pd_ldm,
        )
        logger.debug("Start aggregate extraction")
        lst_aggregates = extractor.get_objects(dict_domains=domains)
        logger.debug("Finished aggregate extraction")
        return lst_aggregates

    def _get_models(self, pd_content: dict, domains: dict) -> list[dict]:
        """Haalt alle model objecten op uit het logisch data model.

        Deze functie verwerkt het opgegeven Power Designer model en retourneert een lijst van model dictionaries.

        Args:
            pd_content (dict): De inhoud van het Power Designer LDM-bestand.

        Returns:
            list[dict]: Een lijst van dictionaries die de modellen representeren.
        """
        extractor = ModelExtractor(pd_content=pd_content, file_pd_ldm=self.file_pd_ldm)
        logger.debug("Start model extraction")
        lst_models = extractor.get_models(dict_domains=domains)
        logger.debug("Finished model extraction")
        return lst_models

    def _get_mappings(
        self,
        pd_content: dict,
        models: list[dict],
        filters: list[dict],
        scalars: list[dict],
        aggregates: list[dict],
    ) -> list[dict]:
        """Haalt alle mapping objecten op uit het logisch data model.

        Deze functie verwerkt het opgegeven Power Designer model en retourneert een lijst van mapping dictionaries.

        Args:
            pd_content (dict): De inhoud van het Power Designer LDM-bestand.
            models (list[dict]): Lijst van model dictionaries.
            filters (list[dict]): Lijst van filter dictionaries.
            scalars (list[dict]): Lijst van scalar dictionaries.
            aggregates (list[dict]): Lijst van aggregaat dictionaries.

        Returns:
            list[dict]: Een lijst van dictionaries die de mappings representeren.
        """
        extractor = MappingExtractor(
            pd_content=pd_content, file_pd_ldm=self.file_pd_ldm
        )
        logger.debug("Start mapping extraction")
        lst_mappings = extractor.get_mappings(
            models=models, filters=filters, scalars=scalars, aggregates=aggregates
        )
        return lst_mappings

    def _write_json(self, file_output: str, dict_document: dict) -> None:
        """Schrijft het opgegeven document als JSON naar het opgegeven bestandspad.

        Deze functie zorgt ervoor dat de outputdirectory bestaat en schrijft het dictionary-object als JSON naar het bestand.

        Args:
            file_output (str): Het pad waar het resultaatbestand wordt opgeslagen.
            dict_document (dict): Het te schrijven document.
        """
        path = Path(file_output)
        Path(path.parent).mkdir(parents=True, exist_ok=True)
        with open(file_output, "w", encoding="utf-8") as outfile:
            json.dump(
                dict_document, outfile, indent=4, default=self._serialize_datetime
            )
        logger.info(f"Document output is written to '{file_output}'")

    def _serialize_datetime(self, obj: datetime) -> str:
        """Converteert een datetime-object naar een ISO 8601 string.

        Deze functie wordt gebruikt voor het serialiseren van datetime-objecten bij het schrijven naar JSON.

        Args:
            obj (datetime): Het te serialiseren object.

        Returns:
            str: De ISO 8601 stringrepresentatie van het datetime-object.

        Raises:
            TypeError: Indien het object geen datetime is.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable")
