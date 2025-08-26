from datetime import datetime
from copy import deepcopy
import json
from pathlib import Path

import xmltodict
from logtools import get_logger

from .base_extractor import BaseExtractor
from .domains_extractor import DomainsExtractor
from .mapping_extractor import MappingExtractor
from .model_extractor import ModelExtractor
from .stereotype_extractor import StereotypeExtractor

logger = get_logger(__name__)


class PDDocument(BaseExtractor):
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

    def extract_to_json(self, path_file_output: Path):
        """Schrijft het geëxtraheerde en getransformeerde model, filters, scalars, aggregaten en mappings naar een outputbestand.

        Deze functie verzamelt alle relevante data uit het logisch datamodel en schrijft deze als JSON naar het opgegeven bestandspad.

        Args:
            path_file_output (Path): Het pad waar het resultaatbestand wordt opgeslagen.
        """
        pd_content = self._read_file_model()
        if not pd_content:
            return None
        dict_extract = {"Info": self._get_document_info(pd_content=pd_content)}
        domains = self._get_domains(pd_content=pd_content)

        filters = self._get_filters(pd_content=pd_content, domains=domains)
        dict_extract["Filters"] = filters

        scalars = self._get_scalars(pd_content=pd_content, domains=domains)
        dict_extract["Scalars"] = scalars

        models = self._get_models(pd_content=pd_content, domains=domains)
        dict_extract["Models"] = models

        mappings = self._get_mappings(
            pd_content=pd_content,
            models=models,
            filters=filters,
            scalars=scalars,
            domains=domains,
        )
        dict_extract["Mappings"] = mappings

        self._write_json(path_output=path_file_output, dict_extract=dict_extract)

    def _read_file_model(self) -> dict | None:
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
        document_info = {
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
        return document_info

    def _get_domains(self, pd_content: dict) -> list[dict] | None:
        extractor = DomainsExtractor(
            pd_content=pd_content, file_pd_ldm=self.file_pd_ldm
        )
        domains = extractor.get_domains()
        return domains

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
            file_pd_ldm=self.file_pd_ldm,
        )
        filters = extractor.get_filters(dict_domains=domains)
        if not filters:
            logger.debug(f"Geen filters gevonden in '{self.file_pd_ldm}'")
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
            file_pd_ldm=self.file_pd_ldm,
        )
        scalars = extractor.get_scalars(dict_domains=domains)
        if not scalars:
            logger.debug(f"Geen scalars gevonden in '{self.file_pd_ldm}'")
        return scalars

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
            file_pd_ldm=self.file_pd_ldm,
        )
        aggregates = extractor.get_aggregates(dict_domains=domains)
        return aggregates

    def _get_models(self, pd_content: dict, domains: dict) -> list[dict]:
        """Haalt alle model objecten op uit het logisch data model.

        Deze functie verwerkt het opgegeven Power Designer model en retourneert een lijst van model dictionaries.

        Args:
            pd_content (dict): De inhoud van het Power Designer LDM-bestand.

        Returns:
            list[dict]: Een lijst van dictionaries die de modellen representeren.
        """
        extractor = ModelExtractor(
            pd_content=deepcopy(pd_content), file_pd_ldm=self.file_pd_ldm
        )
        models = extractor.get_models(dict_domains=domains)
        if not models:
            logger.error(f"Geen modellen gevonden in '{self.file_pd_ldm}'")
        return models

    def _get_mappings(
        self,
        pd_content: dict,
        models: list[dict],
        filters: list[dict],
        scalars: list[dict],
        domains: list[dict],
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
        aggregates = self._get_aggregates(pd_content=pd_content, domains=domains)
        mappings = extractor.get_mappings(
            models=models, filters=filters, scalars=scalars, aggregates=aggregates
        )
        if not mappings:
            logger.warning(f"Geen mappings gevonden in '{self.file_pd_ldm}'")
        return mappings

    def _write_json(self, path_output: Path, dict_extract: dict) -> None:
        """Schrijft het opgegeven document als JSON naar het opgegeven bestandspad.

        Deze functie zorgt ervoor dat de outputdirectory bestaat en schrijft het dictionary-object als JSON naar het bestand.

        Args:
            file_output (Path): Het pad waar het resultaatbestand wordt opgeslagen.
            dict_document (dict): Het te schrijven document.
        """
        path = Path(path_output)
        Path(path.parent).mkdir(parents=True, exist_ok=True)
        with open(path_output, "w", encoding="utf-8") as outfile:
            json.dump(
                dict_extract, outfile, indent=4, default=self._serialize_datetime
            )
        logger.info(f"Document output is written to '{path_output}'")

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
