from logtools import get_logger

from .base_extractor import BaseExtractor
from .mapping_transformers.mapping import MappingTransformer
from .mapping_transformers.attributes import MappingAttributesTransformer
from .mapping_transformers.composition import SourceCompositionTransformer
from .mapping_transformers.target import TargetEntityTransformer

logger = get_logger(__name__)


class MappingExtractor(BaseExtractor):
    """Extraheert ETL specificaties (mappings) vanuit een Power Designer LDM waarin mappings zijn geïmplementeerd met behulp van de
    CrossBreeze MDDE extensie.
    Transformeert de data in een meer leesbaar format door overbodige informatie te verwijderen en relevante model informatie
    toe te voegen.
    """

    def __init__(self, pd_content: dict, file_pd_ldm: str):
        """Initialiseren voor het extraheren van de mapping informatie

        Args:
            pd_content (dict): Power Designer LDM bestand inhoud (gerepresenteerd als een dictionary)
        """
        super().__init__(file_pd_ldm=file_pd_ldm)
        self.content = pd_content

    def get_mappings(
        self,
        models: list[dict],
        filters: list[dict],
        scalars: list[dict],
        aggregates: list[dict],
    ) -> list[dict]:
        """Verwerkt modellen, filters, scalars en aggregaten tot een lijst van mappingdefinities.

        Deze functie combineert entiteiten, filters, scalars, aggregaten en variabelen om relevante mappings te genereren en transformeren naar een gestructureerd formaat.

        Args:
            models (list[dict]): Lijst van model dictionaries.
            filters (list[dict]): Lijst van filter dictionaries.
            scalars (list[dict]): Lijst van scalar dictionaries.
            aggregates (list[dict]): Lijst van aggregaat dictionaries.

        Returns:
            list[dict]: Een lijst van getransformeerde mappingdefinities.
        """
        dict_entities = self._create_entities_lookup(models=models)
        dict_filters = self._create_filters_lookup(filters=filters)
        dict_scalars = self._create_scalars_lookup(scalars=scalars)
        dict_aggregates = self._create_aggregates_lookup(aggregates=aggregates)
        dict_objects = dict_entities | dict_filters | dict_scalars | dict_aggregates
        dict_variables = self._create_variables_lookup(filters=filters, scalars=scalars)
        dict_attributes = self._create_attributes_lookup(models=models)
        dict_datasources = self._create_datasources_lookup(models=models)

        mappings = self._get_relevant_mappings()
        mappings_transformed = [
            self._process_single_mapping(
                mapping=mapping,
                dict_objects=dict_objects,
                dict_attributes=dict_attributes,
                dict_variables=dict_variables,
                dict_datasources=dict_datasources,
            )
            for mapping in mappings
        ]
        return mappings_transformed

    def _process_single_mapping(
        self,
        mapping: dict,
        dict_objects: dict,
        dict_attributes: dict,
        dict_variables: dict,
        dict_datasources: dict,
    ) -> list[dict]:
        """Verwerkt een enkele mapping en transformeert deze naar een gestructureerd formaat.

        Deze functie normaliseert de mappingnaam, bepaalt de target entiteiten, verwerkt de attributen en genereert de broncompositie.

        Args:
            mapping (dict): De mapping die verwerkt moet worden.
            dict_objects (dict): Gecombineerde dictionary van entiteiten, filters, scalars en aggregaten.
            dict_attributes (dict): Dictionary van attributen.
            dict_variables (dict): Dictionary van variabelen.
            dict_datasources (dict): Dictionary van datasources.

        Returns:
            list[dict]: Een lijst met de getransformeerde broncompositie voor de mapping.
        """
        trf_mapping = MappingTransformer(file_pd_ldm=self.file_pd_ldm)
        mapping = trf_mapping.transform(mapping=mapping)

        # Transform target entity
        trf_target_entity = TargetEntityTransformer(
            file_pd_ldm=self.file_pd_ldm, mapping=mapping
        )
        mapping = trf_target_entity.transform(
            dict_objects=dict_objects,
        )

        # Transform source compositions
        dict_attributes_combined = dict_attributes | dict_variables
        trf_source_composition = SourceCompositionTransformer(
            file_pd_ldm=self.file_pd_ldm, mapping=mapping
        )
        mapping = trf_source_composition.transform(
            dict_attributes=dict_attributes_combined,
            dict_objects=dict_objects,
            dict_datasources=dict_datasources,
        )

        # Transform mapping attributes
        trf_attribute_mapping = MappingAttributesTransformer(
            file_pd_ldm=self.file_pd_ldm, mapping=mapping
        )
        mapping = trf_attribute_mapping.transform(
            dict_attributes=dict_attributes_combined,
        )
        return mapping

    def _create_entities_lookup(self, models: list[dict]) -> dict:
        """Maakt een dictionary van entiteiten uit de opgegeven modellen.

        Deze functie doorloopt alle modellen en voegt entiteiten zonder stereotype toe aan de dictionary, waarbij het interne ID als sleutel wordt gebruikt.

        Args:
            models (list[dict]): Lijst van model dictionaries.

        Returns:
            dict: Dictionary met entiteiten, waarbij de sleutel het interne ID is.
        """
        dict_result = {}
        for model in models:
            entities = [
                entity for entity in model["Entities"] if "Stereotype" not in entity
            ]
            for entity in entities:
                dict_result[entity["Id"]] = {
                    "Id": entity["Id"],
                    "Name": entity["Name"],
                    "Code": entity["Code"],
                    "IdModel": model["Id"],
                    "NameModel": model["Name"],
                    "CodeModel": model["Code"],
                    "IsDocumentModel": model["IsDocumentModel"],
                    "Stereotype": None,
                }
        return dict_result

    def _create_filters_lookup(self, filters: list[dict]) -> dict:
        """Maakt een dictionary van filterobjecten uit de opgegeven lijst van filters.

        Deze functie zet elke filter om naar een dictionary met relevante eigenschappen, waarbij het interne ID als sleutel wordt gebruikt.

        Args:
            filters (list[dict]): Lijst van filter dictionaries.

        Returns:
            dict: Dictionary met filters, waarbij de sleutel het interne ID is.
        """
        keys = [
            "Id",
            "Name",
            "Code",
            "CodeModel",
            "Variables",
            "Stereotype",
            "SqlVariable",
            "SqlExpression",
        ]
        dict_result = {
            filter["Id"]: {key: filter[key] for key in keys if key in filter}
            for filter in filters
        }
        return dict_result

    def _create_scalars_lookup(self, scalars: list[dict]) -> dict:
        """Maakt een dictionary van scalarobjecten uit de opgegeven lijst van scalars.

        Deze functie zet elke scalar om naar een dictionary met relevante eigenschappen, waarbij het interne ID als sleutel wordt gebruikt.

        Args:
            scalars (list[dict]): Lijst van scalar dictionaries.

        Returns:
            dict: Dictionary met scalars, waarbij de sleutel het interne ID is.
        """
        keys = [
            "Id",
            "Name",
            "Code",
            "CodeModel",
            "Variables",
            "Stereotype",
            "SqlVariable",
            "SqlExpression",
            "SqlExpressionVariables",
        ]
        dict_result = {
            scalar["Id"]: {key: scalar[key] for key in keys if key in scalar}
            for scalar in scalars
        }
        return dict_result

    def _create_aggregates_lookup(self, aggregates: list[dict]) -> dict:
        """Maakt een dictionary van aggregaatobjecten uit de opgegeven lijst van aggregaten.

        Deze functie zet elke aggregaat om naar een dictionary met relevante eigenschappen, waarbij het interne ID als sleutel wordt gebruikt.

        Args:
            aggregates (list[dict]): Lijst van aggregaat dictionaries.

        Returns:
            dict: Dictionary met aggregaten, waarbij de sleutel het interne ID is.
        """
        keys = [
            "Id",
            "Name",
            "Code",
            "CodeModel",
            "Attributes",
            "Stereotype",
        ]
        dict_result = {
            aggregate["Id"]: {key: aggregate[key] for key in keys if key in aggregate}
            for aggregate in aggregates
        }
        return dict_result

    def _create_attributes_lookup(self, models: list[dict]) -> dict:
        """Maakt een dictionary van attributen uit de opgegeven modellen.

        Deze functie doorloopt alle modellen en entiteiten en voegt de attributen van elke entiteit toe aan de dictionary.

        Args:
            models (list[dict]): Lijst van model dictionaries.

        Returns:
            dict: Dictionary met attributen, waarbij de sleutel het interne ID is.
        """
        dict_result = {}
        for model in models:
            entities = model["Entities"]
            for entity in entities:
                if "Attributes" in entity:
                    attributes = entity["Attributes"]
                    for attr in attributes:
                        dict_result[attr["Id"]] = {
                            "Id": attr["Id"],
                            "Name": attr["Name"],
                            "Code": attr["Code"],
                            "IdModel": model["Id"],
                            "NameModel": model["Name"],
                            "CodeModel": model["Code"],
                            "IsDocumentModel": model["IsDocumentModel"],
                            "IdEntity": entity["Id"],
                            "NameEntity": entity["Name"],
                            "CodeEntity": entity["Code"],
                            "StereotypeEntity": None,
                        }
        return dict_result

    def _create_variables_lookup(
        self, filters: list[dict], scalars: list[dict]
    ) -> dict:
        """Maakt een dictionary van variabelen uit de opgegeven filters en scalars.

        Deze functie combineert alle variabelen uit filters en scalars tot een enkele dictionary, waarbij het interne ID als sleutel wordt gebruikt.

        Args:
            filters (list[dict]): Lijst van filter dictionaries.
            scalars (list[dict]): Lijst van scalar dictionaries.

        Returns:
            dict: Dictionary met variabelen, waarbij de sleutel het interne ID is.
        """
        dict_result = {}
        stereotypes = filters + scalars
        for stereotype in stereotypes:
            for var in stereotype["Variables"]:
                dict_result[var["Id"]] = {
                    "Id": var["Id"],
                    "Name": var["Name"],
                    "Code": var["Code"],
                    "CodeModel": stereotype["Code"],
                    "IdEntity": stereotype["Id"],
                    "NameEntity": stereotype["Name"],
                    "CodeEntity": stereotype["Code"],
                    "StereotypeEntity": stereotype["Stereotype"],
                }
        return dict_result

    def _create_datasources_lookup(self, models: list[dict]) -> dict:
        dict_result = {}
        for model in models:
            if "DataSources" in model:
                dict_result = model["DataSources"]
        return dict_result

    def _get_relevant_mappings(self) -> list[dict]:
        """Selecteert en filtert relevante mappings uit het Power Designer LDM-bestand.

        Deze functie haalt alle mappings op, verwijdert te negeren mappings en retourneert de relevante mappings.

        Returns:
            list[dict]: Een lijst van relevante mappings.
        """
        if "c:Packages" in self.content:
            key_path = [
                "c:Packages",
                "o:Package",
                "c:Mappings",
                "o:DefaultObjectMapping",
            ]
        else:
            key_path = ["c:Mappings", "o:DefaultObjectMapping"]
        mappings = self._get_nested(data=self.content, keys=key_path)

        ignore_mappings = [
            "Mapping Br Custom Business Rule Example",
            "Mapping AggrTotalSalesPerCustomer",
            "Mapping Pivot Orders Per Country Per Date",
        ]
        if not mappings:
            logger.warning(f"Geen mappings gevonden in '{self.file_pd_ldm}'")
        elif isinstance(mappings, list):
            mappings = [m for m in mappings if m["a:Name"] not in ignore_mappings]
        else:
            mappings = [mappings] if mappings["a:Name"] not in ignore_mappings else []
        return mappings
