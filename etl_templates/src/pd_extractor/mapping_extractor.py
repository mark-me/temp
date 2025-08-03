from logtools import get_logger

from .extractor_base import ExtractorBase
from .mapping_attributes_transform import TransformAttributeMapping
from .mapping_composition_transform import TransformSourceComposition
from .mapping_target_transform import TransformTargetEntity

logger = get_logger(__name__)


class MappingExtractor(ExtractorBase):
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
        self.transform_attribute_mapping = TransformAttributeMapping(file_pd_ldm)
        self.transform_source_composition = TransformSourceComposition(file_pd_ldm)
        self.transform_target_entity = TransformTargetEntity(file_pd_ldm)

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
        dict_entities = self._create_entities_dict(models=models)
        dict_filters = self._create_filters_dict(filters=filters)
        dict_scalars = self._create_scalars_dict(scalars=scalars)
        dict_aggregates = self._create_aggregates_dict(aggregates=aggregates)
        dict_objects = dict_entities | dict_filters | dict_scalars | dict_aggregates
        dict_variables = self._create_variables_dict(filters=filters, scalars=scalars)
        dict_attributes = self._create_attributes_dict(models=models)
        dict_datasources = self._create_datasources_dict(models=models)

        lst_mappings = self._get_relevant_mappings()
        lst_mappings_def = [
            self._process_single_mapping(
                mapping=mapping,
                dict_objects=dict_objects,
                dict_attributes=dict_attributes,
                dict_variables=dict_variables,
                dict_datasources=dict_datasources,
            )
            for mapping in lst_mappings
        ]
        return lst_mappings_def

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
        mapping = self._normalize_mapping_name(mapping)
        lst_entity_target = self.transform_target_entity.transform(
            mapping=mapping,
            dict_objects=dict_objects,
        )
        dict_attributes_combined = dict_attributes | dict_variables
        lst_attribute_mapping = self.transform_attribute_mapping.transform(
            dict_entity_target=lst_entity_target,
            dict_attributes=dict_attributes_combined,
        )
        lst_source_composition = self.transform_source_composition.transform(
            lst_attribute_mapping=lst_attribute_mapping,
            dict_attributes=dict_attributes_combined,
            dict_objects=dict_objects,
            dict_datasources=dict_datasources,
        )
        return lst_source_composition

    def _create_entities_dict(self, models: list[dict]) -> dict:
        """Maakt een dictionary van entiteiten zonder stereotype uit de opgegeven modellen.

        Deze functie doorloopt alle modellen en entiteiten en voegt entiteiten zonder stereotype toe aan de dictionary.

        Args:
            models (list[dict]): Lijst van model dictionaries.

        Returns:
            dict: Dictionary met entiteiten zonder stereotype, waarbij de sleutel het interne ID is.
        """

        dict_result = {}
        for model in models:
            entities = model["Entities"]
            for entity in entities:
                if "Stereotype" not in entity:
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

    def _create_filters_dict(self, filters: list[dict]) -> dict:
        """Maakt een dictionary van filterobjecten uit de opgegeven lijst van filters.

        Deze functie zet elke filter om naar een dictionary met relevante eigenschappen, waarbij het interne ID als sleutel wordt gebruikt.

        Args:
            filters (list[dict]): Lijst van filter dictionaries.

        Returns:
            dict: Dictionary met filters, waarbij de sleutel het interne ID is.
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
            for filter in filters
        }
        return dict_result

    def _create_scalars_dict(self, scalars: list[dict]) -> dict:
        """Maakt een dictionary van scalarobjecten uit de opgegeven lijst van scalars.

        Deze functie zet elke scalar om naar een dictionary met relevante eigenschappen, waarbij het interne ID als sleutel wordt gebruikt.

        Args:
            scalars (list[dict]): Lijst van scalar dictionaries.

        Returns:
            dict: Dictionary met scalars, waarbij de sleutel het interne ID is.
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
            for scalar in scalars
        }
        return dict_result

    def _create_aggregates_dict(self, aggregates: list[dict]) -> dict:
        """Maakt een dictionary van aggregaatobjecten uit de opgegeven lijst van aggregaten.

        Deze functie zet elke aggregaat om naar een dictionary met relevante eigenschappen, waarbij het interne ID als sleutel wordt gebruikt.

        Args:
            aggregates (list[dict]): Lijst van aggregaat dictionaries.

        Returns:
            dict: Dictionary met aggregaten, waarbij de sleutel het interne ID is.
        """
        dict_result = {
            aggregate["Id"]: {
                "Id": aggregate["Id"],
                "Name": aggregate["Name"],
                "Code": aggregate["Code"],
                "CodeModel": aggregate["CodeModel"],
                "Variables": aggregate["Attributes"],
                "Stereotype": aggregate["Stereotype"],
            }
            for aggregate in aggregates
        }
        return dict_result

    def _create_attributes_dict(self, models: list[dict]) -> dict:
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

    def _create_variables_dict(self, filters: list[dict], scalars: list[dict]) -> dict:
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
            variables = stereotype["Variables"]
            for var in variables:
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

    def _create_datasources_dict(self, models: list[dict]) -> dict:
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
        lst_mappings = self._get_nested(data=self.content, keys=key_path)

        lst_ignored_mapping = [
            "Mapping Br Custom Business Rule Example",
            "Mapping AggrTotalSalesPerCustomer",
            "Mapping Pivot Orders Per Country Per Date",
        ]
        if not lst_mappings:
            logger.warning(f"Geen mappings gevonden in '{self.file_pd_ldm}'")
        elif isinstance(lst_mappings, list):
            lst_mappings = [
                m for m in lst_mappings if m["a:Name"] not in lst_ignored_mapping
            ]
        else:
            if lst_mappings["a:Name"] not in lst_ignored_mapping:
                lst_mappings = [lst_mappings]
            else:
                lst_mappings = []
        return lst_mappings

    def _normalize_mapping_name(self, mapping: dict) -> dict:
        """Vervangt spaties in de mappingnaam door underscores en logt een waarschuwing indien nodig."""
        if " " in mapping["a:Name"]:
            logger.warning(
                f"Er staan spatie(s) in de mapping naam staan voor '{mapping['a:Name']}' uit {self.file_pd_ldm}."
            )
            mapping["a:Name"] = mapping["a:Name"].replace(" ", "_")
        logger.debug(f"Start mapping voor '{mapping['a:Name']} uit {self.file_pd_ldm}")
        return mapping
