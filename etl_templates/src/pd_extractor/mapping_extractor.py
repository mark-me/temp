from logtools import get_logger

from .extractor_base import ExtractorBase
from .attribute_mapping_transform import TransformAttributeMapping
from .composition_transform import TransformSourceComposition
from .pd_transform_target_entity import TransformTargetEntity

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

    def mappings(
        self,
        dict_objects: dict,
        dict_attributes: dict,
        dict_variables: dict,
        dict_datasources: dict,
    ) -> list:
        """
        Extraheert en transformeert mappings uit een Power Designer LDM-bestand.

        Deze functie selecteert alle relevante mappings, verwijdert overbodige of te negeren mappings,
        transformeert de mappingnamen, selecteert target entities, en verrijkt de mappings met attribute mappings en source composition.

        Args:
            dict_objects (dict): Objecten uit het Power Designer model.
            dict_attributes (dict): Attributen uit het Power Designer model.
            dict_variables (dict): Variabelen uit het Power Designer model.
            dict_datasources (dict): Datasources uit het Power Designer model.

        Returns:
            list: Een lijst van getransformeerde mappings.
        """
        lst_mappings = self._get_relevant_mappings()
        lst_mappings_def = []
        for mapping in lst_mappings:
            mapping = self._normalize_mapping_name(mapping)
            lst_entity_target = self.transform_target_entity.target_entities(
                mapping=mapping,
                dict_objects=dict_objects,
            )
            dict_attributes_combined = dict_attributes | dict_variables
            lst_attribute_mapping = self.transform_attribute_mapping.attribute_mapping(
                dict_entity_target=lst_entity_target,
                dict_attributes=dict_attributes_combined,
            )
            lst_source_composition = (
                self.transform_source_composition.source_composition(
                    lst_attribute_mapping=lst_attribute_mapping,
                    dict_attributes=dict_attributes_combined,
                    dict_objects=dict_objects,
                    dict_datasources=dict_datasources,
                )
            )
            lst_mappings_def.append(lst_source_composition)

        return lst_mappings_def

    def _get_relevant_mappings(self) -> list[dict]:
        """Selecteert en filtert relevante mappings uit het Power Designer LDM-bestand.

        Deze functie haalt alle mappings op, verwijdert te negeren mappings en retourneert de relevante mappings.

        Returns:
            list[dict]: Een lijst van relevante mappings.
        """
        if "c:Packages" in self.content:
            key_path = ["c:Packages", "o:Package", "c:Mappings", "o:DefaultObjectMapping"]
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
