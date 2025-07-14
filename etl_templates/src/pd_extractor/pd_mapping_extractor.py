
from logtools import get_logger

from .pd_transform_attribute_mapping import TransformAttributeMapping
from .pd_transform_source_composition import TransformSourceComposition
from .pd_transform_target_entity import TransformTargetEntity

logger = get_logger(__name__)


class MappingExtractor:
    """Extraheert ETL specificaties (mappings) vanuit een Power Designer LDM waarin mappings zijn geïmplementeerd met behulp van de
    CrossBreeze MDDE extensie.
    Transformeert de data in een meer leesbaar format door overbodige informatie te verwijderen en relevante model informatie
    toe te voegen.
    """

    def __init__(self, pd_content: dict):
        """Initialiseren voor het extraheren van de mapping informatie

        Args:
            pd_content (dict): Power Designer LDM bestand inhoud (gerepresenteerd als een dictionary)
        """
        self.content = pd_content
        self.transform_attribute_mapping = TransformAttributeMapping()
        self.transform_source_composition = TransformSourceComposition()
        self.transform_target_entity = TransformTargetEntity()

    def mappings(
        self, dict_objects: list, dict_attributes: list, dict_variables: list, dict_datasources: list
    ) -> list:
        """Extraheert alle ETL specificaties die het document model vullen

        Args:
            dict_objects (list): Een combinatie van Entiteiten, Filters, Scalars en Aggregaten
            dict_attributes (list): Alle attributen
            dict_variables (list): Alle variabelen

        Returns:
            list: Mapping objecten
        """
        if "c:Packages" in self.content:
            lst_mappings = self.content["c:Packages"]["o:Package"]["c:Mappings"][
                "o:DefaultObjectMapping"
            ]
        else:
            lst_mappings = self.content["c:Mappings"]["o:DefaultObjectMapping"]

        # Mappings to be ignored
        lst_ignored_mapping = [
            "Mapping Br Custom Business Rule Example",
            "Mapping AggrTotalSalesPerCustomer",
            "Mapping Pivot Orders Per Country Per Date",
        ]  # Ignored mappings for 1st version with CrossBreeze example.
        if isinstance(lst_mappings, list):
            lst_mappings = [
                m for m in lst_mappings if m["a:Name"] not in lst_ignored_mapping
            ]
        else:
            if lst_mappings["a:Name"] not in lst_ignored_mapping:
                lst_mappings = [lst_mappings]
        lst_mappings_def = []
        for i in range(len(lst_mappings)):
            mapping = lst_mappings[i]
            logger.debug(f"Mapping starting for '{mapping['a:Name']}")
            if ' ' in mapping['a:Name']:
                logger.warning(f"Er staan spatie(s) in de mapping naam staan voor '{mapping['a:Name']}'.")
            # Select all Target entities with their identifier
            lst_entity_target = self.transform_target_entity.target_entities(
                mapping=mapping,
                dict_objects=dict_objects,
            )
            # Get all attribute mappings (source/target)
            dict_attributes = dict_attributes | dict_variables
            lst_attribute_mapping = self.transform_attribute_mapping.attribute_mapping(
                dict_entity_target=lst_entity_target, dict_attributes=dict_attributes
            )
            lst_source_composition = (
                self.transform_source_composition.source_composition(
                    lst_attribute_mapping=lst_attribute_mapping,
                    dict_attributes=dict_attributes,
                    dict_objects=dict_objects,
                    dict_datasources=dict_datasources
                )
            )
            lst_mappings_def.append(lst_source_composition)

        lst_mappings = []
        lst_mappings = lst_mappings_def
        return lst_mappings
