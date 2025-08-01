
from logtools import get_logger

from .pd_transform_object import ObjectTransformer

logger = get_logger(__name__)

class TransformAttributeMapping(ObjectTransformer):
    """Collectie van functies om attribuut mappings conform het afgestemde JSON format op te bouwen om ETL generatie te faciliteren
    """
    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm

    def attribute_mapping(self, dict_entity_target: dict, dict_attributes:dict) -> dict:
        """Verrijkt, schoont en hangt attribuut mappings om ten behoeven van een mapping

        Args:
            dict_entity_target (dict): Entity gevoed door de mapping
            dict_attributes (dict): Alle attributen van het Power Designer LDM die beschikbaar zijn als bron voor de attribuut mapping

        Returns:
            dict: Mapping met geschoonde en verrijkte attribuut mapping
        """
        lst_attribute_mapping = dict_entity_target
        mapping = lst_attribute_mapping

        if "c:StructuralFeatureMaps" in mapping:
            lst_attr_maps = mapping["c:StructuralFeatureMaps"]["o:DefaultStructuralFeatureMapping"]
            if isinstance(lst_attr_maps, dict):
                lst_attr_maps = [lst_attr_maps].copy()
            lst_attr_maps = self.clean_keys(lst_attr_maps)
            for j in range(len(lst_attr_maps)):
                attr_map = lst_attr_maps[j].copy()
                attr_map["Order"] = j
                self._process_attribute_map(attr_map=attr_map, dict_attributes=dict_attributes)
                lst_attr_maps[j] = attr_map.copy()
            mapping["AttributeMapping"] = lst_attr_maps
            mapping.pop("c:StructuralFeatureMaps")
            lst_attribute_mapping = mapping
        else:
            logger.error(f"attributemapping voor {mapping['Name']} van {self.file_pd_ldm} niet gevonden")
        return lst_attribute_mapping

    def _process_attribute_map(self, attr_map: dict, dict_attributes: dict):
        """Verwerkt een enkele attribuut mapping en verrijkt deze met de juiste informatie.

        Args:
            attr_map (dict): De attribuut mapping die verwerkt wordt.
            index (int): De volgorde van de mapping in de lijst.
            dict_attributes (dict): Alle attributen van het Power Designer LDM.
        """
        logger.debug(f"Start attributemapping voor  {attr_map['Id']} van {self.file_pd_ldm} ")

        id_attr = attr_map["c:BaseStructuralFeatureMapping.Feature"]["o:EntityAttribute"]["@Ref"]
        if id_attr in dict_attributes:
            attr_map["AttributeTarget"] = dict_attributes[id_attr].copy()
            attr_map.pop("c:BaseStructuralFeatureMapping.Feature")

            self._process_source_features(attr_map=attr_map, dict_attributes=dict_attributes)
        else:
            logger.warning(f"{id_attr} van {self.file_pd_ldm} is niet gevonden binnen target attributen")

    def _extract_entity_alias(self, attr_map: dict):
        """Extraheert de entity alias uit de attribuut mapping indien aanwezig.

        Args:
            attr_map (dict): De attribuut mapping.

        Returns:
            tuple: (has_entity_alias (bool), id_entity_alias (str of None))
        """
        has_entity_alias = False
        id_entity_alias = None
        if "c:ExtendedCollections" in attr_map:
            has_entity_alias = True
            id_entity_alias = attr_map["c:ExtendedCollections"]["o:ExtendedCollection"]["c:Content"]["o:ExtendedSubObject"]["@Ref"]
            logger.info("Ongebruikt object; file:pd_transform_attribute_mapping; object:id_entity_alias")
            logger.info(f"Object bevat volgende data: '{id_entity_alias}'")
            attr_map.pop("c:ExtendedCollections")
        return has_entity_alias, id_entity_alias

    def _process_source_features(self, attr_map: dict, dict_attributes: dict):
        """Verwerkt de source features van een attribuut mapping.

        Args:
            attr_map (dict): De attribuut mapping.
            dict_attributes (dict): Alle attributen van het Power Designer LDM.
        """
        has_entity_alias, id_entity_alias = self._extract_entity_alias(attr_map=attr_map)
        if "c:SourceFeatures" in attr_map:
            type_entity = [
                value
                for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                if value in attr_map["c:SourceFeatures"]
            ][0]
            id_attr = attr_map["c:SourceFeatures"][type_entity]["@Ref"]
            if id_attr in dict_attributes:
                if dict_attributes[id_attr]["StereotypeEntity"] is None:
                    attr_map["AttributesSource"] = dict_attributes[id_attr].copy()
                    if has_entity_alias:
                        attr_map["AttributesSource"]["EntityAlias"] = id_entity_alias
                if "ExtendedAttributesText" in attr_map:
                    attr_map["Expression"] = self.extract_value_from_attribute_text(
                        attr_map["ExtendedAttributesText"], preceded_by="mdde_Aggregate,"
                    )
                if dict_attributes[id_attr]["StereotypeEntity"] == "mdde_ScalarBusinessRule" and has_entity_alias:
                    attr_map["EntityAlias"] = id_entity_alias
            else:
                logger.warning(f"{id_attr} van {self.file_pd_ldm}is niet gevonden binnen bron attributen")
            attr_map.pop("c:SourceFeatures")
        else:
            logger.warning(f"Source attributes van {self.file_pd_ldm} niet gevonden")

