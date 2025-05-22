import logging
from .pd_transform_object import ObjectTransformer

logger = logging.getLogger(__name__)

class TransformAttributeMapping(ObjectTransformer):
    """Collectie van functies om attribuut mappings conform het afgestemde JSON format op te bouwen om ETL generatie te faciliteren
    """
    def __init__(self):
        super().__init__()

    def attribute_mapping(self, dict_entity_target: dict, dict_attributes:dict, lst_mappings_full:dict) -> list:
        """Verrijkt, schoont en hangt attribuut mappings om ten behoeven van een mapping

        Args:
            dict_entity_target (dict): Entity gevoed door de mapping
            dict_attributes (dict): Alle attributen van het Power Designer LDM die beschikbaar zijn als bron voor de attribuut mapping

        Returns:
            list: Mapping met geschoonde en verrijkte attribuut mapping
        """
        lst_attribute_mapping = dict_entity_target 
        dict_attributes = dict_attributes
        mapping = lst_attribute_mapping
        
        if "c:StructuralFeatureMaps" in mapping:
            lst_attr_maps = {}
            attr_map = {}
            lst_attr_maps = mapping["c:StructuralFeatureMaps"][
                "o:DefaultStructuralFeatureMapping"
            ]
            if isinstance(lst_attr_maps, dict):
                logging.warning("List object is actually dictionary; file:pd_transform_attribute_mapping; object:lst_attr_maps")
                lst_attr_maps = [lst_attr_maps].copy()
            lst_attr_maps = self.clean_keys(lst_attr_maps)
            for j in range(len(lst_attr_maps)):
                logger.debug(f"Starting attributemapping for {lst_attr_maps[j]['Id']}")
                attr_map = lst_attr_maps[j].copy()
                # Ordering
                attr_map["Order"] = j
                # Target feature
                id_attr = attr_map["c:BaseStructuralFeatureMapping.Feature"]["o:EntityAttribute"]["@Ref"]
                if id_attr in dict_attributes:
                    attr_map["AttributeTarget"] = dict_attributes[id_attr].copy()
                    attr_map.pop("c:BaseStructuralFeatureMapping.Feature")
                # Source feature's entity alias
                    has_entity_alias = False
                    if "c:ExtendedCollections" in attr_map:
                        has_entity_alias = True
                        id_entity_alias = attr_map["c:ExtendedCollections"]["o:ExtendedCollection"]["c:Content"]["o:ExtendedSubObject"]["@Ref"]
                        id_entity_alias = id_entity_alias
                        logger.info("unused object; file:pd_transform_attribute_mapping; object:id_entity_alias")
                        logger.info(f"object has following data: '{id_entity_alias}'")
                        attr_map.pop("c:ExtendedCollections")
                # Source attribute
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
                                attr_map["Expression"] =  self.extract_value_from_attribute_text(
                                attr_map["ExtendedAttributesText"],preceded_by="mdde_Aggregate,")
                            if dict_attributes[id_attr]["StereotypeEntity"] == "mdde_ScalarBusinessRule":
                                if has_entity_alias:
                                    attr_map["EntityAlias"] = id_entity_alias
                                
                        else:
                            logger.warning(f"{id_attr} is niet gevonden binnen bron attributen")
                        attr_map.pop("c:SourceFeatures")
                    else:
                        logger.warning("Source attributes niet gevonden")
                else:
                    logger.warning(f"{id_attr} is niet gevonden binnen target attributen")
                lst_attr_maps[j] = attr_map.copy()
            mapping["AttributeMapping"] = lst_attr_maps
            mapping.pop("c:StructuralFeatureMaps")
            lst_attribute_mapping = mapping
        else:
            logger.error(f"attributemapping voor {mapping['Name']} niet gevonden")
        return lst_attribute_mapping
    
