
from logtools import get_logger

from .pd_transform_object import ObjectTransformer

logger = get_logger(__name__)

class TransformTargetEntity(ObjectTransformer):
    """Vormt mapping data om en verrijkt dit met entiteit en attribuut data
    """
    def __init__(self):
        super().__init__()

    def target_entities(self, mapping: dict, dict_objects: dict) -> dict:
        """Omvormen van mapping data en verrijkt dit met doelentiteit en attribuut data

        Args:
            mapping (dict): Een mapping uit een Power Designer document
            dict_objects (dict): Alle objecten(entities/filters/scalars/aggregaten) in het document (internal en external)

        Returns:
            list: een mapping met geschoonde doelentiteit data.
        """
        mapping = self.clean_keys(mapping)
        logger.debug(
            f"Starting target_entity for '{mapping['Name']}'"
        )
        # Target entity rerouting and enriching
        if "o:Entity" in mapping["c:Classifier"]:
            id_entity_target = mapping["c:Classifier"]["o:Entity"]["@Ref"]
            mapping["EntityTarget"] = dict_objects[id_entity_target]
            logger.debug(
                f"Mapping target entity: '{mapping['EntityTarget']['Name']}'"
            )
            mapping = self._remove_source_entities(
                mapping = mapping, dict_objects=dict_objects
            )
        else:
            logger.warning(f"Mapping without entity found: '{mapping['Name']}'")
        mapping.pop("c:Classifier")
        mapping.pop("SourceObjects_REMOVE")
        return mapping

    def _remove_source_entities(self, mapping: dict, dict_objects: dict) -> dict:
        """Verwijderd de bron entiteiten die onderdeel uitmaken van een mapping

        Args:
            mapping (dict): Het deel van het Power Designer document die de mapping beschrijft
            dict_objects (dict): Alle objecten(entities/filters/scalars/aggregaten) in het document (internal en external)

        Returns:
            dict: Versie van de mapping data waar bron entiteit data is verwijderd
        """
        logger.debug(
            f"Starting sources entities transform for mapping '{mapping['Name']}'"
        )
        lst_source_entity = []
        for entity_type in ["o:Entity", "o:Shortcut"]:
            if entity_type in mapping["c:SourceClassifiers"]:
                source_entity = mapping["c:SourceClassifiers"][entity_type]
                if isinstance(source_entity, dict):
                    source_entity = [source_entity]
                source_entity = [d["@Ref"] for d in source_entity]
                lst_source_entity = lst_source_entity + source_entity
        # onderstaande regel geeft problemen als  item in lst_source_entity niet in dict_object aanwezig is
        try:
            lst_source_entity = [dict_objects[item] for item in lst_source_entity]
        except KeyError as e:
            logger.error(f"Entiteit niet gevonden voor externe model met referentie '{e.args[0]}', mogelijk omdat het externe model gegenereerd is door Object in plaats van Shortcut")
            return
        mapping["SourceObjects_REMOVE"] = lst_source_entity
        mapping.pop("c:SourceClassifiers")
        return mapping