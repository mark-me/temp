from log_config import logging
from .pd_transform_object import ObjectTransformer

logger = logging.getLogger(__name__)


class TransformModelsExternal(ObjectTransformer):
    """Functions die gebruikt worden voor het transformeren van model data die gebruikt gaat worden voor de mapping van het model
    """
    def __init__(self):
        super().__init__()

    def models(self, lst_models: list, dict_entities: dict) -> list:
        """Doelmodellen bevatten verwijzingen naar entiteiten uit een ander model. Het doelmodel wordt verrijkt met deze entiteiten

        Args:
            lst_models (list): Data van de doelmodellen
            dict_entities (dict): Bevat alle externe entiteiten 

        Returns:
            list: Doelmodellen met entiteit data
        """
        lst_result = []
        lst_models = self.clean_keys(lst_models)

        for model in lst_models:
            if "c:SessionShortcuts" in model:
                shortcuts = model["c:SessionShortcuts"]["o:Shortcut"]
                if isinstance(shortcuts, dict):
                    shortcuts = [shortcuts]
                shortcuts = [i["@Ref"] for i in shortcuts]
                model["Entities"] = [
                    dict_entities[id] for id in shortcuts if id in dict_entities
                ]
            if "Entities" in model:
                if len(model["Entities"]) > 0:
                    model["IsDocumentModel"] = False
                    lst_result.append(model)
                    model.pop("c:SessionShortcuts")
                    if "c:SessionReplications" in model:
                        model.pop("c:SessionReplications")
                    if "c:FullShortcutModel" in model:
                        model.pop("c:FullShortcutModel")
        return lst_result

    def entities(self, lst_entities: list) -> list:
        """Vormt om en schoont de Power Designer document data van externe entiteiten

        Args:
            lst_entities (list): Het deel van het Power Designer document dat de externe entiteiten beschrijft

        Returns:
            list: De geschoonde versie van de externe entiteit data
        """
        if isinstance(lst_entities, dict):
            lst_entities = [lst_entities]
        lst_entities = self.clean_keys(lst_entities)
        for i in range(len(lst_entities)):
            entity = lst_entities[i]
            if "c:FullShortcutReplica" in entity:
                entity.pop("c:FullShortcutReplica")
            self.__entity_attribute(entity)
            entity.pop("c:SubShortcuts")
            lst_entities[i] = entity
        return lst_entities

    def __entity_attribute(self, entity: dict) -> dict:
        """Vormt om en schoont attributen van een entiteit

        Args:
            entity (dict): Power Designer content dat een entiteit vertegenwoordigd

        Returns:
            dict: Entiteit data met omgevormde en geschoonde attribuut data
        """
        lst_attributes = entity["c:SubShortcuts"]["o:Shortcut"]
        if isinstance(lst_attributes, dict):
            lst_attributes = [lst_attributes]
        for i in range(len(lst_attributes)):
            attr = lst_attributes[i]
            if "c:FullShortcutReplica" in attr:
                attr.pop("c:FullShortcutReplica")
            attr["Order"] = i
            lst_attributes[i] = attr
        lst_attributes = self.clean_keys(lst_attributes)
        entity["Attributes"] = lst_attributes
        return entity
