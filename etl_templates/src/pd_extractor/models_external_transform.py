from logtools import get_logger

from .transformer_base import TransformerBase

logger = get_logger(__name__)


class TransformModelsExternal(TransformerBase):
    """Functions die gebruikt worden voor het transformeren van model data die gebruikt gaat worden voor de mapping van het model
    """
    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def models(self, lst_models: list[dict], dict_entities: dict) -> list[dict]:
        """Doelmodellen bevatten verwijzingen naar entiteiten uit een ander model. Het doelmodel wordt verrijkt met deze entiteiten

        Args:
            lst_models (list[dict]): Data van de doelmodellen
            dict_entities (dict): Bevat alle externe entiteiten

        Returns:
            list[dict]: Doelmodellen met entiteit data
        """
        lst_result = []
        lst_models = self.clean_keys(lst_models)

        for model in lst_models:
            path_keys = ["c:SessionShortcuts", "o:Shortcut"]
            if shortcuts := self._get_nested(data=model, keys=path_keys):
                if isinstance(shortcuts, dict):
                    shortcuts = [shortcuts]
                shortcuts = [i["@Ref"] for i in shortcuts]
                model["Entities"] = [
                    dict_entities[id] for id in shortcuts if id in dict_entities
                ]
            if "Entities" in model and len(model["Entities"]) > 0:
                model["IsDocumentModel"] = False
                lst_result.append(model)
                model.pop("c:SessionShortcuts")
                if "c:SessionReplications" in model:
                    model.pop("c:SessionReplications")
                if "c:FullShortcutModel" in model:
                    model.pop("c:FullShortcutModel")
        return lst_result

    def entities(self, lst_entities: list[dict] | dict) -> list[dict]:
        """
        Transformeert en schoont externe entiteiten uit het Power Designer model.

        Deze functie verwerkt een lijst van externe entiteiten, verwijdert overbodige attributen,
        transformeert de attribuutdata en retourneert een geschoonde lijst van entiteiten.

        Args:
            lst_entities (list[dict] | dict): Externe entiteiten of een enkele entiteit.

        Returns:
            list[dict]: Geschoonde en getransformeerde entiteiten.
        """
        if isinstance(lst_entities, dict):
            lst_entities = [lst_entities]
        lst_entities = self.clean_keys(lst_entities)
        for i in range(len(lst_entities)):
            entity = lst_entities[i]
            if "c:FullShortcutReplica" in entity:
                entity.pop("c:FullShortcutReplica")
            self._entity_attribute(entity)
            entity.pop("c:SubShortcuts")
            lst_entities[i] = entity
        return lst_entities

    def _entity_attribute(self, entity: list[dict] | dict) -> dict:
        """Vormt om en schoont attributen van een entiteit

        Args:
            entity (list[dict] | dict): Power Designer content dat een entiteit vertegenwoordigd

        Returns:
            dict: Entiteit data met omgevormde en geschoonde attribuut data
        """
        lst_attributes = entity["c:SubShortcuts"]["o:Shortcut"]
        if isinstance(lst_attributes, dict):
            lst_attributes = [lst_attributes]
        for i, attr in enumerate(lst_attributes):
            if "c:FullShortcutReplica" in attr:
                attr.pop("c:FullShortcutReplica")
            attr["Order"] = i
        lst_attributes = self.clean_keys(lst_attributes)
        entity["Attributes"] = lst_attributes
        return entity
