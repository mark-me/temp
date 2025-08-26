from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class ModelsExternalTransformer(BaseTransformer):
    """Functions die gebruikt worden voor het transformeren van model data die gebruikt gaat worden voor de mapping van het model"""

    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def transform(self, models: list[dict], dict_entities: dict) -> list[dict]:
        """Doelmodellen bevatten verwijzingen naar entiteiten uit een ander model. Het doelmodel wordt verrijkt met deze entiteiten

        Args:
            models (list[dict]): Data van de doelmodellen
            dict_entities (dict): Bevat alle externe entiteiten

        Returns:
            list[dict]: Doelmodellen met entiteit data
        """
        results = []
        models = self.clean_keys(models)

        for model in models:
            self._enrich_model_with_entities(model, dict_entities)
            if "Entities" in model and len(model["Entities"]) > 0:
                model["IsDocumentModel"] = False
                results.append(model)
                self._cleanup_model(model)
        return results

    def _enrich_model_with_entities(self, model: dict, dict_entities: dict) -> None:
        """
        Verrijkt het model met entiteiten op basis van shortcuts.

        Deze functie zoekt naar shortcuts in het model en koppelt de bijbehorende entiteiten uit dict_entities aan het model.

        Args:
            model (dict): Het model dat verrijkt moet worden.
            dict_entities (dict): Dictionary met externe entiteiten.
        """
        path_keys = ["c:SessionShortcuts", "o:Shortcut"]
        if shortcuts := self._get_nested(data=model, keys=path_keys):
            if isinstance(shortcuts, dict):
                shortcuts = [shortcuts]
            shortcuts = [i["@Ref"] for i in shortcuts]
            model["Entities"] = [
                dict_entities[id] for id in shortcuts if id in dict_entities
            ]

    def _cleanup_model(self, model: dict) -> None:
        """Verwijdert overbodige keys uit het model.

        Deze functie verwijdert specifieke keys die niet langer nodig zijn na het verrijken van het model met entiteiten.

        Args:
            model (dict): Het model waarvan overbodige keys verwijderd worden.
        """
        model.pop("c:SessionShortcuts")
        if "c:SessionReplications" in model:
            model.pop("c:SessionReplications")
        if "c:FullShortcutModel" in model:
            model.pop("c:FullShortcutModel")

    def transform_entities(self, entities: list[dict] | dict) -> list[dict]:
        """
        Transformeert en schoont externe entiteiten uit het Power Designer model.

        Deze functie verwerkt een lijst van externe entiteiten, verwijdert overbodige attributen,
        transformeert de attribuutdata en retourneert een geschoonde lijst van entiteiten.

        Args:
            entities (list[dict] | dict): Externe entiteiten of een enkele entiteit.

        Returns:
            list[dict]: Geschoonde en getransformeerde entiteiten.
        """
        entities = (
            [entities] if isinstance(entities, dict) else entities
        )
        entities = self.clean_keys(entities)
        for entity in entities:
            if "c:FullShortcutReplica" in entity:
                entity.pop("c:FullShortcutReplica")
            self._handle_entity_attributes(entity)
            entity.pop("c:SubShortcuts")
        return entities

    def _handle_entity_attributes(self, entity: dict) -> dict:
        """Vormt om en schoont attributen van een entiteit

        Args:
            entity (dict): Power Designer content dat een entiteit vertegenwoordigd

        Returns:
            dict: Entiteit data met omgevormde en geschoonde attribuut data
        """
        attributes = entity["c:SubShortcuts"]["o:Shortcut"]
        attributes = [attributes] if isinstance(attributes, dict) else attributes
        for i, attr in enumerate(attributes):
            if "c:FullShortcutReplica" in attr:
                attr.pop("c:FullShortcutReplica")
            attr["Order"] = i
        attributes = self.clean_keys(attributes)
        entity["Attributes"] = attributes
        return entity
