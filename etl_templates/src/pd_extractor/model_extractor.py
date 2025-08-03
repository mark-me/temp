from logtools import get_logger

from .extractor_base import ExtractorBase
from .model_internal_transform import TransformModelInternal
from .models_external_transform import TransformModelsExternal
from .model_relationships_transform import TransformRelationships

logger = get_logger(__name__)


class ModelExtractor(ExtractorBase):
    """Collectie van functies die gebruikt worden om de relevante objecten uit een Power Designer LDM te extraheren"""

    def __init__(self, pd_content: dict, file_pd_ldm: str):
        super().__init__(file_pd_ldm=file_pd_ldm)
        self.content = pd_content
        self.transform_model_internal = TransformModelInternal(file_pd_ldm)
        self.transform_models_external = TransformModelsExternal(file_pd_ldm)
        self.dict_domains = self._domains()

    def get_models(self) -> list[dict]:
        """Haalt alle modellen en hun bijbehorende objecten op die gebruikt worden in het Power Designer LDM

        Args:
            lst_aggregates (list): Aggregaten die onderdeel zijn van het doelmodel en gebruikt worden in de ETL

        Returns:
            list[dict]: lijst van modellen die gebruikt worden in het Power Designer LDM document
        """
        dict_model_internal = self._model_internal()
        # TODO: need to add the condition for c:Packages if we encounter models that use packages
        if "o:Shortcut" in self.content.get("c:Entities", None):
            lst_models_external = self._models_external()
        else:
            lst_models_external = []
            logger.warning(f"o:Shortcut mist in self.content in {self.file_pd_ldm}")
        # Combine models
        if not lst_models_external:
            return [dict_model_internal]
        else:
            return lst_models_external + [dict_model_internal]

    def _model_internal(self) -> dict:
        """Haalt alle vastgelegde data van het model op vanuit het geladen Power Designer document

        Args:
            lst_aggregates (list[dict]): Aggregaten die onderdeel zijn van het doelmodel en gebruikt worden in de ETL

        Returns:
            dict: In het Power Designer LDM ontworpen model (niet geïmporteerd voor ETL)
        """
        model = self.transform_model_internal.transform(content=self.content)
        # Model add entity data
        self.lst_entity = self._entities_internal()
        model["Entities"] = self.lst_entity
        model["Relationships"] = self._relationships(lst_entity=self.lst_entity)
        model["DataSources"] = self._datasources()
        return model

    def _entities_internal(self) -> list[dict]:
        """Haalt alle interne entiteiten op uit het Power Designer model.

        Deze functie filtert entiteiten op basis van hun stereotype en transformeert ze met behulp van de interne transformer.

        Returns:
            list[dict]: Lijst van interne entiteiten uit het model.
        """
        lst_entity = self.content["c:Entities"]["o:Entity"]

        entity1 = []
        for i in range(len(lst_entity)):
            entity_in = lst_entity[i]
            if (
                "Stereotype" not in entity_in
                or entity_in["Stereotype"] == "mdde_AggregateBusinessRule"
            ):
                entity1.append(entity_in)
        lst_entity = entity1
        self.transform_model_internal.entities(
            lst_entity, dict_domains=self.dict_domains
        )
        return lst_entity

    def _models_external(self) -> list[dict] | None:
        """Haalt externe modellen op die zijn gekoppeld aan entity shortcuts in het Power Designer model.

        Deze functie zoekt naar target models die referenties bevatten naar externe entiteiten en
        retourneert een lijst van deze modellen, of None als er geen doelmodellen zijn gevonden.

        Returns:
            list[dict] | None: Lijst van externe modellen of None als er geen doelmodellen zijn gevonden.
        """
        # The models will be derived by looking up the TargetModels associated with the entity shortcuts
        # External entity (shortcut) data
        dict_entities = self._entities_external()
        # Retain 'TargetModels' have references to entities
        path_keys = ["c:TargetModels", "o:TargetModel"]
        if lst_target_model := self._get_nested(data=self.content, keys=path_keys):
            lst_models = self.transform_models_external.transform(
                lst_models=lst_target_model, dict_entities=dict_entities
            )
            return lst_models
        else:
            logger.warning(f"Geen doelmodellen gevonden in '{self.file_pd_ldm}'")
            return None

    def _entities_external(self) -> dict:
        """Haalt alle entities en bijbehorende informatie op van het externe model op

        Returns:
            dict: Een dict van Entiteiten, waar elke sleutel data als waarde bevat van een Entiteit en de bijbehorende attributen
        """
        # External model entity data
        dict_result = {}
        path_keys = (
            ["c:Packages", "o:Package", "c:Entities", "o:Shortcut"]
            if "c:Packages" in self.content
            else ["c:Entities", "o:Shortcut"]
        )
        lst_entities = self._get_nested(data=self.content, keys=path_keys)
        if isinstance(lst_entities, dict):
            lst_entities = [lst_entities]
        lst_entities = self.transform_models_external.transform_entities(
            lst_entities=lst_entities
        )
        for entity in lst_entities:
            logger.debug(
                f"Externe entiteit shortcut gevonden '{entity['Name']} in {self.file_pd_ldm}'"
            )
            dict_result[entity["Id"]] = entity
        return dict_result

    def _domains(self) -> dict | None:
        """Extraheert alle domeinen die in het Power Designer model zijn gedefinieerd.

        Deze functie zoekt naar domeinen in het model en retourneert een dictionary met domeininformatie,
        of None als er geen domeinen zijn gevonden.

        Returns:
            dict | None: Een dictionary met domeinen of None als er geen domeinen zijn gevonden.
        """
        path_keys = ["c:Domains", "o:Domain"]
        if lst_domains := self._get_nested(data=self.content, keys=path_keys):
            dict_domains = self.transform_model_internal.domains(
                lst_domains=lst_domains
            )
            return dict_domains
        else:
            logger.error(
                f"Er is geen gebruik van Domain geconstateerd binnen het extraheren van {self.file_pd_ldm}"
            )
            return None

    def _datasources(self) -> dict | None:
        """Haalt alle datasources op die in het Power Designer model zijn gedefinieerd.

        Deze functie zoekt naar datasources in het model en retourneert een dictionary met datasource-informatie,
        of None als er geen datasources zijn gevonden.

        Returns:
            dict | None: Een dictionary met datasources of None als er geen datasources zijn gevonden.
        """
        path_keys = ["c:DataSources", "o:DefaultDataSource"]
        if lst_datasources := self._get_nested(data=self.content, keys=path_keys):
            dict_datasources = self.transform_model_internal.datasources(
                lst_datasources=lst_datasources
            )
            return dict_datasources
        else:
            logger.error(
                f"Er is geen default data source gevonden tijdens het extraheren van {self.file_pd_ldm}"
            )
            return None

    def _relationships(self, lst_entity: list[dict]) -> list[dict] | None:
        """Haalt alle relaties tussen entiteiten op uit het Power Designer model.

        Deze functie zoekt naar relaties tussen entiteiten en retourneert een lijst van deze relaties,
        of None als er geen relaties zijn gevonden.

        Args:
            lst_entity (list[dict]): Lijst van entiteiten waarvoor relaties gezocht worden.

        Returns:
            list[dict] | None: Lijst van relaties of None als er geen relaties zijn gevonden.
        """
        transform_relationships = TransformRelationships(
            file_pd_ldm=self.file_pd_ldm, lst_entities=lst_entity
        )
        path_keys = ["c:Relationships", "o:Relationship"]
        if lst_pd_relationships := self._get_nested(data=self.content, keys=path_keys):
            lst_relationships = transform_relationships.transform(
                lst_relationships=lst_pd_relationships
            )
            return lst_relationships
        else:
            logger.info(
                f"Het extraheren van de relaties tussen entiteiten is gefaald, er zijn geen relaties gevonden. Betreft: {self.file_pd_ldm}."
            )
            return None
