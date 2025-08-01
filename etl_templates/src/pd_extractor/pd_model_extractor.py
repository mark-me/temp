from logtools import get_logger

from .pd_transform_model_internal import TransformModelInternal
from .pd_transform_models_external import TransformModelsExternal

logger = get_logger(__name__)


class ModelExtractor:
    """Collectie van functies die gebruikt worden om de relevante objecten uit een Power Designer LDM te extraheren"""

    def __init__(self, pd_content: dict, file_pd_ldm: str):
        self.file_pd_ldm = file_pd_ldm
        self.content = pd_content
        self.transform_model_internal = TransformModelInternal(file_pd_ldm)
        self.transform_models_external = TransformModelsExternal(file_pd_ldm)
        self.dict_domains = self._domains()

    def models(self) -> list[dict]:
        """Haalt alle modellen en hun bijbehorende objecten op die gebruikt worden in het Power Designer LDM

        Args:
            lst_aggregates (list): Aggregaten die onderdeel zijn van het doelmodel en gebruikt worden in de ETL

        Returns:
            list[dict]: lijst van modellen die gebruikt worden in het Power Designer LDM document
        """
        dict_model_internal = self._model_internal()
        # TODO: need to add the condition for c:Packages if we encounter models that use packages
        if "o:Shortcut" in self.content["c:Entities"]:
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
        model = self.transform_model_internal.model(content=self.content)
        # Model add entity data
        self.lst_entity = self._entities_internal()
        if isinstance(self.lst_entity, dict):
            self.lst_entity = [self.lst_entity]
        model["Entities"] = self.lst_entity
        model["Relationships"] = self._relationships(lst_entity=self.lst_entity)
        model["DataSources"] = self._datasources()
        return model

    def _entities_internal(self) -> list[dict]:
        """Geeft alle entiteiten uit het Power Designer model terug met al zijn attributen en identifiers


        Returns:
            list[dict]: Entities
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

    def _models_external(self) -> list[dict]:
        """Haalt alle data van modellen op die worden onderhouden buiten het geladen Power Designer document en die gebruikt
        worden voor horizontale lineage

        Returns:
            list[dict]: Lijst van externe modellen met al hun bijbehorende elementen
        """
        # The models will be derived by looking up the TargetModels associated with the entity shortcuts
        # External entity (shortcut) data
        dict_entities = self._entities_external()
        # Retain 'TargetModels' have references to entities
        lst_target_model = self.content["c:TargetModels"]["o:TargetModel"]
        lst_models = self.transform_models_external.models(
            lst_models=lst_target_model, dict_entities=dict_entities
        )
        return lst_models

    def _entities_external(self) -> dict:
        """Haalt alle entities en bijbehorende informatie op van het externe model op

        Returns:
            dict: Een dict van Entiteiten, waar elke sleutel data als waarde bevat van een Entiteit en de bijbehorende attributen
        """
        # External model entity data
        dict_result = {}
        if "c:Packages" in self.content:
            lst_entities = self.content["c:Packages"]["o:Package"]["c:Entities"][
                "o:Shortcut"
            ]
        else:
            lst_entities = self.content["c:Entities"]["o:Shortcut"]
        if isinstance(lst_entities, dict):
            lst_entities = [lst_entities]
        lst_entities = self.transform_models_external.entities(
            lst_entities=lst_entities
        )
        for entity in lst_entities:
            logger.debug(
                f"Externe entiteit shortcut gevonden '{entity['Name']} in {self.file_pd_ldm}'"
            )
            dict_result[entity["Id"]] = entity
        return dict_result

    def _domains(self) -> dict:
        """Extraheert data type informatie (domains) dat domain codes van een attribuut koppelt aan logische data types

        Returns:
            dict: Domain codes met logische data types
        """
        dict_domains = {}
        if "c:Domains" in self.content:
            if "o:Domain" in self.content["c:Domains"]:
                lst_domains = self.content["c:Domains"]["o:Domain"]
                dict_domains = self.transform_model_internal.domains(
                    lst_domains=lst_domains
                )
            else:
                logger.error(
                    f"Er is geen Domain gevonden tijdens het extraheren van {self.file_pd_ldm}, dit is nodig voor het maken van een werkend script"
                )
        else:
            logger.error(
                f"Er is geen gebruik van Domain geconstateerd binnen het extraheren van {self.file_pd_ldm}"
            )
        return dict_domains

    def _datasources(self) -> dict:
        """Extraheert datasources die worden gebruikt in het model

        Returns:
            dict: alle informatie over datasources gebruikt in het model
        """
        dict_datasources = {}
        if "c:DataSources" in self.content:
            if "o:DefaultDataSource" in self.content["c:DataSources"]:
                lst_datasources = self.content["c:DataSources"]["o:DefaultDataSource"]
                dict_datasources = self.transform_model_internal.datasources(
                    lst_datasources=lst_datasources
                )
            else:
                logger.error(
                    f"Er is geen default data source gevonden tijdens het extraheren van {self.file_pd_ldm}"
                )
        else:
            logger.error(
                f"Er is geen data source gevonden tijdens het extraheren van het {self.file_pd_ldm}"
            )
            dict_datasources = dict_datasources
        return dict_datasources

    def _relationships(self, lst_entity: list[dict]) -> list[dict]:
        """
        Extraheert en transformeert de relaties tussen entiteiten uit het Power Designer model.

        Deze functie haalt de relaties op uit het model, transformeert deze met behulp van de interne transformer,
        en retourneert een lijst van gestructureerde relatie-objecten.

        Args:
            lst_entity (list[dict]): Lijst van entiteiten uit het model.

        Returns:
            list[dict]: Lijst van getransformeerde relaties tussen entiteiten.
        """
        lst_relationships = []
        if "c:Relationships" in self.content:
            lst_pd_relationships = self.content["c:Relationships"]["o:Relationship"]
            lst_relationships = self.transform_model_internal.relationships(
                lst_relationships=lst_pd_relationships, lst_entity=lst_entity
            )
        else:
            logger.warning(
                f"Het extraheren van de relaties tussen entiteiten is gefaald, er zijn geen relaties gevonden. Betreft: {self.file_pd_ldm}."
            )
        return lst_relationships
