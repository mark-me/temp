from log_config import logging
from .pd_transform_model_internal import TransformModelInternal
from .pd_transform_models_external import TransformModelsExternal

logger = logging.getLogger(__name__)


class ModelExtractor:
    """Collectie van functies die gebruikt worden om de relevante objecten uit een Power Designer LDM te extraheren
    """

    def __init__(self, pd_content):
        self.content = pd_content
        self.transform_model_internal = TransformModelInternal()
        self.transform_models_external = TransformModelsExternal()
        self.dict_domains = self.__domains()

    def models(self, lst_aggregates: list) -> list:
        """"Haalt alle modellen en hun bijbehorende objecten op die gebruikt worden in het Power Designer LDM

        Args:
            lst_aggregates (list): Aggregaten die onderdeel zijn van het doelmodel en gebruikt worden in de ETL

        Returns:
            list: lijst van modellen die gebruikt worden in het Power Designer LDM document
        """
        # TODO: added lst_aggregates as input because of reference issues due to relationships between entity and objects
        dict_model_internal = self.__model_internal(lst_aggregates=lst_aggregates)
        #TODO: need to add the condition for c:Packages if we encounter models that use packages
        if "o:Shortcut" in self.content["c:Entities"]:
            lst_models_external = self.__models_external()
        else:
            lst_models_external = []
            logger.warning("o:Shortcut is missing in self.content")
        # dict_model_physical = self.__models_physical()
        # Combine models
        if  not lst_models_external:
            lst_models = [dict_model_internal] #+ [dict_model_physical]
        else:
            lst_models = lst_models_external + [dict_model_internal]
        return lst_models

    def __model_internal(self, lst_aggregates: list) -> dict:
        """Haalt alle vastgelegde data van het model op vanuit het geladen Power Designer document 

        Args:
            lst_aggregates (list): Aggregaten die onderdeel zijn van het doelmodel en gebruikt worden in de ETL

        Returns:
            dict: In het Power Designer LDM ontworpen model (niet geïmporteerd voor ETL)
        """
        # TODO: added lst_aggregates as input because of reference issues due to relationships between entity and objects
        model = self.transform_model_internal.model(content=self.content)
        # Model add entity data
        self.lst_entity = self.__entities_internal()
        if isinstance(self.lst_entity, dict):
            logging.warning("List object is actually dictionary; file:pd_model_extractor; object:lst_entity")
            self.lst_entity = [self.lst_entity]
        model["Entities"] = self.lst_entity
        model["Relationships"] = self.__relationships(lst_entity=self.lst_entity, lst_aggregates=lst_aggregates)
        model["DataSources"] = self.__datasources()
        return model

    def __entities_internal(self) -> list:
        """Geeft alle entiteiten uit het Power Designer model terug met al zijn attributen en identifiers


        Returns:
            list: Entities
        """
        lst_entity = self.content["c:Entities"]["o:Entity"]

        entity1 = []
        for i in range(len(lst_entity)):
            entity_in = lst_entity[i]
            if "Stereotype" not in entity_in or entity_in["Stereotype"] == 'mdde_AggregateBusinessRule':
                entity1.append(entity_in)
        lst_entity = entity1
        self.transform_model_internal.entities(lst_entity, dict_domains=self.dict_domains)
        return lst_entity

    def __models_external(self) -> list:
        """Haalt alle data van modellen op die worden onderhouden buiten het geladen Power Designer document en die gebruikt 
        worden voor horizontale lineage

        Returns:
            list: Lijst van externe modellen met al hun bijbehorende elementen
        """
        # The models will be derived by looking up the TargetModels associated with the entity shortcuts
        # External entity (shortcut) data
        dict_entities = self.__entities_external()
        # Retain 'TargetModels' have references to entities
        lst_target_model = self.content["c:TargetModels"]["o:TargetModel"]
        lst_models = self.transform_models_external.models(
            lst_models=lst_target_model, dict_entities=dict_entities
        )
        return lst_models

    def __entities_external(self) -> dict:
        """Haalt alle entities en bijbehorende informatie op van het externe model op

        Returns:
            dict: Een dict van Entiteiten, waar elke sleutel data als waarde bevat van een Entiteit en de bijbehorende attributen
        """
        # External model entity data
        dict_result = {}
        if "c:Packages" in self.content:
            lst_entities = self.content["c:Packages"]["o:Package"]["c:Entities"]["o:Shortcut"]
        else:
            lst_entities = self.content["c:Entities"]["o:Shortcut"]
        if isinstance(lst_entities, dict):
            lst_entities = [lst_entities]
        lst_entities = self.transform_models_external.entities(lst_entities=lst_entities)
        for entity in lst_entities:
            logger.debug(f"Found external entity shortcut for '{entity['Name']}'")
            dict_result[entity["Id"]] = entity
        return dict_result

    def __domains(self) -> dict:
        """Extraheert data type informatie (domains) dat domain codes van een attribuut koppelt aan logische data types

        Returns:
            dict: Domain codes met logische data types
        """
        dict_domains = {}
        if "c:Domains" in self.content:
            if "o:Domain" in self.content["c:Domains"]:
                lst_domains = self.content["c:Domains"]["o:Domain"]
                dict_domains = self.transform_model_internal.domains(lst_domains=lst_domains)
            else:
                logger.error("Er is geen Domain gevonden tijdens het extraheren van een model, dit is nodig voor het maken van een werkend script")
        else:
            logger.error("Er is geen gebruik van Domain geconstateerd binnen het extraheren van een model")
        return dict_domains

    def __datasources(self) -> dict:
        """Extraheert datasources die worden gebruikt in het model
        
        Returns:
            dict: alle informatie over datasources gebruikt in het model
        """
        dict_datasources = {}
        if "c:DataSources" in self.content:
            if "o:DefaultDataSource" in self.content["c:DataSources"]:
                lst_datasources = self.content["c:DataSources"]["o:DefaultDataSource"]
                dict_datasources = self.transform_model_internal.datasources(lst_datasources=lst_datasources)
            else:
                logger.error("Er is geen default data source gevonden tijdens het extraheren van het model")
        else:
            logger.error("Er is geen data source gevonden tijdens het extraheren van het model")
            dict_datasources = dict_datasources
        return dict_datasources
            

    def __relationships(self, lst_entity: list, lst_aggregates: list) -> list:
        """Extraheert relaties tussen entiteiten

        Args:
            lst_entity (list): Entiteiten die worden gebruikt in de relatiebeschrijving
            lst_aggregates (list): Aggregaten die worden gebruikt in de relatiebeschrijving

        Returns:
            list: _description_
        """
        # TODO: lst_aggregates added due to reference issues caused by relation between object and entity
        lst_relationships = []
        if "c:Relationships" in self.content:
            lst_pd_relationships = self.content["c:Relationships"]["o:Relationship"]
            lst_relationships = self.transform_model_internal.relationships(
                lst_relationships=lst_pd_relationships, lst_entity=lst_entity, lst_aggregates = lst_aggregates
            )
        else:
            logger.warning("Het extraheren van de relaties tussen entiteiten is gefaald, er zijn geen relaties gevonden.")
        return lst_relationships
