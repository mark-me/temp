from log_config import logging
from .pd_transform_stereotype import TransformStereotype

logger = logging.getLogger(__name__)

class StereotypeExtractor:
    """Extraheert Power Designer document objecten die filters, aggregaten en scalars representeren
    """
    def __init__(self,pd_content: dict, stereotype_input: str):
        """Initialiseert StereotypeExtractor

        Args:
            pd_content (dict): Power Designer document data
            stereotype_input (str): StereoType die aangeeft of het een filter(mdde_FilterBusinessRule), scalar(mdde_ScalarBusinessRule) of aggregate (mdde_AggregateBusinessRule) betreft
        """
        self.content = pd_content
        self.transform_stereotype = TransformStereotype()
        self.stereotype = stereotype_input
        self.dict_domains = self.__domains()

    def objects(self) -> list:
        """Haalt alle objecten op uit het model op basis van het stereotype gespecificeerd in de initialisatie

        Returns:
            list: List van geschoonde objecten van het opgegeven stereotype
        """
        lst_objects = self.__objects()
        return lst_objects

    def __objects(self) -> list:
        """Haalt alle objecten van het opgegeven stereotype op gespecificeerd in de initialisatie

        Returns:
            list: List van geschoonde objecten van het opgegeven stereotype
        """
        #TODO: containers that need to be removed added to list (f.e. lst_ignored_mappings construction in pd_mapping_extractor)
        lst_objects_input = self.content["c:Entities"] ["o:Entity"]
        model = self.content["a:Code"]
        stereotype = self.stereotype

        lst_objects = []
        for i in range(len(lst_objects_input)):
            object = lst_objects_input[i]
            if "a:Stereotype" in object:
                if object["a:Stereotype"]  == stereotype:
                    object["CodeModel"] = model
                    if "c:ExtendedCollections" in object:
                        object.pop("c:ExtendedCollections")
                        logger.debug("Removed c:ExtendedCollections from lst_objects'")
                    if "c:ExtendedCompositions" in object:
                        object.pop("c:ExtendedCompositions")
                        logger.debug("Removed c:ExtendedCompositions from lst_objects'")
                    if "c:DefaultMapping" in object:
                        object.pop("c:DefaultMapping")
                        logger.debug("Removed c:DefaultMapping from lst_objects'")
                    if stereotype != 'mdde_AggregateBusinessRule':
                        if  ("c:PrimaryIdentifier") in object:
                            object.pop("c:PrimaryIdentifier")
                        if "c:Identifiers" in object:
                            object.pop("c:Identifiers")
                            logger.debug("Removed c:Identifiers from lst_objects'")
                    lst_objects.append(object)
        lst_objects = lst_objects
        logger.debug("Start with transform for stereotype")
        self.transform_stereotype.objects(lst_objects, dict_domains=self.dict_domains)
        logger.debug("Finished with transform for stereotype")
        return lst_objects

    def __domains(self) -> dict:
        """Haalt op en schoont domain data voor de objecten van het opgegeven stereotype

        Returns:
            dict: Domains waar het Power Designer object id is de sleutel van het domain
        """
        dict_domains = {}
        if "c:Domains" in self.content:
            if "o:Domain" in self.content["c:Domains"]:
                lst_domains = self.content["c:Domains"]["o:Domain"]
                logger.debug("Start with collecting domains for stereotype")
                dict_domains = self.transform_stereotype.domains(lst_domains=lst_domains)
                logger.debug("Finished with collecting domains for stereotype")
            else:
                logger.error("Er is geen Domain gevonden tijdens het extraheren van een stereotype, dit is nodig voor het maken van een werkend script")
        else:
            logger.error("Er is geen gebruik van Domain geconstateerd voor het extraheren van een stereotype")
        return dict_domains