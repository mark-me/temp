from logtools import get_logger
from .pd_transform_stereotype import TransformStereotype

logger = get_logger(__name__)

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
        self.dict_domains = self._domains()

    def objects(self) -> list:
        """Haalt alle objecten op uit het model op basis van het stereotype gespecificeerd in de initialisatie

        Returns:
            list: List van geschoonde objecten van het opgegeven stereotype
        """
        lst_objects = self._objects()
        return lst_objects

    def _objects(self) -> list:
        """Haalt alle objecten van het opgegeven stereotype op gespecificeerd in de initialisatie

        Returns:
            list: List van geschoonde objecten van het opgegeven stereotype
        """
        lst_objects_input = self.content["c:Entities"]["o:Entity"]
        model = self.content["a:Code"]

        lst_objects = []
        for i in range(len(lst_objects_input)):
            stereotype_object = lst_objects_input[i]
            if self._is_matching_stereotype(stereotype_object=stereotype_object):
                self._clean_stereotype_object(stereotype_object=stereotype_object, model=model)
                lst_objects.append(stereotype_object)
        logger.debug("Start with transform for stereotype")
        self.transform_stereotype.objects(lst_objects, dict_domains=self.dict_domains)
        logger.debug("Finished with transform for stereotype")
        return lst_objects

    def _is_matching_stereotype(self, stereotype_object: dict) -> bool:
        """Controleert of het object het opgegeven stereotype heeft.

        Deze functie vergelijkt het stereotype van het object met het opgegeven stereotype en retourneert True als deze overeenkomen.

        Args:
            stereotype_object (dict): Het object waarvan het stereotype gecontroleerd wordt.

        Returns:
            bool: True als het stereotype overeenkomt, anders False.
        """
        return "a:Stereotype" in stereotype_object and stereotype_object["a:Stereotype"] == self.stereotype

    def _clean_stereotype_object(self, stereotype_object: dict, model: str):
        """Maakt een stereotype object schoon door overbodige velden te verwijderen en het model toe te voegen.

        Deze functie voegt het model toe aan het object en verwijdert attributen die niet relevant zijn voor het opgegeven stereotype.

        Args:
            stereotype_object (dict): Het object dat opgeschoond moet worden.
            model (str): De code van het model waartoe het object behoort.
        """
        stereotype_object["CodeModel"] = model
        if "c:ExtendedCollections" in stereotype_object:
            stereotype_object.pop("c:ExtendedCollections")
            logger.debug("Removed c:ExtendedCollections from lst_objects'")
        if "c:ExtendedCompositions" in stereotype_object:
            stereotype_object.pop("c:ExtendedCompositions")
            logger.debug("Removed c:ExtendedCompositions from lst_objects'")
        if "c:DefaultMapping" in stereotype_object:
            stereotype_object.pop("c:DefaultMapping")
            logger.debug("Removed c:DefaultMapping from lst_objects'")
        # if stereotype_object["a:Stereotype"] != 'mdde_AggregateBusinessRule':
        #     if "c:PrimaryIdentifier" in stereotype_object:
        #         stereotype_object.pop("c:PrimaryIdentifier")
        #     if "c:Identifiers" in stereotype_object:
        #         stereotype_object.pop("c:Identifiers")
        #         logger.debug("Removed c:Identifiers from lst_objects'")

    def _domains(self) -> dict:
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