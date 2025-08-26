from logtools import get_logger

from .base_extractor import BaseExtractor
from .stereotype_transform import StereotypeTransformer

logger = get_logger(__name__)


class StereotypeExtractor(BaseExtractor):
    """Extraheert Power Designer document objecten die filters, aggregaten en scalars representeren"""

    def __init__(self, pd_content: dict, file_pd_ldm: str):
        """Initialiseert StereotypeExtractor

        Args:
            pd_content (dict): Power Designer document data
            file_pd_ldm (str): Bestand van waar de stereotypes uit geëxtraheerd worden.
        """
        super().__init__(file_pd_ldm=file_pd_ldm)
        self.content = pd_content
        self.transform_stereotype = StereotypeTransformer(file_pd_ldm)

    def get_scalars(self, dict_domains: dict) -> list[dict]:
        return self._get_objects(
            stereotype="mdde_ScalarBusinessRule", dict_domains=dict_domains
        )

    def get_aggregates(self, dict_domains: dict) -> list[dict]:
        return self._get_objects(
            stereotype="mdde_AggregateBusinessRule", dict_domains=dict_domains
        )

    def get_filters(self, dict_domains: dict) -> list[dict]:
        return self._get_objects(
            stereotype="mdde_FilterBusinessRule", dict_domains=dict_domains
        )

    def _get_objects(self, stereotype: str, dict_domains: dict) -> list[dict]:
        """Haalt alle objecten van het opgegeven stereotype op gespecificeerd in de initialisatie

        Args:
            stereotype (str): Type stereotype object dat geretourneerd moet worden.
            dict_domains (dict): Domeinen die gebruikt worden voor transformaties

        Returns:
            list[dict]: Lijst van geschoonde objecten van het opgegeven stereotype
        """
        objects_input = self.content["c:Entities"]["o:Entity"]
        if isinstance(objects_input, dict):
            objects_input = [objects_input]
        model = self.content["a:Code"]

        objects = []
        for stereotype_object in objects_input:
            if self._is_matching_stereotype(
                stereotype_expected=stereotype, stereotype_object=stereotype_object
            ):
                self._clean_stereotype_object(
                    stereotype_object=stereotype_object, model=model
                )
                objects.append(stereotype_object)
        logger.debug(f"Start met transformaties voor stereotype uit {self.file_pd_ldm}")
        self.transform_stereotype.transform(objects, dict_domains=dict_domains)
        return objects

    def _is_matching_stereotype(
        self, stereotype_expected: str, stereotype_object: dict
    ) -> bool:
        """Controleert of het object het opgegeven stereotype heeft.

        Deze functie vergelijkt het stereotype van het object met het opgegeven stereotype en retourneert True als deze overeenkomen.

        Args:
            stereotype_object (dict): Het object waarvan het stereotype gecontroleerd wordt.

        Returns:
            bool: True als het stereotype overeenkomt, anders False.
        """
        return (
            "a:Stereotype" in stereotype_object
            and stereotype_object["a:Stereotype"] == stereotype_expected
        )

    def _clean_stereotype_object(self, stereotype_object: dict, model: str):
        """Maakt een stereotype object schoon door overbodige velden te verwijderen en het model toe te voegen.

        Deze functie voegt het model toe aan het object en verwijdert attributen die niet relevant zijn voor het opgegeven stereotype.

        Args:
            stereotype_object (dict): Het object dat opgeschoond moet worden.
            model (str): De code van het model waartoe het object behoort.
        """
        stereotype_object["CodeModel"] = model
        keys_to_remove = [
            "c:ExtendedCollections",
            "c:ExtendedCompositions",
            "c:DefaultMapping",
        ]
        for key in keys_to_remove:
            if key in stereotype_object:
                stereotype_object.pop(key)
