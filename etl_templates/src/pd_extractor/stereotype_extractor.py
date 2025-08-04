from logtools import get_logger

from .base_extractor import BaseExtractor
from .stereotype_transform import StereotypeTransformer

logger = get_logger(__name__)


class StereotypeExtractor(BaseExtractor):
    """Extraheert Power Designer document objecten die filters, aggregaten en scalars representeren"""

    def __init__(self, pd_content: dict, stereotype_input: str, file_pd_ldm: str):
        """Initialiseert StereotypeExtractor

        Args:
            pd_content (dict): Power Designer document data
            stereotype_input (str): StereoType die aangeeft of het een filter(mdde_FilterBusinessRule), scalar(mdde_ScalarBusinessRule) of aggregate (mdde_AggregateBusinessRule) betreft
            file_pd_ldm (str): Bestand van waar de stereotypes uit geëxtraheerd worden.
        """
        super().__init__(file_pd_ldm=file_pd_ldm)
        self.content = pd_content
        self.transform_stereotype = StereotypeTransformer(file_pd_ldm)
        self.stereotype = stereotype_input

    def get_objects(self, dict_domains: dict) -> list[dict]:
        """Haalt alle objecten van het opgegeven stereotype op gespecificeerd in de initialisatie

        Returns:
            list[dict]: Lijst van geschoonde objecten van het opgegeven stereotype
        """
        lst_objects_input = self.content["c:Entities"]["o:Entity"]
        if isinstance(lst_objects_input, dict):
            lst_objects_input = [lst_objects_input]
        model = self.content["a:Code"]

        lst_objects = []
        for stereotype_object in lst_objects_input:
            if self._is_matching_stereotype(stereotype_object=stereotype_object):
                self._clean_stereotype_object(
                    stereotype_object=stereotype_object, model=model
                )
                lst_objects.append(stereotype_object)
        logger.debug(f"Start met transformaties voor stereotype uit {self.file_pd_ldm}")
        self.transform_stereotype.transform(lst_objects, dict_domains=dict_domains)
        return lst_objects

    def _is_matching_stereotype(self, stereotype_object: dict) -> bool:
        """Controleert of het object het opgegeven stereotype heeft.

        Deze functie vergelijkt het stereotype van het object met het opgegeven stereotype en retourneert True als deze overeenkomen.

        Args:
            stereotype_object (dict): Het object waarvan het stereotype gecontroleerd wordt.

        Returns:
            bool: True als het stereotype overeenkomt, anders False.
        """
        return (
            "a:Stereotype" in stereotype_object
            and stereotype_object["a:Stereotype"] == self.stereotype
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
            "c:DefaultMapping"
        ]
        for key in keys_to_remove:
            if key in stereotype_object:
                stereotype_object.pop(key)
                logger.debug(f"Verwijderd {key} voor lst_objects in {self.file_pd_ldm}")

    def _domains(self) -> dict | None:
        """Extraheert alle domeinen die in het Power Designer model zijn gedefinieerd voor stereotypes.

        Deze functie zoekt naar domeinen in het model en retourneert een dictionary met domeininformatie,
        of None als er geen domeinen zijn gevonden.

        Returns:
            dict | None: Een dictionary met domeinen of None als er geen domeinen zijn gevonden.
        """
        path_keys = ["c:Domains", "o:Domain"]
        if lst_domains := self._get_nested(data=self.content, keys=path_keys):
            dict_domains = self.transform_stereotype.domains(
                lst_domains=lst_domains
            )
            return dict_domains
        else:
            logger.error(
                f"Er is geen gebruik van Domain geconstateerd in '{self.file_pd_ldm}' voor het extraheren van een stereotype"
            )
            return None
