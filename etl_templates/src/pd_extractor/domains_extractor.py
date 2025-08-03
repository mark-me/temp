from logtools import get_logger

from .extractor_base import ExtractorBase
from .domains_transformer import TransformDomains
logger = get_logger(__name__)


class DomainsExtractor(ExtractorBase):
    def __init__(self, pd_content: dict, file_pd_ldm: str):
        super().__init__(file_pd_ldm=file_pd_ldm)
        self.content = pd_content

    def get_domains(self) -> dict | None:
        """Extraheert alle domeinen die in het Power Designer model zijn gedefinieerd.

        Deze functie zoekt naar domeinen in het model en retourneert een dictionary met domeininformatie,
        of None als er geen domeinen zijn gevonden.

        Returns:
            dict | None: Een dictionary met domeinen of None als er geen domeinen zijn gevonden.
        """
        path_keys = ["c:Domains", "o:Domain"]
        if domains := self._get_nested(data=self.content, keys=path_keys):
            transform_domains = TransformDomains(file_pd_ldm=self.file_pd_ldm)
            dict_domains = transform_domains.transform(
                lst_domains=domains
            )
            return dict_domains
        else:
            logger.error(
                f"Er is geen gebruik van Domains geconstateerd binnen het extraheren van {self.file_pd_ldm}"
            )
            return None