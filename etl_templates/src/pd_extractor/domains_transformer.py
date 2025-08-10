from logtools import get_logger

from .base_transformer import BaseTransformer

logger = get_logger(__name__)


class DomainsTransformer(BaseTransformer):
    """Hangt om en schoont elk onderdeel van de metadata van het model dat omgezet wordt naar DDL en ETL generatie"""

    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def transform(self, domains: list[dict]) -> dict:
        """Verwerkt en schoont domein data uit het Power Designer model.

        Deze functie converteert timestamps, maakt de domein data schoon en retourneert een dictionary met domeinen.

        Args:
            domains (list[dict]): Lijst van domeinen uit het Power Designer model.

        Returns:
            dict: Dictionary met domeinen, waarbij de sleutel het domein-ID is.
        """
        domains = [domains] if isinstance(domains, dict) else domains
        domains = self.convert_timestamps(domains)
        domains = self.clean_keys(domains)
        dict_domains = {
            domain["Id"]: domain for domain in domains if "Id" in domain
        }
        return dict_domains
