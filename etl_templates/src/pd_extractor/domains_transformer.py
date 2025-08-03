from logtools import get_logger

from .base_transformer import TransformerBase

logger = get_logger(__name__)


class TransformDomains(TransformerBase):
    """Hangt om en schoont elk onderdeel van de metadata van het model dat omgezet wordt naar DDL en ETL generatie"""

    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def transform(self, lst_domains: list[dict]) -> dict:
        """Verwerkt en schoont domein data uit het Power Designer model.

        Deze functie converteert timestamps, maakt de domein data schoon en retourneert een dictionary met domeinen.

        Args:
            lst_domains (list[dict]): Lijst van domeinen uit het Power Designer model.

        Returns:
            dict: Dictionary met domeinen, waarbij de sleutel het domein-ID is.
        """
        lst_domains = [lst_domains] if isinstance(lst_domains, dict) else lst_domains
        lst_domains = self.convert_timestamps(lst_domains)
        lst_domains = self.clean_keys(lst_domains)
        dict_domains = {
            domain["Id"]: domain for domain in lst_domains if "Id" in domain
        }
        return dict_domains
