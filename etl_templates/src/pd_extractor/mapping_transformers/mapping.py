from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class MappingTransformer(BaseTransformer):
    """Transformeert en normaliseert mapping data.

    Deze klasse zorgt voor het opschonen en normaliseren van mappings, waaronder het vervangen van spaties in namen.
    """
    def __init__(self, file_pd_ldm):
        super().__init__(file_pd_ldm)

    def transform(self, mapping: list[dict]) -> dict:
        """Schoont en normaliseert de mapping data.

        Deze functie verwerkt de mapping, verwijdert onnodige tekens en normaliseert de namen.

        Args:
            mapping (list[dict]): De mapping die getransformeerd moet worden.

        Returns:
            dict: De opgeschoonde en genormaliseerde mapping.
        """
        mapping = self.clean_keys(mapping)
        mapping = self._normalize_names(mapping)
        return mapping

    def _normalize_names(self, mapping: dict) -> dict:
        """Normaliseert de namen in de mapping door spaties te vervangen.

        Deze functie controleert of er spaties in de naam staan en vervangt deze door underscores.

        Args:
            mapping (dict): De mapping waarvan de naam genormaliseerd moet worden.

        Returns:
            dict: De mapping met genormaliseerde naam.
        """
        if " " in mapping["Name"]:
            logger.warning(
                f"Er staan spatie(s) in de mapping naam staan voor '{mapping['Name']}' uit {self.file_pd_ldm}."
            )
            mapping["Name"] = mapping["Name"].replace(" ", "_")
        return mapping
