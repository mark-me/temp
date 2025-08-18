from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class TargetEntityTransformer(BaseTransformer):
    """Vormt mapping data om en verrijkt dit met entiteit en attribuut data"""

    def __init__(self, file_pd_ldm: str, mapping: dict):
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm
        self.mapping = mapping


    def transform(self, dict_objects: dict) -> dict:
        """Omvormen van mapping data en verrijkt dit met doelentiteit en attribuut data

        Args:
            dict_objects (dict): Alle objecten(entities/filters/scalars/aggregaten) in het document (internal en external)

        Returns:
            list: een mapping met geschoonde doelentiteit data.
        """
        # Target entity rerouting and enriching
        path_keys = ["c:Classifier", "o:Entity", "@Ref"]
        if id_entity_target := self._get_nested(data=self.mapping, keys=path_keys):
            self.mapping["EntityTarget"] = dict_objects[id_entity_target]
        else:
            logger.warning(
                f"Mapping zonder doel entiteit gevonden: '{self.mapping['Name']}' in {self.file_pd_ldm}"
            )
        return self.mapping
