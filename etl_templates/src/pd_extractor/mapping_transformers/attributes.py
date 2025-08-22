from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class MappingAttributesTransformer(BaseTransformer):
    """Collectie van functies om attribuut mappings conform het afgestemde JSON format op te bouwen om ETL generatie te faciliteren"""

    def __init__(self, file_pd_ldm: str, mapping: dict):
        """_summary_

        Args:
            file_pd_ldm (str): Bestandsnaam van het Power Designer document waar de data uit komt
            mapping (dict): Mapping waarvoor de mapping attributes voor worden getransformeerd
        """
        super().__init__(file_pd_ldm=file_pd_ldm)
        self.mapping = mapping
        self.scalar_lookup = {
            scalar["Id"]: scalar
            for scalar in self.mapping.get("SourceComposition")
            if "Expression" in scalar
        }

    def transform(self, dict_attributes: dict) -> dict:
        """Verrijkt, schoont en hangt attribuut mappings om ten behoeven van een mapping

        Args:
            dict_attributes (dict): Alle attributen van het Power Designer LDM die beschikbaar zijn als bron voor de attribuut mapping

        Returns:
            dict: Mapping met geschoonde en verrijkte attribuut mapping
        """
        key_path = ["c:StructuralFeatureMaps", "o:DefaultStructuralFeatureMapping"]
        if attr_maps := self._get_nested(data=self.mapping, keys=key_path):
            attr_maps = [attr_maps] if isinstance(attr_maps, dict) else attr_maps
            attr_maps = self.clean_keys(attr_maps)
            for j in range(len(attr_maps)):
                attr_map = attr_maps[j].copy()
                attr_map["Order"] = j
                self._attribute_mapping(
                    attr_map=attr_map, dict_attributes=dict_attributes
                )
                attr_maps[j] = attr_map.copy()
            self.mapping["AttributeMapping"] = attr_maps
            self.mapping.pop("c:StructuralFeatureMaps", None)
        else:
            logger.error(
                f"Geen Attribute-mapping voor {self.mapping['Name']} van {self.file_pd_ldm} gevonden"
            )
        return self.mapping

    def _attribute_mapping(self, attr_map: dict, dict_attributes: dict) -> None:
        """Verwerkt een enkele attribuut mapping en verrijkt deze met target en source attributen.

        Deze functie zoekt het target attribuut op, koppelt het aan de mapping en verwerkt de bronattributen.

        Args:
            attr_map (dict): De attribuut mapping die verwerkt wordt.
            dict_attributes (dict): Alle attributen van het Power Designer LDM.
        """
        logger.debug(
            f"Start attributemapping voor  {attr_map['Id']} van {self.file_pd_ldm} "
        )
        key_path = [
            "c:BaseStructuralFeatureMapping.Feature",
            "o:EntityAttribute",
            "@Ref",
        ]
        id_attr = self._get_nested(data=attr_map, keys=key_path)
        if id_attr in dict_attributes:
            # Set target attribute
            attr_map["AttributeTarget"] = dict_attributes[id_attr].copy()
            attr_map.pop("c:BaseStructuralFeatureMapping.Feature")
            # Set source for an attribute (called features because they can contain scalar business rules as well)
            self._process_source_features(
                attr_map=attr_map, dict_attributes=dict_attributes
            )
        else:
            logger.warning(
                f"{id_attr} van {self.file_pd_ldm} is niet gevonden binnen target attributen"
            )

    def _process_source_features(self, attr_map: dict, dict_attributes: dict):
        """Verwerkt de source features van een attribuut mapping.

        Args:
            attr_map (dict): De attribuut mapping.
            dict_attributes (dict): Alle attributen van het Power Designer LDM.
        """
        id_entity_alias = self._extract_entity_alias(attr_map=attr_map)
        if "c:SourceFeatures" not in attr_map:
            logger.warning(
                f"Geen source attributen gevonden in mapping '{self.mapping['Name']}' uit '{self.file_pd_ldm}'"
            )
            return
        type_entity = self.determine_reference_type(data=attr_map["c:SourceFeatures"])
        id_attr = attr_map["c:SourceFeatures"][type_entity]["@Ref"]
        if id_attr not in dict_attributes:
            logger.warning(
                f"Bronattribuut '{id_attr}' niet gevonden in mapping '{self.mapping['Name']}' uit '{self.file_pd_ldm}'"
            )
        else:
            attribute = dict_attributes[id_attr]
            self._handle_regular_mapping(attr_map, attribute, id_entity_alias)
            self._handle_aggregate_expression(attr_map)
            self._handle_scalar_mapping(attr_map, attribute, id_entity_alias)
        attr_map.pop("c:SourceFeatures", None)

    def _handle_regular_mapping(self, attr_map: dict, attribute: dict, id_entity_alias: str) -> None:
        """Voegt het bronattribuut toe aan de mapping als het een reguliere mapping betreft.

        Deze functie controleert of het attribuut een reguliere mapping is en voegt in dat geval het bronattribuut en eventueel de entity alias toe aan de mapping.

        Args:
            attr_map (dict): De attribuut mapping die verrijkt wordt.
            attribute (dict): Het attribuut uit het Power Designer LDM.
            id_entity_alias (str): De waarde van de entity alias, indien aanwezig.
        """
        is_regular_mapping = (
            "StereotypeEntity" not in attribute or attribute["StereotypeEntity"] is None
        )
        if is_regular_mapping:
            attr_map["AttributesSource"] = attribute.copy()
            if id_entity_alias:
                attr_map["AttributesSource"]["EntityAlias"] = id_entity_alias

    def _handle_aggregate_expression(self, attr_map: dict) -> None:
        """Voegt een expressie toe aan de mapping als er een aggregate expressie aanwezig is.

        Deze functie controleert of er een aggregate expressie in de mapping staat en voegt deze toe als expressie.

        Args:
            attr_map (dict): De attribuut mapping die verrijkt wordt.
        """
        if "ExtendedAttributesText" in attr_map:
            attr_map["Expression"] = self.extract_value_from_attribute_text(
                attr_map["ExtendedAttributesText"],
                preceded_by="mdde_Aggregate,",
            )

    def _handle_scalar_mapping(self, attr_map: dict, attribute: dict, id_entity_alias: str) -> None:
        if (
            attribute.get("StereotypeEntity") == "mdde_ScalarBusinessRule"
            and id_entity_alias
        ):
            if scalar := self.scalar_lookup.get(id_entity_alias):
                attr_map["Expression"] = scalar.get("Expression")
            else:
                logger.error(
                    f"Kan geen scalar vinden voor attibute mapping '{attr_map['AttributeTarget']['Code']}' in mapping '{self.mapping['Name']}'"
                )

    def _extract_entity_alias(self, attr_map: dict) -> tuple[bool, str | None]:
        """Extraheert de entity alias uit de attribuut mapping indien aanwezig.

        Args:
            attr_map (dict): De attribuut mapping.

        Returns:
            tuple: (has_entity_alias (bool), id_entity_alias (str of None))
        """
        path_keys = [
            "c:ExtendedCollections",
            "o:ExtendedCollection",
            "c:Content",
            "o:ExtendedSubObject",
            "@Ref",
        ]
        id_entity_alias = self._get_nested(data=attr_map, keys=path_keys)
        if id_entity_alias:
            logger.info(
                "Ongebruikt object; file:pd_transform_attribute_mapping; object:id_entity_alias"
            )
            logger.info(f"Object bevat volgende data: '{id_entity_alias}'")
            attr_map.pop("c:ExtendedCollections")
        return id_entity_alias

    def _attribute_scalars(self) -> None:
        """Creëert een expressie string die gebruikt wordt in de attribute mapping
        wanneer een bron attribuut verwijst naar een scalar

        Returns:
            dict: mapping met de zojuist toegevoegde expressie
        """
        if attr_mappings := self.mapping.get("AttributeMapping"):
            for attr_mapping in attr_mappings:
                if alias_id := attr_mapping.get("EntityAlias"):
                    # Lookup the alias_id in composition. For the attributemapping we'll replace the entityalias with the expression we've created in the sourcecomposition
                    if scalar := self.scalar_lookup.get(alias_id):
                        attr_mapping["Expression"] = scalar.get("Expression")
                        attr_mapping.pop("EntityAlias", None)
        else:
            logger.warning(
                f"Attributemapping van {self.mapping['Name']} in '{self.file_pd_ldm}' ontbreekt voor update"
            )
