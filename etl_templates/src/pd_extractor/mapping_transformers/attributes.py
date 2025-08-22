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
        if attr_mappings := self._get_attribute_mappings():
            for j in range(len(attr_mappings)):
                attr_map = attr_mappings[j].copy()
                attr_map["Order"] = j
                self._handle_attribute_mapping(
                    attr_map=attr_map, dict_attributes=dict_attributes
                )
                attr_mappings[j] = attr_map.copy()
            self.mapping["AttributeMapping"] = attr_mappings
            self.mapping.pop("c:StructuralFeatureMaps", None)
        return self.mapping

    def _get_attribute_mappings(self) -> list[dict] | None:
        """Haalt de lijst van attribuut mappings op uit de mapping.

        Deze functie zoekt naar de attribuut mappings in de mapping en retourneert deze als een lijst,
        of None als ze niet gevonden zijn.

        Returns:
            list[dict] | None: De lijst van attribuut mappings of None als deze niet gevonden zijn.
        """
        key_path = ["c:StructuralFeatureMaps", "o:DefaultStructuralFeatureMapping"]
        if attr_mappings := self._get_nested(data=self.mapping, keys=key_path):
            attr_mappings = (
                [attr_mappings] if isinstance(attr_mappings, dict) else attr_mappings
            )
            attr_mappings = self.clean_keys(attr_mappings)
            return attr_mappings
        else:
            logger.error(
                f"Geen Attribute-mapping voor {self.mapping['Name']} van {self.file_pd_ldm} gevonden"
            )
            return None

    def _handle_attribute_mapping(self, attr_map: dict, dict_attributes: dict) -> None:
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
            self._handle_source_feature(
                attr_map=attr_map, dict_attributes=dict_attributes
            )
        else:
            logger.warning(
                f"{id_attr} van {self.file_pd_ldm} is niet gevonden binnen target attributen"
            )

    def _handle_source_feature(self, attr_map: dict, dict_attributes: dict):
        """Verwerkt de source feature van een attribuut mapping.

        Args:
            attr_map (dict): De attribuut mapping.
            dict_attributes (dict): Alle attributen van het Power Designer LDM.
        """
        id_composition = self._get_composition_alias(attr_map=attr_map)
        if "c:SourceFeatures" not in attr_map:
            logger.warning(
                f"Geen source attributen gevonden in mapping '{self.mapping['Name']}' uit '{self.file_pd_ldm}'"
            )
            return
        id_attr = self._get_source_feature_reference(attr_map)
        if id_attr not in dict_attributes:
            logger.warning(
                f"Bronattribuut '{id_attr}' niet gevonden in mapping '{self.mapping['Name']}' uit '{self.file_pd_ldm}'"
            )
        else:
            attribute = dict_attributes[id_attr]
            self._handle_regular_mapping(
                attr_map=attr_map, attribute=attribute, id_composition=id_composition
            )
            self._handle_aggregate_expression(attr_map)
            self._handle_scalar_mapping(
                attr_map=attr_map, attribute=attribute, id_composition=id_composition
            )
        attr_map.pop("c:SourceFeatures", None)

    def _get_composition_alias(self, attr_map: dict) -> str | None:
        """Zoekt en retourneert de composition alias uit de attribuut mapping indien aanwezig.

        Deze functie zoekt naar een composition alias in de attribuut mapping en verwijdert de bijbehorende collectie als deze gevonden is.

        Args:
            attr_map (dict): De attribuut mapping waarin gezocht wordt naar de composition alias.

        Returns:
            str | None: De waarde van de composition alias indien gevonden, anders None.
        """
        path_keys = [
            "c:ExtendedCollections",
            "o:ExtendedCollection",
            "c:Content",
            "o:ExtendedSubObject",
            "@Ref",
        ]
        id_alias = self._get_nested(data=attr_map, keys=path_keys)
        if id_alias:
            # FIXME logger info informatief maken
            logger.info(
                "Ongebruikte object; file:pd_transform_attribute_mapping; object:id_entity_alias"
            )
            logger.info(f"Object bevat volgende data: '{id_alias}'")
            attr_map.pop("c:ExtendedCollections")
        return id_alias

    def _get_source_feature_reference(self, attr_map: dict) -> str:
        """Bepaalt het type entity en retourneert de referentie naar het bronattribuut.

        Deze functie zoekt het juiste type entity in de source features en geeft de bijbehorende referentie terug.

        Args:
            attr_map (dict): De attribuut mapping waarin de source features staan.

        Returns:
            str: De referentie naar het bronattribuut.
        """
        type_entity = self.determine_reference_type(data=attr_map["c:SourceFeatures"])
        id_attr = attr_map["c:SourceFeatures"][type_entity]["@Ref"]
        return id_attr

    def _handle_regular_mapping(
        self, attr_map: dict, attribute: dict, id_composition: str
    ) -> None:
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
            if id_composition:
                attr_map["AttributesSource"]["EntityAlias"] = id_composition

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

    def _handle_scalar_mapping(
        self, attr_map: dict, attribute: dict, id_composition: str
    ) -> None:
        """Voegt een scalar expressie toe aan de mapping als het attribuut een scalar business rule is.

        Deze functie controleert of het attribuut een scalar business rule is en voegt de bijbehorende scalar expressie toe aan de mapping, of logt een foutmelding als deze niet gevonden kan worden.

        Args:
            attr_map (dict): De attribuut mapping die verrijkt wordt.
            attribute (dict): Het attribuut uit het Power Designer LDM.
            id_composition (str): De waarde van de composition alias, indien aanwezig.
        """
        is_scalar = attribute.get("StereotypeEntity") == "mdde_ScalarBusinessRule"
        if is_scalar and id_composition:
            if scalar := self.scalar_lookup.get(id_composition):
                attr_map["Expression"] = scalar.get("Expression")
            else:
                logger.error(
                    f"Kan geen scalar vinden voor attibute mapping '{attr_map['AttributeTarget']['Code']}' in mapping '{self.mapping['Name']}'"
                )
