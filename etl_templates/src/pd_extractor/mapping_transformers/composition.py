from logtools import get_logger

from ..base_transformer import BaseTransformer
from .composition_join_conditions import JoinConditionsTransformer
from .composition_source_condition import SourceConditionTransform
from .composition_busines_rule import BusinessRuleTransform

logger = get_logger(__name__)


class SourceCompositionTransformer(BaseTransformer):
    def __init__(self, file_pd_ldm: str, mapping: dict):
        """Transformator voor source composition data.

        Args:
            mapping (dict): De mapping die getransformeerd moet worden.
        """
        super().__init__(file_pd_ldm)
        self.mapping = mapping

    def transform(
        self, dict_objects: dict, dict_attributes: dict, dict_datasources: dict
    ) -> dict:
        """Transformeert en verrijkt de mapping met source composition data.

        Deze functie verwerkt de mapping, haalt compositie-items op, verrijkt deze, filtert specifieke items en voegt de resultaten toe aan de mapping.

        Args:
            dict_objects (dict): Alle objecten (entiteiten, filters, scalars, aggregaten).
            dict_attributes (dict): Alle attributen.
            dict_datasources (dict): Alle datasources.

        Returns:
            dict: De getransformeerde mapping met verrijkte source composition data.
        """
        composition = self._extract_composition(self.mapping)
        composition_items = self._extract_composition_items(composition)
        composition_items = self._transform_composition_items(
            composition_items=composition_items,
            dict_objects=dict_objects,
            dict_attributes=dict_attributes,
        )

        self._mapping_enrich_datasource(dict_datasources=dict_datasources)
        return self.mapping

    def _extract_composition(self, mapping: dict) -> list[dict]:
        """Haalt de lijst van composities op uit de mapping."""
        path_keys = ["c:ExtendedCompositions", "o:ExtendedComposition"]
        composition = self._get_nested(data=mapping, keys=path_keys)
        composition = self.clean_keys(composition)
        composition = [composition] if isinstance(composition, dict) else composition
        composition = self._compositions_remove_mdde_examples(composition)
        return composition

    def _extract_composition_items(self, composition: dict) -> list[dict]:
        """Haalt de compositie-items op uit de opgegeven compositie.

        Deze functie zoekt en retourneert alle relevante compositie-items uit de compositie,
        en geeft een lege lijst terug als er geen inhoud is.

        Args:
            composition (dict): De compositie waaruit de items gehaald worden.

        Returns:
            list[dict]: Een lijst van compositie-items.
        """
        lst_composition_items = []
        content = composition.get("c:ExtendedComposition.Content")
        if (
            "c:ExtendedCollections" in content["o:ExtendedSubObject"]
            or "o:ExtendedSubObject" in content
        ):
            lst_composition_items = content["o:ExtendedSubObject"]
        elif "c:ExtendedCollections" in content:
            lst_composition_items = content["c:ExtendedCollections"]
        else:
            logger.warning(f"Mapping zonder inhoud voor {self.file_pd_ldm}")
        if isinstance(lst_composition_items, dict):
            lst_composition_items = [lst_composition_items]
        return lst_composition_items

    def _transform_composition_items(
        self,
        composition_items: list[dict],
        dict_objects: dict,
        dict_attributes: dict,
    ) -> list[dict]:
        """Transformeert en verrijkt een lijst van compositie-items.

        Deze functie verrijkt elk compositie-item, voegt een volgorde toe, verwijdert overbodige composities en filtert specifieke items.

        Args:
            composition_items (list[dict]): Lijst van compositie-items die getransformeerd moeten worden.
            dict_objects (dict): Alle beschikbare objecten voor het verrijken van de compositie-items.
            dict_attributes (dict): Alle attributen voor het verwerken van de compositie-items.

        Returns:
            list[dict]: Een lijst van getransformeerde en gefilterde compositie-items.
        """
        for i, composition_item in enumerate(composition_items):
            composition_item = self._enrich_composition_item(
                composition_item,
                dict_objects=dict_objects,
                dict_attributes=dict_attributes,
            )
            composition_item["Order"] = i
            if "c:ExtendedCompositions" in composition_item:
                composition_item.pop("c:ExtendedCompositions")
            composition_items[i] = composition_item
        composition_items = [
            item
            for item in composition_items
            if item["Entity"]["Stereotype"] != "mdde_ScalarBusinessRule"
        ]
        self.mapping["SourceComposition"] = composition_items
        return composition_items

    def _compositions_remove_mdde_examples(self, compositions: list[dict]) -> dict:
        """Verwijderd de MDDE voorbeeld compositie veronderstelt dat er 1 compositie overblijft

        Args:
            lst_compositions (list[dict]): Composities, inclusief MDDE voorbeeld composities

        Returns:
            dict: Composities zonder de MDDE extensie voorbeelden
        """
        composition = {}
        compositions_new = []
        for item in compositions:
            if "ExtendedBaseCollection.CollectionName" in item:
                if (
                    item["ExtendedBaseCollection.CollectionName"]
                    != "mdde_Mapping_Examples"
                ):
                    compositions_new.append(item)
            else:
                logger.warning(
                    f"Geen 'ExtendedBaseCollection.CollectionName' voor {self.file_pd_ldm}"
                )
        # We assume there is only one composition per mapping, which is why we fill lst
        composition = compositions_new[0] if compositions_new else None
        return composition

    def _enrich_composition_item(
        self, composition: dict, dict_objects: dict, dict_attributes: dict
    ) -> dict:
        """Verrijkt en schoont de compositie

        Args:
            composition (dict): Power Designer LDM compositie object
            dict_objects (dict): Alle Power Designer LDM objecten (Entities, Scalars, Filters en Aggregaten) voor het verrijken van de compositie
            dict_attributes (dict): Alle attributen van het Power Designer LDM document om de composities te verrijken

        Returns:
            dict: Geschoonde en verrijkte versie van de compositie
        """
        composition = self.clean_keys(composition)
        self._set_join_alias_and_type(composition)
        composition = self._composition_entity(
            composition=composition, dict_objects=dict_objects
        )
        join_type = composition.get("JoinType", "")
        if join_type == "APPLY":
            composition = self._handle_apply_type(composition, dict_attributes)
        elif join_type != "FROM":
            composition = self._handle_join_conditions(composition, dict_attributes)
        return composition

    def _handle_apply_type(self, composition: dict, dict_attributes: dict) -> dict:
        """Handelt het verrijken van een compositie af op basis van het apply type.

        Deze functie bepaalt het type business rule en roept de juiste transformatie aan voor filter of scalar business rules.

        Args:
            composition (dict): De compositie die verrijkt moet worden.
            dict_attributes (dict): Alle attributen van het Power Designer LDM document om de composities te verrijken.

        Returns:
            dict: De verrijkte compositie.
        """
        apply_type = composition["Entity"]["Stereotype"]
        if apply_type == "mdde_FilterBusinessRule":
            trf_source_condition = SourceConditionTransform(
                file_pd_ldm=self.file_pd_ldm,
                mapping=self.mapping,
                composition=composition,
            )
            self.composition = trf_source_condition.transform(
                dict_attributes=dict_attributes
            )
            return composition
        elif apply_type == "mdde_ScalarBusinessRule":
            trf_business_rule = BusinessRuleTransform(
                file_pd_ldm=self.file_pd_ldm,
                mapping=self.mapping,
                composition=composition,
            )
            composition_result = trf_business_rule.transform(
                dict_attributes=dict_attributes
            )
            composition |= composition_result
            return composition
        return composition

    def _handle_join_conditions(self, composition: dict, dict_attributes: dict) -> dict:
        """Handelt het verrijken van een compositie af voor join condities.

        Deze functie roept de JoinConditionsTransformer aan om de join condities te transformeren en te verrijken.

        Args:
            composition (dict): De compositie die verrijkt moet worden.
            dict_attributes (dict): Alle attributen van het Power Designer LDM document om de composities te verrijken.

        Returns:
            dict: De verrijkte compositie.
        """
        trf_join_conditions = JoinConditionsTransformer(
            file_pd_ldm=self.file_pd_ldm,
            mapping=self.mapping,
            composition=composition,
        )
        return trf_join_conditions.transform(dict_attributes=dict_attributes)

    def _set_join_alias_and_type(self, composition: dict):
        """Stelt de JoinAlias, JoinAliasName en JoinType in voor de compositie.

        Args:
            composition (dict): De compositie waarvoor de join eigenschappen worden ingesteld.
        """
        composition["JoinAlias"] = composition["Id"]
        if "ExtendedAttributesText" in composition:
            composition["JoinAliasName"] = self._extract_value_from_attribute_text(
                composition["ExtendedAttributesText"],
                preceded_by="mdde_JoinAlias,",
            )
            composition["JoinType"] = self._extract_value_from_attribute_text(
                composition["ExtendedAttributesText"],
                preceded_by="mdde_JoinType,",
            ).upper()
        else:
            logger.warning(
                f"Geen Join type gevonden in de 'ExtendedAttributesText' voor '{composition['Name']}' in {self.file_pd_ldm}"
            )

    def _composition_entity(self, composition: dict, dict_objects: dict) -> dict:
        """Vormt om en verrijkt de compositie met entiteit data

        Args:
            composition (dict): Compositie data
            dict_objects (dict): Alle entiteiten/filters (in- en external)

        Returns:
            dict: Een geschoonde en verrijkte versie van compositie data
        """
        logger.debug(
            f"Start met transformeren entiteit voor compositie '{composition['Name']} for {self.file_pd_ldm}'"
        )

        path_keys_1 = ["c:ExtendedComposition.Content", "o:ExtendedSubObject"]
        path_keys_2 = ["c:ExtendedCollections", "o:ExtendedCollection"]
        if entity := self._get_nested(data=composition, keys=path_keys_1):
            root_data = "c:ExtendedComposition.Content"
        elif entity := self._get_nested(data=composition, keys=path_keys_2):
            root_data = "c:ExtendedCollections"
        elif "c:Content" in composition:
            root_data = "c:Content"
            entity = composition
        else:
            return composition
        entity = self.clean_keys(entity)
        if "c:Content" in entity:
            type_entity = self.determine_reference_type(data=entity["c:Content"])
            id_entity = entity["c:Content"][type_entity]["@Ref"]
            entity = dict_objects[id_entity]
        composition["Entity"] = entity
        composition.pop(root_data)
        return composition

    def _mapping_enrich_datasource(self, dict_datasources: dict) -> None:
        """Verrijkt de mapping met de datasource die als bron is aangewezen voor de mapping
        ten behoeve van het genereren van de DDL en ETL

        Args:
            dict_datasources (dict): dictionary met daarin alle beschikbare datasources
        """
        if "c:DataSource" in self.mapping:
            datasource_alias_id = self.mapping["c:DataSource"]["o:DefaultDataSource"][
                "@Ref"
            ]
            datasource_code = dict_datasources[datasource_alias_id]["Code"]
            self.mapping["DataSource"] = datasource_code
            self.mapping.pop("c:DataSource")
