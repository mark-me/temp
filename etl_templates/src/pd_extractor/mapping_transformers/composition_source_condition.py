from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class SourceConditionTransform(BaseTransformer):
    """Vormt mapping data om en verrijkt dit met entiteit en attribuut data"""

    def __init__(self, file_pd_ldm: str, mapping: dict, composition: dict):
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm
        self.mapping = mapping
        self.composition = composition

    def transform(self, dict_attributes: dict):
        """Transformeert de source condities in de compositie en verrijkt deze met entiteit en attribuut data.

        Deze functie verwerkt alle source condities voor de huidige mapping en werkt de compositie bij met de getransformeerde condities.

        Args:
            dict_attributes (dict): Alle attributen (intern en extern) die gebruikt worden voor verrijking.

        Returns:
            dict: De bijgewerkte compositie dictionary met getransformeerde source condities.
        """
        lst_conditions = self._extract_source_conditions()
        lst_conditions = self.clean_keys(lst_conditions)
        for i, condition in enumerate(lst_conditions):
            self._process_source_condition(
                condition=condition, index=i, dict_attributes=dict_attributes
            )
        self.composition["SourceConditions"] = lst_conditions
        return self.composition

    def _extract_source_conditions(self):
        """Haalt de lijst van source condities uit de compositie.

        Args:
            composition (dict): De compositie waaruit de source condities worden gehaald.

        Returns:
            list: Een lijst met source condities uit de compositie.
        """
        path_keys = [
            "c:ExtendedCompositions",
            "o:ExtendedComposition",
            "c:ExtendedComposition.Content",
            "o:ExtendedSubObject",
        ]
        lst_conditions = self._get_nested(data=self.composition, keys=path_keys)
        lst_conditions = (
            [lst_conditions] if isinstance(lst_conditions, dict) else lst_conditions
        )
        return lst_conditions

    def _process_source_condition(
        self, index: int, dict_attributes: dict, condition: dict
    ) -> None:
        """Verwerkt een enkele source conditie binnen een compositie.

        Args:
            condition (dict): De conditie die verwerkt wordt.
            index (int): De volgorde van de conditie in de lijst.
            dict_attributes (dict): Alle attributen (in- en external).
            composition (dict): De compositie waartoe de conditie behoort.
        """
        condition["Order"] = index
        parent_literal = ""
        if extended_attr_text := self._get_nested(
            data=condition, keys=["ExtendedAttributesText"]
        ):
            parent_literal = self._extract_value_from_attribute_text(
                text=extended_attr_text,
                preceded_by="mdde_ParentLiteralValue,",
            )
        lst_components = condition["c:ExtendedCollections"]["o:ExtendedCollection"]
        lst_components = (
            [lst_components] if isinstance(lst_components, dict) else lst_components
        )
        source_condition_variable = self._source_condition_components(
            lst_components=lst_components,
            dict_attributes=dict_attributes,
            parent_literal=parent_literal,
        )
        if len(source_condition_variable) > 0:
            condition["SourceConditionVariable"] = source_condition_variable
        elif parent_literal != "":
            condition["SourceConditionVariable"] = parent_literal
        else:
            logger.warning(
                f"Geen SourceConditionVariable gevonden voor condition {condition.get('Code', '')} in mapping {self.mapping['a:Name']} voor {self.file_pd_ldm}"
            )
        condition.pop("c:ExtendedCollections")

    def _source_condition_components(
        self, lst_components: list[dict], dict_attributes: dict, parent_literal: str
    ) -> dict:
        """Vormt om, schoont en verrijkt component data van 1 source conditie

        Args:
            lst_components (list[dict]): source conditie component
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Geschoonde, omgevormde en verrijkte source conditie component data
        """
        dict_source_condition_attribute = {}
        dict_parent, alias_parent = self._get_source_parent_attribute_and_alias(
            lst_components=lst_components, dict_attributes=dict_attributes
        )
        dict_child = self._get_source_child_attribute(
            lst_components=lst_components, dict_attributes=dict_attributes
        )
        if len(dict_parent) > 0:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_source_condition_attribute["SourceAttribute"] = dict_parent
        if parent_literal != "" and len(dict_child) > 0:
            dict_source_condition_attribute["SourceAttribute"] = dict_child
            dict_source_condition_attribute["SourceAttribute"]["Expression"] = (
                parent_literal
            )
        return dict_source_condition_attribute

    def _get_source_parent_attribute_and_alias(
        self, lst_components: list, dict_attributes: dict
    ) -> tuple:
        """Haalt het parent attribute dictionary en alias op uit de source conditie componenten.

        Args:
            lst_components (list): Lijst van componenten.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            tuple: (parent attribute dict, alias_parent of None)
        """
        dict_parent = {}
        alias_parent = None
        lst_components = self.clean_keys(lst_components)
        for component in lst_components:
            if component["Name"] == "mdde_ParentSourceObject":
                alias_parent = self._get_nested(
                    data=component, keys=["c:Content", "o:ExtendedSubObject", "@Ref"]
                )
            elif component["Name"] == "mdde_ParentAttribute":
                type_entity = self.determine_reference_type(data=component["c:Content"])
                id_attr = component["c:Content"][type_entity]["@Ref"]
                dict_parent = dict_attributes[id_attr].copy()
        return dict_parent, alias_parent

    def _get_source_child_attribute(
        self, lst_components: list[dict], dict_attributes: dict
    ) -> dict:
        """Haalt het child attribute dictionary op uit de source conditie componenten.

        Args:
            lst_components (list[dict]): Lijst van componenten.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het child attribute dictionary, of leeg dict als niet gevonden.
        """
        lst_components = self.clean_keys(lst_components)
        lst_components = [x for x in lst_components if x["Name"] == "mdde_ChildAttribute"]
        for component in lst_components:
            type_entity = self.determine_reference_type(data=component["c:Content"])
            id_attr = component["c:Content"][type_entity]["@Ref"]
            return dict_attributes[id_attr].copy()
        return {}
