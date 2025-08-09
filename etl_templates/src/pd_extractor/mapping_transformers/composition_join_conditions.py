from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class JoinConditionsTransformer(BaseTransformer):
    """Vormt mapping data om en verrijkt dit met entiteit en attribuut data"""

    def __init__(self, file_pd_ldm: str, mapping: dict, composition: dict):
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm
        self.mapping = mapping
        self.composition = composition

    def transform(self, dict_attributes: dict) -> dict:
        """Transforms join conditions in the composition and enriches them with entity and attribute data.

        This method processes all join conditions for the current mapping and updates the composition accordingly.

        Args:
            dict_attributes (dict): All attributes (internal and external) used for enrichment.

        Returns:
            dict: The updated composition dictionary with transformed join conditions.
        """
        lst_conditions = self._get_conditions()
        lst_conditions = self.clean_keys(lst_conditions)

        for i, condition in enumerate(lst_conditions):
            self._process_condition(
                condition=condition, index=i, dict_attributes=dict_attributes
            )

        self.composition["JoinConditions"] = lst_conditions
        self.composition.pop("c:ExtendedCompositions")
        return self.composition

    def _get_conditions(self):
        """Haalt de lijst van condities uit de compositie.

        Deze functie retourneert alle condities die aanwezig zijn in de opgegeven compositie.

        Returns:
            list: Een lijst met condities uit de compositie.
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

    def _process_condition(self, condition: dict, index: int, dict_attributes: dict):
        """Verwerkt een enkele join conditie binnen een compositie.

        Deze functie stelt de volgorde, operator, parent literal en componenten in voor een join conditie.

        Args:
            condition (dict): De conditie die verwerkt wordt.
            index (int): De volgorde van de conditie in de lijst.
            composition (dict): De compositie waartoe de conditie behoort.
            dict_attributes (dict): Alle attributen (in- en external).
        """
        condition["Order"] = index
        self._set_condition_operator_and_literal(condition)
        self._set_condition_components(
            condition=condition, dict_attributes=dict_attributes
        )

    def _set_condition_operator_and_literal(self, condition: dict):
        """Stelt de operator en parent literal in voor een join conditie.

        Deze functie bepaalt de operator en parent literal op basis van de attributen van de conditie en voegt deze toe aan de conditie.

        Args:
            condition (dict): De conditie waarvoor de operator en parent literal worden ingesteld.
        """
        condition_operator = "="
        parent_literal = ""
        if "ExtendedAttributesText" in condition:
            condition_operator = self._extract_value_from_attribute_text(
                condition["ExtendedAttributesText"],
                preceded_by="mdde_JoinOperator,",
            )
            parent_literal = self._extract_value_from_attribute_text(
                condition["ExtendedAttributesText"],
                preceded_by="mdde_ParentLiteralValue,",
            )
        condition["Operator"] = "=" if condition_operator == "" else condition_operator
        condition["ParentLiteral"] = parent_literal

    def _set_condition_components(self, condition: dict, dict_attributes: dict):
        """Stelt de componenten van een join conditie in voor een gegeven conditie.

        Deze functie haalt de componenten op uit de conditie, verwerkt deze en voegt ze toe aan de conditie.

        Args:
            condition (dict): De conditie waarvoor de componenten worden ingesteld.
            composition (dict): De compositie waartoe de conditie behoort.
            dict_attributes (dict): Alle attributen (in- en external).
        """
        if "c:ExtendedCollections" not in condition:
            logger.warning(
                f"Er zijn geen c:ExtendedCollections, controleer model voor ongeldige mapping {self.mapping["a:Name"]} in {self.file_pd_ldm} "
            )
        lst_components = condition["c:ExtendedCollections"]["o:ExtendedCollection"]
        lst_components = (
            [lst_components] if isinstance(lst_components, dict) else lst_components
        )
        condition["JoinConditionComponents"] = self._transform_join_condition_components(
            lst_components=lst_components,
            dict_attributes=dict_attributes,
            alias_child=self.composition["Id"],
        )
        condition.pop("c:ExtendedCollections")

    def _transform_join_condition_components(
        self, lst_components: list, dict_attributes: dict, alias_child: str
    ) -> dict:
        """Vormt om, schoont en verrijkt component data van 1 join conditie

        Args:
            lst_components (list): Join conditie componenten
            dict_attributes (dict): Alle attributes (in- en external)
            alias_child (str): De door Power Designer gegenereerde id voor het component (JOIN) van de compositie

        Returns:
            dict: Geschoonde, omgevormde en verrijkte data van het join conditie component
        """
        dict_components = {}
        dict_child, dict_parent, alias_parent = self._extract_join_components(
            lst_components, dict_attributes
        )
        if len(dict_parent) > 0:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_components["AttributeParent"] = dict_parent
        if len(dict_child) > 0:
            dict_child.update({"EntityAlias": alias_child})
            dict_components["AttributeChild"] = dict_child
        return dict_components

    def _extract_join_components(self, lst_components: list, dict_attributes: dict):
        """Extraheert het child attribute, parent attribute en parent alias uit join conditie componenten.

        Deze methode verwerkt een lijst van join conditie componenten en retourneert de relevante child en
        parent attribute dictionaries, evenals de parent alias indien aanwezig.

        Args:
            lst_components (list): De lijst van join conditie componenten.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            tuple: Een tuple met het child attribute dict, parent attribute dict en parent alias.
        """
        dict_child = {}
        dict_parent = {}
        alias_parent = None
        lst_components = self.clean_keys(lst_components)
        for component in lst_components:
            type_component = component["Name"]
            if type_component == "mdde_ChildAttribute":
                dict_child = self._extract_join_child_attribute(
                    component, dict_attributes
                )
            elif type_component == "mdde_ParentSourceObject":
                alias_parent = self._extract_join_parent_source_object(component)
            elif type_component == "mdde_ParentAttribute":
                dict_parent = self._extract_join_parent_attribute(
                    component, dict_attributes
                )
            else:
                logger.warning(
                    f"Ongeldige join item in conditie '{type_component}' for {self.file_pd_ldm}"
                )
        return dict_child, dict_parent, alias_parent

    def _extract_join_child_attribute(
        self, component: dict, dict_attributes: dict
    ) -> dict:
        """Haalt het child attribute dictionary op voor een join conditie component.

        Args:
            component (dict): Het component dat de child attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het child attribute dictionary.
        """
        logger.debug(f"Child attribute toegevoegd voor {self.file_pd_ldm}")
        type_entity = [
            value
            for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
            if value in component["c:Content"]
        ][0]
        id_attr = component["c:Content"][type_entity]["@Ref"]
        return dict_attributes[id_attr].copy()

    def _extract_join_parent_source_object(self, component: dict) -> str:
        """Haalt de alias van het parent source object op voor een join conditie component.

        Args:
            component (dict): Het component dat de parent source object referentie bevat.

        Returns:
            str: De alias van het parent source object.
        """
        path_keys = ["c:Content", "o:ExtendedSubObject", "@Ref"]
        logger.debug(f"Parent entity alias toegevoegd voor {self.file_pd_ldm}")
        self._get_nested(data=component, keys=path_keys)
        parent_alias = self._get_nested(data=component, keys=path_keys)
        if not parent_alias:
            logger.error(
                f"Kan geen parent alias vinden voor een mapping in {self.file_pd_ldm}"
            )
        return parent_alias

    def _extract_join_parent_attribute(
        self, component: dict, dict_attributes: dict
    ) -> dict:
        """Haalt het parent attribute dictionary op voor een join conditie component.

        Args:
            component (dict): Het component dat de parent attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het parent attribute dictionary.
        """
        if content := component.get("c:Content"):
            logger.debug(f"Parent attribute toegevoegd voor {self.file_pd_ldm}")
            type_entity = [
                value
                for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                if value in content
            ][0]
            id_attr = content[type_entity]["@Ref"]
            return dict_attributes[id_attr].copy()
