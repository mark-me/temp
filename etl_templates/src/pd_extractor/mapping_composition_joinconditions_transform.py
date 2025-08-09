import re

from logtools import get_logger

from .base_transformer import BaseTransformer

logger = get_logger(__name__)


class JoinConditionsTransformer(BaseTransformer):
    """Vormt mapping data om en verrijkt dit met entiteit en attribuut data"""

    def __init__(self, file_pd_ldm: str, mapping: dict, composition: dict):
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm
        self.mapping = mapping
        self.composition = composition

    def transform(self, dict_attributes: dict) -> dict:
        """Handelt de verschillende condities van de compositie af, zoals join, source en scalar condities.

        Args:
            composition (dict): De compositie waarvoor de condities worden afgehandeld.
            dict_attributes (dict): Alle attributen (in- en external).
        """
        if "c:ExtendedCompositions" in self.composition:
            join_type = self.composition.get("JoinType", "").upper()
            if join_type not in ["FROM", "APPLY"]:
                self._composition_join_conditions(
                    dict_attributes=dict_attributes
                )
            elif join_type in ["APPLY"]:
                if self.composition["Entity"]["Stereotype"] == "mdde_FilterBusinessRule":
                    self._composition_source_conditions(
                        dict_attributes=dict_attributes
                    )
                if self.composition["Entity"]["Stereotype"] == "mdde_ScalarBusinessRule":
                    composition_result = self._composition_scalar_conditions(
                        dict_attributes=dict_attributes
                    )
                    self.composition |= composition_result
        return self.composition

    def _composition_join_conditions(
        self, dict_attributes: dict
    ) -> None:
        """Schoont en verrijkt data van de join condities van 1 van de composities

        Args:
            composition (dict): Compositie data
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            None
        """
        logger.debug(
            f"Join conditities in {self.composition["Name"]} transformeren voor mapping '{self.mapping['a:Name']} for {self.file_pd_ldm}'"
        )
        lst_conditions = self._extract_conditions_from_composition()
        lst_conditions = self.clean_keys(lst_conditions)

        for i, condition in enumerate(lst_conditions):
            self._process_condition(condition=condition, index=i, dict_attributes=dict_attributes)

        self.composition["JoinConditions"] = lst_conditions
        self.composition.pop("c:ExtendedCompositions")

    def _composition_source_conditions(
        self, dict_attributes: dict
    ):
        """Schoont en verrijkt data van de source (bron) conditie van 1 van de composities

        Args:
            composition (dict): Compositie data
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            None
        """
        logger.debug(
            f"Source conditie transformeren voor compositie  {self.composition['Name']} for {self.file_pd_ldm}"
        )
        lst_conditions = self._extract_source_conditions_from_composition()
        lst_conditions = self.clean_keys(lst_conditions)

        for i, condition in enumerate(lst_conditions):
            self._process_source_condition(condition=condition, index=i, dict_attributes=dict_attributes)
        self.composition["SourceConditions"] = lst_conditions

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
                extended_attr_text,
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
                f"Geen SourceConditionVariable gevonden voor condition {condition.get('Code', '')} in compositie{self.composition.get('Name', '')} voor {self.file_pd_ldm}"
            )
        condition.pop("c:ExtendedCollections")


    def _extract_source_conditions_from_composition(self):
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

    def _composition_scalar_conditions(
        self, dict_attributes: dict
    ) -> None:
        """Schoont en verrijkt data van de scalar condities van 1 van de composities

        Args:
            composition (dict): Compositie data
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            None
        """
        logger.debug(
            f"Source conditions transformeren voor compositie  {self.composition['Name']} voor {self.file_pd_ldm}"
        )
        lst_conditions = self._extract_scalar_conditions_from_composition()
        lst_conditions = self.clean_keys(lst_conditions)

        for i in range(len(lst_conditions)):
            condition = lst_conditions[i]
            condition["Order"] = i
            self._process_scalar_condition(
                condition=condition, dict_attributes=dict_attributes
            )
            lst_conditions[i] = condition
        self.composition["ScalarConditions"] = lst_conditions

        sql_expression = self.composition["Entity"]["SqlExpression"]
        lst_sql_expression_variables = self.composition["Entity"]["SqlExpressionVariables"]
        dict_scalar_conditions = self._build_scalar_conditions_dict(
            lst_scalar_conditions=self.composition["ScalarConditions"]
        )

        sql_expression = self._replace_sql_expression_variables(
            sql_expression=sql_expression,
            lst_sql_expression_variables=lst_sql_expression_variables,
            dict_scalar_conditions=dict_scalar_conditions,
        )

        if sql_expression is not None:
            self.composition["Expression"] = sql_expression
        self.composition.pop("ScalarConditions")

    def _process_scalar_condition(self, condition: dict, dict_attributes: dict):
        """Verwerkt een enkele scalar conditie binnen een compositie.

        Args:
            condition (dict): De conditie die verwerkt wordt.
            dict_attributes (dict): Alle attributen (in- en external).
        """
        lst_components = condition["c:ExtendedCollections"]["o:ExtendedCollection"]
        if isinstance(lst_components, dict):
            lst_components = [lst_components]
        condition["ScalarConditionVariable"] = self._scalar_condition_components(
            lst_components=lst_components, dict_attributes=dict_attributes
        )
        condition.pop("c:ExtendedCollections")

    def _extract_scalar_conditions_from_composition(self) -> list:
        """Haalt de lijst van scalar condities uit de compositie.

        Args:
            composition (dict): De compositie waaruit de scalar condities worden gehaald.

        Returns:
            list: Een lijst met scalar condities uit de compositie.
        """
        lst_conditions = self.composition["c:ExtendedCompositions"]["o:ExtendedComposition"][
            "c:ExtendedComposition.Content"
        ]["o:ExtendedSubObject"]
        if isinstance(lst_conditions, dict):
            lst_conditions = [lst_conditions]
        return lst_conditions

    def _build_scalar_conditions_dict(self, lst_scalar_conditions: list[dict]) -> dict:
        """Bouwt een dictionary van scalar condities op basis van hun Id.

        Args:
            lst_scalar_conditions (list[dict]): Lijst van scalar conditie dictionaries.

        Returns:
            dict: Dictionary met scalar conditie Id's als sleutel en relevante variabelen als waarde.
        """
        dict_scalar_conditions = {
            scalar_condition["Id"]: {
                "Id": scalar_condition["Id"],
                "TargetVariable": scalar_condition["ScalarConditionVariable"][
                    "AttributeChild"
                ],
                "SourceVariable": scalar_condition["ScalarConditionVariable"][
                    "SourceAttribute"
                ],
            }
            for scalar_condition in lst_scalar_conditions
        }
        return dict_scalar_conditions

    def _extract_conditions_from_composition(self):
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

    def _process_condition(
        self, condition: dict, index: int, dict_attributes: dict
    ):
        """Verwerkt een enkele join conditie binnen een compositie.

        Deze functie stelt de volgorde, operator, parent literal en componenten in voor een join conditie.

        Args:
            condition (dict): De conditie die verwerkt wordt.
            index (int): De volgorde van de conditie in de lijst.
            composition (dict): De compositie waartoe de conditie behoort.
            dict_attributes (dict): Alle attributen (in- en external).
        """
        condition["Order"] = index
        logger.debug(
            f"Join conditities transformeren voor {index} '{condition['Name']}' voor {self.file_pd_ldm}"
        )
        self._set_condition_operator_and_literal(condition)
        self._set_condition_components(condition=condition, dict_attributes=dict_attributes)

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

    def _set_condition_components(
        self, condition: dict, dict_attributes: dict
    ):
        """Stelt de componenten van een join conditie in voor een gegeven conditie.

        Deze functie haalt de componenten op uit de conditie, verwerkt deze en voegt ze toe aan de conditie.

        Args:
            condition (dict): De conditie waarvoor de componenten worden ingesteld.
            composition (dict): De compositie waartoe de conditie behoort.
            dict_attributes (dict): Alle attributen (in- en external).
        """
        if "c:ExtendedCollections" not in condition:
            logger.warning(
                f"Er zijn geen c:ExtendedCollections, controleer model voor ongeldige mapping in {self.file_pd_ldm} "
            )
        lst_components = condition["c:ExtendedCollections"]["o:ExtendedCollection"]
        lst_components = (
            [lst_components] if isinstance(lst_components, dict) else lst_components
        )
        condition["JoinConditionComponents"] = self._join_condition_components(
            lst_components=lst_components,
            dict_attributes=dict_attributes,
            alias_child=self.composition["Id"],
        )
        condition.pop("c:ExtendedCollections")

    def _join_condition_components(
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
        """Extraheert child, parent en parent alias uit de join componenten."""
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


    def _replace_sql_expression_variables(
        self,
        sql_expression: str,
        lst_sql_expression_variables: tuple,
        dict_scalar_conditions: dict,
    ) -> str:
        """
        Vervangt variabelen in een SQL-expressie door de bijbehorende source variabelen uit de scalar condities.

        Deze functie zoekt naar target variabelen in de SQL-expressie en vervangt deze door de juiste source variabelen,
        op basis van de mapping in dict_scalar_conditions.

        Args:
            sql_expression (str): De SQL-expressie waarin variabelen vervangen moeten worden.
            lst_sql_expression_variables (tuple): Tuple van variabelen die in de expressie voorkomen.
            dict_scalar_conditions (dict): Dictionary met target en source variabelen per conditie.

        Returns:
            str: De aangepaste SQL-expressie met vervangen variabelen.
        """
        for condition in dict_scalar_conditions:
            target_variable = dict_scalar_conditions[condition][
                "TargetVariable"
            ].upper()
            for variable in lst_sql_expression_variables:
                variable_compare = variable[1:]
                if target_variable == variable_compare:
                    source_variable = dict_scalar_conditions[condition][
                        "SourceVariable"
                    ]
                    pattern = r"" + variable + r"\b"
                    sql_expression = re.sub(pattern, source_variable, sql_expression)
                else:
                    logger.info(
                        f"Er is geen sql_expression gevonden voor {self.file_pd_ldm}"
                    )
        return sql_expression


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
                logger.debug(
                    f"SourceConditionAttribute alias toegevoegd voor {self.file_pd_ldm}"
                )
                alias_parent = self._get_nested(
                    data=component, keys=["c:Content", "o:ExtendedSubObject", "@Ref"]
                )
            elif component["Name"] == "mdde_ParentAttribute":
                logger.debug(
                    f"SourceConditionAttribute alias toegevoegd voor {self.file_pd_ldm}"
                )
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
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
        for component in lst_components:
            if component["Name"] == "mdde_ChildAttribute":
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
                id_attr = component["c:Content"][type_entity]["@Ref"]
                return dict_attributes[id_attr].copy()
        return {}
