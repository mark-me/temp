import re

from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class BusinessRuleTransform(BaseTransformer):
    """Vormt mapping data om en verrijkt dit met entiteit en attribuut data"""

    def __init__(self, file_pd_ldm: str, mapping: dict, composition: dict):
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm
        self.mapping = mapping
        self.composition = composition

    def transform(self, dict_attributes: dict) -> dict:
        """Transformeert de scalar condities en SQL-expressie in de compositie en verrijkt deze met entiteit en attribuut data.

        Deze functie verwerkt alle scalar condities voor de huidige mapping, vervangt variabelen in de SQL-expressie en werkt de compositie bij met de getransformeerde expressie.

        Args:
            dict_attributes (dict): Alle attributen (intern en extern) die gebruikt worden voor verrijking.

        Returns:
            dict: De bijgewerkte compositie dictionary met getransformeerde expressie.
        """
        self._process_all_scalar_conditions(dict_attributes)
        self._process_sql_expression()
        return self.composition

    def _process_all_scalar_conditions(self, dict_attributes: dict) -> None:
        """Verwerkt alle scalar condities en voegt deze toe aan de compositie.

        Deze functie haalt alle scalar condities op, verwerkt ze en voegt ze toe aan de compositie.

        Args:
            dict_attributes (dict): Alle attributen (intern en extern) die gebruikt worden voor verrijking.
        """
        lst_conditions = self._extract_scalar_conditions()
        lst_conditions = self.clean_keys(lst_conditions)
        for i, condition in enumerate(lst_conditions):
            condition["Order"] = i
            self._process_scalar_condition(
                condition=condition, dict_attributes=dict_attributes
            )
            lst_conditions[i] = condition
        self.composition["ScalarConditions"] = lst_conditions

    def _process_sql_expression(self) -> None:
        """Vervangt variabelen in de SQL-expressie en werkt de compositie bij met de aangepaste expressie.

        Deze functie haalt de SQL-expressie en variabelen op, vervangt de variabelen met de juiste waarden
        uit de scalar condities, en slaat het resultaat op in de compositie.

        Returns:
            None
        """
        # Retrieves the SQL expression and its variables from the composition.
        sql_expression = self.composition["Entity"]["SqlExpression"]
        lst_sql_expression_variables = self.composition["Entity"][
            "SqlExpressionVariables"
        ]
        dict_scalar_conditions = self._create_scalar_conditions_lookup(
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

    def _extract_scalar_conditions(self) -> list[dict]:
        """Haalt de lijst van scalar condities uit de compositie.

        Args:
            composition (dict): De compositie waaruit de scalar condities worden gehaald.

        Returns:
            list[dict]: Een lijst met scalar condities uit de compositie.
        """
        lst_conditions = self.composition["c:ExtendedCompositions"][
            "o:ExtendedComposition"
        ]["c:ExtendedComposition.Content"]["o:ExtendedSubObject"]
        if isinstance(lst_conditions, dict):
            lst_conditions = [lst_conditions]
        return lst_conditions

    def _process_scalar_condition(self, condition: dict, dict_attributes: dict) -> None:
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
                    pattern = f"{variable}" + r"\b"
                    sql_expression = re.sub(pattern, source_variable, sql_expression)
                else:
                    logger.info(
                        f"Er is geen sql_expression gevonden voor {self.file_pd_ldm}"
                    )
        return sql_expression

    def _create_scalar_conditions_lookup(
        self, lst_scalar_conditions: list[dict]
    ) -> dict:
        """Maakt een lookup dictionary van scalar condities voor snelle toegang tot target en source variabelen.

        Deze functie zet een lijst van scalar condities om naar een dictionary met Id, TargetVariable en SourceVariable per conditie.

        Args:
            lst_scalar_conditions (list[dict]): Lijst van scalar conditie dictionaries.

        Returns:
            dict: Dictionary met per conditie de Id, TargetVariable en SourceVariable.
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

    def _scalar_condition_components(
        self, lst_components: list[dict], dict_attributes: dict
    ) -> dict:
        """Vormt om, schoont en verrijkt component data van 1 scalar conditie

        Args:
            lst_components (list[dict]): scalar conditie component
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Geschoonde, omgevormde en verrijkte scalar conditie component data
        """
        dict_scalar_condition_attribute = {}
        dict_child = self._get_scalar_child_attribute(
            lst_components=lst_components, dict_attributes=dict_attributes
        )
        dict_parent, alias_parent = self._get_scalar_parent_attribute_and_alias(
            lst_components=lst_components, dict_attributes=dict_attributes
        )
        if len(dict_parent) > 0:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_scalar_condition_attribute["SourceAttribute"] = (
                dict_parent["EntityAlias"] + "." + dict_parent["Code"]
            )
        if len(dict_child) > 0:
            dict_scalar_condition_attribute["AttributeChild"] = dict_child["Code"]
        return dict_scalar_condition_attribute

    def _extract_child_attribute(self, component: dict, dict_attributes: dict) -> dict:
        """Haalt het child attribute dictionary op uit het opgegeven component.

        Deze functie zoekt het child attribute op dat wordt gerefereerd in het component en retourneert een kopie van het dictionary uit dict_attributes.

        Args:
            component (dict): Het component dat de child attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het child attribute dictionary.
        """
        type_entity = self.determine_reference_type(data=component["c:Content"])
        id_attr = component["c:Content"][type_entity]["@Ref"]
        return dict_attributes[id_attr].copy()

    def _get_scalar_child_attribute(
        self, lst_components: list[dict], dict_attributes: dict
    ) -> dict:
        """Haalt het child attribute dictionary op uit de scalar conditie componenten.

        Args:
            lst_components (list[dict]): Lijst van componenten.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het child attribute dictionary, of leeg dict als niet gevonden.
        """
        lst_components = self.clean_keys(content=lst_components)
        return next(
            (
                self._extract_child_attribute(
                    component=component, dict_attributes=dict_attributes
                )
                for component in lst_components
                if component["Name"] == "mdde_ChildAttribute"
            ),
            {},
        )

    def _get_scalar_parent_attribute_and_alias(
        self, lst_components: list[dict], dict_attributes: dict
    ) -> tuple:
        """Haalt het parent attribute dictionary en alias op uit de scalar conditie componenten.

        Args:
            lst_components (list[dict]): Lijst van componenten.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            tuple: (parent attribute dict, alias_parent of None)
        """
        dict_parent = {}
        alias_parent = None
        lst_components = self.clean_keys(lst_components)
        for component in lst_components:
            if component["Name"] == "mdde_ParentSourceObject":
                alias_parent = component["c:Content"]["o:ExtendedSubObject"]["@Ref"]
            elif component["Name"] == "mdde_ParentAttribute":
                dict_parent = self._extract_parent_attribute(
                    component=component, dict_attributes=dict_attributes
                )
        return dict_parent, alias_parent

    def _extract_parent_attribute(self, component: dict, dict_attributes: dict) -> dict:
        """Haalt het parent attribute dictionary op uit het opgegeven component.

        Deze functie zoekt het parent attribute op dat wordt gerefereerd in het component en retourneert
        een kopie van het dictionary uit dict_attributes.

        Args:
            component (dict): Het component dat de parent attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het parent attribute dictionary.
        """
        logger.debug(f"ScalarConditionAttribute toegevoegd voor {self.file_pd_ldm}")
        type_entity = self.determine_reference_type(data=component["c:Content"])
        id_attr = component["c:Content"][type_entity]["@Ref"]
        return dict_attributes[id_attr].copy()
