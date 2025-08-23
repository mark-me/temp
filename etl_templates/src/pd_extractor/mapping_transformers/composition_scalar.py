import re

from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class ScalarTransform(BaseTransformer):
    """Transformeert en verrijkt scalars en SQL-expressies in een compositie.

    Deze klasse verwerkt scalars, koppelt attributen en vervangt variabelen in SQL-expressies
    binnen een gegeven compositie.
    """

    def __init__(self, file_pd_ldm: str, mapping: dict, composition: dict):
        """Initialiseert de ScalarTransform met het opgegeven LDM-bestand, mapping en compositie.

        Deze constructor slaat het pad naar het LDM-bestand, de mapping en de compositie op als attributen van de instantie.

        Args:
            file_pd_ldm (str): Pad naar het Power Designer LDM-bestand.
            mapping (dict): Mapping dictionary met scalars.
            composition (dict): Compositie dictionary die getransformeerd wordt.
        """
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm
        self.mapping = mapping
        self.composition = composition

    def transform(self, dict_attributes: dict) -> dict:
        """Transformeert de s en SQL-expressie in de compositie en verrijkt deze met entiteit en attribuut data.

        Deze functie verwerkt alle scalars voor de huidige mapping,
        vervangt variabelen in de SQL-expressie en werkt de compositie bij met de getransformeerde expressie.

        Args:
            dict_attributes (dict): Alle attributen (intern en extern) die gebruikt worden voor verrijking.

        Returns:
            dict: De bijgewerkte compositie dictionary met getransformeerde expressie.
        """
        self._process_all_scalars(dict_attributes)
        self._process_sql_expression()
        return self.composition

    def _process_all_scalars(self, dict_attributes: dict) -> None:
        """Verwerkt alle scalars in de compositie en verrijkt deze met component data.

        Deze functie haalt alle scalars op, verrijkt ze met attributen en voegt ze toe aan de compositie.

        Args:
            dict_attributes (dict): Alle attributen (intern en extern) die gebruikt worden voor verrijking.
        """
        scalars = self._get_scalars()
        scalars = self.clean_keys(scalars)
        for i, scalar in enumerate(scalars):
            scalar["Order"] = i
            self._process_scalar(scalar=scalar, dict_attributes=dict_attributes)
            scalars[i] = scalar
        self.composition["ScalarConditions"] = scalars

    def _get_scalars(self) -> list[dict]:
        """Haalt alle scalars uit de compositie.

        Deze functie zoekt in de compositie naar de scalars en retourneert deze als een lijst van dictionaries.

        Returns:
            list[dict]: Lijst van scalars uit de compositie.
        """
        path_keys = [
            "c:ExtendedCompositions",
            "o:ExtendedComposition",
            "c:ExtendedComposition.Content",
            "o:ExtendedSubObject",
        ]
        scalars = self._get_nested(data=self.composition, keys=path_keys)
        scalars = [scalars] if isinstance(scalars, dict) else scalars
        return scalars

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
        dict_scalar_conditions = self._create_scalar_lookup(
            scalars=self.composition["ScalarConditions"]
        )
        sql_expression = self._replace_sql_expression_variables(
            sql_expression=sql_expression,
            sql_expression_variables=lst_sql_expression_variables,
            dict_scalar_conditions=dict_scalar_conditions,
        )
        if sql_expression is not None:
            self.composition["Expression"] = sql_expression
        self.composition.pop("ScalarConditions")

    def _process_scalar(self, scalar: dict, dict_attributes: dict) -> None:
        """Verwerkt één scalar en verrijkt deze met component data.

        Deze functie haalt de componenten van de scalar op,
        verrijkt deze met attributen en voegt het resultaat toe aan de conditie.

        Args:
            scalar (dict): De scalar die verwerkt moet worden.
            dict_attributes (dict): Alle attributen (intern en extern) die gebruikt worden voor verrijking.
        """
        components = self._get_nested(
            data=scalar, keys=["c:ExtendedCollections", "o:ExtendedCollection"]
        )
        components = [components] if isinstance(components, dict) else components
        scalar["ScalarConditionVariable"] = self._scalar_components(
            components=components, dict_attributes=dict_attributes
        )
        scalar.pop("c:ExtendedCollections")

    def _scalar_components(self, components: list[dict], dict_attributes: dict) -> dict:
        """Bepaalt de componenten van een scalar en koppelt deze aan de juiste attributen.

        Deze functie haalt het child en parent attribute op uit de componenten en bouwt een
        dictionary met de relevante attributen.

        Args:
            components (list[dict]): Lijst van componenten van de scalar.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Dictionary met de gekoppelde source en child attributen.
        """

        dict_scalar_attribute = {}
        dict_child = self._get_child_attribute(
            components=components, dict_attributes=dict_attributes
        )
        dict_parent, alias_parent = self._get_parent_attribute_and_alias(
            components=components, dict_attributes=dict_attributes
        )
        if len(dict_parent) > 0:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_scalar_attribute["SourceAttribute"] = (
                dict_parent["EntityAlias"] + "." + dict_parent["Code"]
            )
        if len(dict_child) > 0:
            dict_scalar_attribute["AttributeChild"] = dict_child["Code"]
        return dict_scalar_attribute

    def _replace_sql_expression_variables(
        self,
        sql_expression: str,
        sql_expression_variables: tuple,
        dict_scalar_conditions: dict,
    ) -> str:
        """
        Vervangt variabelen in een SQL-expressie door de bijbehorende source variabelen uit de scalar condities.

        Deze functie zoekt naar target variabelen in de SQL-expressie en vervangt deze door de juiste source variabelen,
        op basis van de mapping in dict_scalar_conditions.

        Args:
            sql_expression (str): De SQL-expressie waarin variabelen vervangen moeten worden.
            sql_expression_variables (tuple): Tuple van variabelen die in de expressie voorkomen.
            dict_scalar_conditions (dict): Dictionary met target en source variabelen per conditie.

        Returns:
            str: De aangepaste SQL-expressie met vervangen variabelen.
        """

        def _get_target_variable(condition: str) -> str:
            return dict_scalar_conditions[condition]["TargetVariable"].upper()

        def _get_source_variable(condition: str) -> str:
            return dict_scalar_conditions[condition]["SourceVariable"]

        def _find_matching_variable(
            target_variable: str, sql_expression_variables: tuple
        ) -> str | None:
            for variable in sql_expression_variables:
                variable_compare = variable[1:]
                if target_variable == variable_compare:
                    return variable
            return None

        for condition in dict_scalar_conditions:
            target_variable = _get_target_variable(condition)
            variable = _find_matching_variable(
                target_variable, sql_expression_variables
            )
            if variable is not None:
                source_variable = _get_source_variable(condition)
                pattern = f"{variable}" + r"\b"
                sql_expression = re.sub(pattern, source_variable, sql_expression)
            else:
                logger.info(
                    f"Er is geen sql_expression gevonden voor {self.file_pd_ldm}"
                )
        return sql_expression

    def _create_scalar_lookup(self, scalars: list[dict]) -> dict:
        """Maakt een lookup dictionary aan voor scalars op basis van scalar condities.

        Deze functie genereert een dictionary waarin elke scalar wordt gekoppeld aan zijn Id, target en source variabelen.

        Args:
            scalars (list[dict]): Lijst van scalar dictionaries.

        Returns:
            dict: Een dictionary met scalar lookup per Id.
        """
        dict_scalars = {
            scalar["Id"]: {
                "Id": scalar["Id"],
                "TargetVariable": scalar["ScalarConditionVariable"]["AttributeChild"],
                "SourceVariable": scalar["ScalarConditionVariable"]["SourceAttribute"],
            }
            for scalar in scalars
        }
        return dict_scalars

    def _get_child_attribute(self, component: dict, dict_attributes: dict) -> dict:
        # sourcery skip: class-extract-method
        """Haalt het child attribute dictionary op uit het opgegeven component.

        Deze functie zoekt het child attribute op dat wordt gerefereerd in het component en
        retourneert een kopie van het dictionary uit dict_attributes.

        Args:
            component (dict): Het component dat de child attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het child attribute dictionary.
        """
        type_entity = self.determine_reference_type(data=component["c:Content"])
        id_attr = component["c:Content"][type_entity]["@Ref"]
        return dict_attributes[id_attr].copy()

    def _get_child_attribute(
        self, components: list[dict], dict_attributes: dict
    ) -> dict:
        """Zoekt en retourneert het child attribute uit de componentenlijst.

        Deze functie doorzoekt de lijst van componenten naar het child attribute en retourneert het bijbehorende dictionary.

        Args:
            components (list[dict]): Lijst van componenten waarin gezocht wordt.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Het gevonden child attribute dictionary, of een lege dictionary als niet gevonden.
        """
        components = self.clean_keys(content=components)
        return next(
            (
                self._get_child_attribute(
                    component=component, dict_attributes=dict_attributes
                )
                for component in components
                if component["Name"] == "mdde_ChildAttribute"
            ),
            {},
        )

    def _get_parent_attribute_and_alias(
        self, components: list[dict], dict_attributes: dict
    ) -> tuple:
        """Haalt het parent attribute dictionary en alias op uit de scalar componenten.

        Args:
            components (list[dict]): Lijst van componenten.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            tuple: (parent attribute dict, alias_parent of None)
        """
        dict_parent = {}
        alias_parent = None
        components = self.clean_keys(components)
        for component in components:
            if component["Name"] == "mdde_ParentSourceObject":
                alias_parent = component["c:Content"]["o:ExtendedSubObject"]["@Ref"]
            elif component["Name"] == "mdde_ParentAttribute":
                dict_parent = self._get_parent_attribute(
                    component=component, dict_attributes=dict_attributes
                )
        return dict_parent, alias_parent

    def _get_parent_attribute(self, component: dict, dict_attributes: dict) -> dict:
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
