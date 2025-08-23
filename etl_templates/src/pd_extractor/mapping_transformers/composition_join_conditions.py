import copy

from logtools import get_logger

from ..base_transformer import BaseTransformer

logger = get_logger(__name__)


class JoinConditionsTransformer(BaseTransformer):
    """Transformeert en verrijkt join condities in een compositie.

    Deze klasse verwerkt join condities, koppelt attributen en verrijkt de compositie met
    getransformeerde join condities.
    """

    def __init__(self, file_pd_ldm: str, mapping: dict, composition: dict):
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm
        self.mapping = mapping
        self.composition = composition

    def transform(self, dict_attributes: dict) -> dict:
        """Transformeert de join condities in de compositie en verrijkt deze met entiteit en attribuut data.

        Deze functie verwerkt alle join condities voor de huidige mapping en werkt de compositie bij met de getransformeerde condities.

        Args:
            dict_attributes (dict): Alle attributen (intern en extern) die gebruikt worden voor verrijking.

        Returns:
            dict: De bijgewerkte compositie dictionary met getransformeerde join condities.
        """
        if conditions := self._get_conditions():
            conditions = self.clean_keys(conditions)
            for i, condition in enumerate(conditions):
                self._process_condition(
                    condition=condition, index=i, dict_attributes=dict_attributes
                )
            self.composition["JoinConditions"] = conditions
            self.composition.pop("c:ExtendedCompositions", None)
        return self.composition

    def _get_conditions(self) -> list:
        """Haalt de lijst van join condities uit de compositie.

        Deze functie retourneert alle condities die aanwezig zijn in de opgegeven compositie.

        Returns:
            list: Een lijst met join condities uit de compositie.
        """
        path_keys = [
            "c:ExtendedCompositions",
            "o:ExtendedComposition",
            "c:ExtendedComposition.Content",
            "o:ExtendedSubObject",
        ]
        conditions = self._get_nested(data=self.composition, keys=path_keys)
        if not conditions:
            logger.error(
                f"Kan geen join condities vinden in mapping '{self.mapping['Name']}' in '{self.file_pd_ldm}'"
            )
            return []
        conditions = [conditions] if isinstance(conditions, dict) else conditions
        return conditions

    def _process_condition(self, condition: dict, index: int, dict_attributes: dict):
        """Verwerkt een enkele join conditie binnen een compositie.

        Deze functie stelt de volgorde, operator, parent literal en componenten in voor een join conditie.

        Args:
            condition (dict): De conditie die verwerkt wordt.
            index (int): De volgorde van de conditie in de lijst.
            dict_attributes (dict): Alle attributen (in- en external).
        """
        condition["Order"] = index
        self._handle_condition_operator_and_literal(condition)
        self._handle_condition_components(
            condition=condition, dict_attributes=dict_attributes
        )

    def _handle_condition_operator_and_literal(self, condition: dict):
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

    def _handle_condition_components(
        self, condition: dict, dict_attributes: dict
    ) -> None:
        """Verwerkt de componenten van een join conditie en voegt deze toe aan de conditie.

        Deze functie haalt de child en parent componenten op, verrijkt deze met de juiste entity alias en voegt ze toe aan de join conditie.

        Args:
            condition (dict): De join conditie waarvan de componenten worden verwerkt.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.
        """
        dict_components = {}

        dict_child, dict_parent, alias_parent = self._split_join_components(
            condition=condition, dict_attributes=dict_attributes
        )

        if dict_parent:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_components["AttributeParent"] = copy.deepcopy(dict_parent)
            # Deepcopy makes sure entityalias for child and parent are not the same

        if dict_child:
            dict_child.update({"EntityAlias": self.composition["Id"]})
            dict_components["AttributeChild"] = copy.deepcopy(dict_child)

        condition["JoinConditionComponents"] = dict_components
        condition.pop("c:ExtendedCollections", None)

    def _split_join_components(
        self, condition: dict, dict_attributes: dict
    ) -> tuple[dict, dict | None, str | None]:
        """Splitst de componenten van een join conditie in child, parent en parent alias.

        Deze functie haalt de child attribute, parent attribute en parent alias op uit de componenten van een join conditie.

        Args:
            condition (dict): De join conditie waarvan de componenten worden gesplitst.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            tuple[dict, dict | None, str | None]: Een tuple met het child attribute dict, parent attribute dict en parent alias string.
        """
        dict_child = {}
        dict_parent = {}
        alias_parent = None

        components = self._get_condition_components(condition=condition)

        for component in components:
            type_component = component.get("Name")
            if type_component == "mdde_ChildAttribute":
                dict_child = self._handle_component_child_attribute(
                    component, dict_attributes
                )
            elif type_component == "mdde_ParentSourceObject":
                alias_parent = self._get_component_parent_alias(component=component)
            elif type_component == "mdde_ParentAttribute":
                dict_parent = self._handle_component_parent_attribute(
                    component=component, dict_attributes=dict_attributes
                )
            else:
                logger.warning(
                    f"Ongeldige join item in conditie '{type_component}' voor mapping '{self.mapping['a:Name']}' in {self.file_pd_ldm}"
                )
        return dict_child, dict_parent, alias_parent

    def _get_condition_components(self, condition: dict) -> list[dict] | None:
        """Haalt de componenten van een join conditie op uit de opgegeven conditie.

        Deze functie zoekt naar de componenten van een join conditie in de opgegeven conditie en retourneert deze als een lijst van dictionaries.

        Args:
            condition (dict): De conditie waaruit de componenten worden gehaald.

        Returns:
            list[dict]: Een lijst met componenten van de join conditie.
        """
        components = self._get_nested(
            data=condition, keys=["c:ExtendedCollections", "o:ExtendedCollection"]
        )
        if not components:
            logger.error(
                f"Kan geen join conditie componenten vinden voor mapping '{self.mapping['a:Name']}' in '{self.file_pd_ldm}'"
            )
            return
        components = [components] if isinstance(components, dict) else components
        components = self.clean_keys(components)
        return components

    def _handle_component_child_attribute(
        self, component: dict, dict_attributes: dict
    ) -> dict | None:
        """Haalt het child attribute dictionary op voor een join conditie component.

        Args:
            component (dict): Het component dat de child attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict | None: Een kopie van het child attribute dictionary.
        """
        logger.debug(f"Child attribute toegevoegd voor {self.file_pd_ldm}")
        type_entity = self.determine_reference_type(data=component["c:Content"])
        path_keys = ["c:Content", type_entity, "@Ref"]
        if id_attr := self._get_nested(data=component, keys=path_keys):
            if attr := dict_attributes.get(id_attr):
                return attr
            else:
                logger.error(
                    f"Kon attribuut niet vinden voor een child voor join condities voor mapping '{self.mapping['a:Name']}' uit '{self.file_pd_ldm}'"
                )
        logger.error(
            f"Geen attribuut referentie gevonden een child voor join condities voor mapping '{self.mapping['a:Name']}' uit '{self.file_pd_ldm}'"
        )
        return None

    def _get_component_parent_alias(self, component: dict) -> str | None:
        """Haalt de parent entity alias op uit een join conditie component.

        Deze functie zoekt de parent entity alias op in het component en retourneert deze als string.

        Args:
            component (dict): Het component dat de parent entity alias referentie bevat.

        Returns:
            str | None: De parent entity alias als string, of None als deze niet gevonden is.
        """
        path_keys = ["c:Content", "o:ExtendedSubObject", "@Ref"]
        logger.debug(f"Parent entity alias toegevoegd voor {self.file_pd_ldm}")
        parent_alias = self._get_nested(data=component, keys=path_keys)
        if not parent_alias:
            logger.error(
                f"Kan geen parent alias vinden voor een join conditie in '{self.composition['Name']}' van mapping '{self.mapping['a:Name']}' uit '{self.file_pd_ldm}'"
            )
        return parent_alias

    def _handle_component_parent_attribute(
        self, component: dict, dict_attributes: dict
    ) -> dict:
        """Haalt het parent attribute dictionary op uit een join conditie component.

        Deze functie zoekt het parent attribute op dat wordt gerefereerd in het component en
        retourneert een kopie van het dictionary uit dict_attributes.

        Args:
            component (dict): Het component dat de parent attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het parent attribute dictionary.
        """
        try:
            content = component.get("c:Content")
            type_entity = self.determine_reference_type(data=content)
            id_attr = content[type_entity]["@Ref"]
        except KeyError:
            logger.error(
                f"Kan geen attribuut vinden voor '{self.mapping['a:Name']}' in '{self.file_pd_ldm}'"
            )
            return {}
        if attr := dict_attributes.get(id_attr):
            return attr
        else:
            logger.error(
                f"Kan geen attribuut vinden voor referentie '{id_attr}' in mapping '{self.mapping['a:Name']}' in '{self.file_pd_ldm}'"
            )
