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
        """Transformeert de join condities in de compositie en verrijkt deze met entiteit en attribuut data.

        Deze functie verwerkt alle join condities voor de huidige mapping en werkt de compositie bij met de getransformeerde condities.

        Args:
            dict_attributes (dict): Alle attributen (intern en extern) die gebruikt worden voor verrijking.

        Returns:
            dict: De bijgewerkte compositie dictionary met getransformeerde join condities.
        """
        if lst_conditions := self._get_conditions():
            lst_conditions = self.clean_keys(lst_conditions)
            for i, condition in enumerate(lst_conditions):
                self._process_condition(
                    condition=condition, index=i, dict_attributes=dict_attributes
                )
            self.composition["JoinConditions"] = lst_conditions
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
        lst_conditions = self._get_nested(data=self.composition, keys=path_keys)
        if not lst_conditions:
            logger.error(f"Kan geen join condities vinden in mapping '{self.mapping["a:Name"]}' in '{self.file_pd_ldm}'")
            return []
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
        lst_components = self._get_nested(
            data=condition, keys=["c:ExtendedCollections", "o:ExtendedCollection"]
        )
        if not lst_components:
            logger.error(
                f"Kan geen componenten vinden voor mapping '{self.mapping['a:Name']}' in '{self.file_pd_ldm}'"
            )
            return
        lst_components = (
            [lst_components] if isinstance(lst_components, dict) else lst_components
        )
        condition["JoinConditionComponents"] = (
            self._transform_join_condition_components(
                components=lst_components,
                dict_attributes=dict_attributes,
                alias_child=self.composition["Id"],
            )
        )
        condition.pop("c:ExtendedCollections", None)

    def _transform_join_condition_components(
        self, components: list, dict_attributes: dict, alias_child: str
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
            components=components, dict_attributes=dict_attributes
        )
        if dict_parent:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_components["AttributeParent"] = dict_parent
        if dict_child:
            dict_child.update({"EntityAlias": alias_child})
            dict_components["AttributeChild"] = dict_child
        return dict_components

    def _extract_join_components(self, components: list, dict_attributes: dict):
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
        components = self.clean_keys(components)
        for component in components:
            type_component = component.get("Name")
            if type_component == "mdde_ChildAttribute":
                dict_child = self._extract_join_child_attribute(
                    component, dict_attributes
                )
            elif type_component == "mdde_ParentSourceObject":
                alias_parent = self._extract_join_parent_alias(component)
            elif type_component == "mdde_ParentAttribute":
                dict_parent = self._extract_join_parent_attribute(
                    component, dict_attributes
                )
            else:
                logger.warning(
                    f"Ongeldige join item in conditie '{type_component}' voor mapping '{self.mapping['a:Name']}' in {self.file_pd_ldm}"
                )
        return dict_child, dict_parent, alias_parent

    def _extract_join_child_attribute(
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

    def _extract_join_parent_alias(self, component: dict) -> str:
        """Haalt de parent alias op uit een join conditie component.

        Deze functie zoekt de parent alias op in het component en retourneert deze als string.

        Args:
            component (dict): Het component dat de parent alias referentie bevat.

        Returns:
            str: De parent alias als string, of None als niet gevonden.
        """
        path_keys = ["c:Content", "o:ExtendedSubObject", "@Ref"]
        logger.debug(f"Parent entity alias toegevoegd voor {self.file_pd_ldm}")
        parent_alias = self._get_nested(data=component, keys=path_keys)
        if not parent_alias:
            logger.error(
                f"Kan geen parent alias vinden voor een join conditie in mapping '{self.mapping['a:Name']}' in '{self.file_pd_ldm}'"
            )
        return parent_alias

    def _extract_join_parent_attribute(
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
