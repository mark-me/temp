import re

from logtools import get_logger

from .base_transformer import TransformerBase

logger = get_logger(__name__)


class TransformSourceComposition(TransformerBase):
    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def transform(
        self,
        mapping: dict,
        dict_objects: dict,
        dict_attributes: dict,
        dict_datasources: dict,
    ) -> dict:
        """Transformeert en verrijkt de mapping met source composition data.

        Deze functie verwerkt de mapping, haalt compositie-items op, verrijkt deze, filtert specifieke items en voegt de resultaten toe aan de mapping.

        Args:
            mapping (dict): De mapping die getransformeerd moet worden.
            dict_objects (dict): Alle objecten (entiteiten, filters, scalars, aggregaten).
            dict_attributes (dict): Alle attributen.
            dict_datasources (dict): Alle datasources.

        Returns:
            dict: De getransformeerde mapping met verrijkte source composition data.
        """
        def _filter_scalar_business_rules(items):
            return [
                item
                for item in items
                if item["Entity"]["Stereotype"] != "mdde_ScalarBusinessRule"
            ]

        logger.debug(
            f"Start composities voor het extraheren van mapping '{mapping['Name']}' for {self.file_pd_ldm}"
        )

        composition = self._get_composition_list(mapping)
        composition = self.compositions_remove_mdde_examples(composition)
        lst_composition_items = self._extract_composition_items(composition)
        lst_composition_items = self._transform_composition_items(
            lst_composition_items, dict_objects, dict_attributes
        )

        mapping["SourceComposition"] = lst_composition_items
        mapping.pop("c:ExtendedCompositions", None)
        if "c:DataSource" in mapping:
            mapping = self._mapping_datasource(
                mapping=mapping, dict_datasources=dict_datasources
            )
            mapping.pop("c:DataSource")
        mapping = self._mapping_update(mapping=mapping)
        lst_source_composition_items = mapping["SourceComposition"]
        lst_source_composition_items = _filter_scalar_business_rules(lst_source_composition_items)
        mapping.pop("SourceComposition")
        mapping["SourceComposition"] = lst_source_composition_items
        return mapping

    def _get_composition_list(self, mapping: dict) -> list[dict]:
        """Haalt de lijst van composities op uit de mapping."""
        path_keys = ["c:ExtendedCompositions", "o:ExtendedComposition"]
        composition = self._get_nested(data=mapping, keys=path_keys)
        composition = [composition] if isinstance(composition, dict) else composition
        return self.clean_keys(composition)

    def _extract_composition_items(self, composition: dict) -> list[dict]:
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
        lst_composition_items: list[dict],
        dict_objects: dict,
        dict_attributes: dict,
    ) -> list[dict]:
        """Transformeert en verrijkt individuele compositie-items."""
        for i, composition_item in enumerate(lst_composition_items):
            composition_item = self._composition(
                composition_item,
                dict_objects=dict_objects,
                dict_attributes=dict_attributes,
            )
            composition_item["Order"] = i
            if "c:ExtendedCompositions" in composition_item:
                composition_item.pop("c:ExtendedCompositions")
                logger.info(
                    f"c:ExtendedCompositions is verwijderd van composition_item voor {self.file_pd_ldm}"
                )
            lst_composition_items[i] = composition_item
        return lst_composition_items

    def compositions_remove_mdde_examples(self, lst_compositions: list[dict]) -> dict:
        """Verwijderd de MDDE voorbeeld compositie veronderstelt dat er 1 compositie overblijft

        Args:
            lst_compositions (list[dict]): Composities, inclusief MDDE voorbeeld composities

        Returns:
            dict: Composities zonder de MDDE extensie voorbeelden
        """
        composition = {}
        compositions_new = []
        for item in lst_compositions:
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

    def _composition(
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
        self._handle_composition_conditions(composition, dict_attributes)
        return composition

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
            )
            logger.debug(
                f"Compositie {composition['JoinType']} voor '{composition['Name']}' in {self.file_pd_ldm}"
            )
        else:
            logger.warning(f"Geen 'ExtendedAttributesText' voor {self.file_pd_ldm}")

    def _handle_composition_conditions(self, composition: dict, dict_attributes: dict):
        """Handelt de verschillende condities van de compositie af, zoals join, source en scalar condities.

        Args:
            composition (dict): De compositie waarvoor de condities worden afgehandeld.
            dict_attributes (dict): Alle attributen (in- en external).
        """
        if "c:ExtendedCompositions" in composition:
            join_type = composition.get("JoinType", "").upper()
            if join_type not in ["FROM", "APPLY"]:
                composition_result = self._composition_join_conditions(
                    composition=composition, dict_attributes=dict_attributes
                )
                composition |= composition_result
            elif join_type in ["APPLY"]:
                if composition["Entity"]["Stereotype"] == "mdde_FilterBusinessRule":
                    composition_result = self._composition_source_conditions(
                        composition=composition, dict_attributes=dict_attributes
                    )
                    composition |= composition_result
                if composition["Entity"]["Stereotype"] == "mdde_ScalarBusinessRule":
                    composition_result = self._composition_scalar_conditions(
                        composition=composition, dict_attributes=dict_attributes
                    )
                    composition |= composition_result

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
            type_entity = [
                value
                for value in ["o:Entity", "o:Shortcut"]
                if value in entity["c:Content"]
            ][0]
            id_entity = entity["c:Content"][type_entity]["@Ref"]
            entity = dict_objects[id_entity]
            logger.debug(
                f"Composition entiteit '{entity['Name']}'voor {self.file_pd_ldm}"
            )
        composition["Entity"] = entity
        composition.pop(root_data)
        return composition

    def _composition_join_conditions(
        self, composition: dict, dict_attributes: dict
    ) -> dict:
        """Schoont en verrijkt data van de join condities van 1 van de composities

        Args:
            composition (dict): Compositie data
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Een geschoonde en verrijkte versie van join conditie data
        """
        logger.debug(
            f"Join conditities transformeren voor compositie '{composition['Name']} for {self.file_pd_ldm}'"
        )
        lst_conditions = self._extract_conditions_from_composition(composition)
        lst_conditions = self.clean_keys(lst_conditions)

        for i, condition in enumerate(lst_conditions):
            self._process_condition(condition, i, composition, dict_attributes)

        composition["JoinConditions"] = lst_conditions
        composition.pop("c:ExtendedCompositions")
        return composition

    def _extract_conditions_from_composition(self, composition: dict):
        """Haalt de lijst van condities uit de compositie.

        Deze functie retourneert alle condities die aanwezig zijn in de opgegeven compositie.

        Args:
            composition (dict): De compositie waaruit de condities worden gehaald.

        Returns:
            list: Een lijst met condities uit de compositie.
        """
        path_keys = [
            "c:ExtendedCompositions",
            "o:ExtendedComposition",
            "c:ExtendedComposition.Content",
            "o:ExtendedSubObject",
        ]
        lst_conditions = self._get_nested(data=composition, keys=path_keys)
        lst_conditions = (
            [lst_conditions] if isinstance(lst_conditions, dict) else lst_conditions
        )
        return lst_conditions

    def _process_condition(
        self, condition: dict, index: int, composition: dict, dict_attributes: dict
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
        self._set_condition_components(condition, composition, dict_attributes)

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
        self, condition: dict, composition: dict, dict_attributes: dict
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
            alias_child=composition["Id"],
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
        return self._get_nested(data=component, keys=path_keys)

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

    def _composition_source_conditions(
        self, composition: dict, dict_attributes: dict
    ) -> dict:
        """Schoont en verrijkt data van de source (bron) conditie van 1 van de composities

        Args:
            composition (dict): Compositie data
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Een geschoonde en verrijkte versie van source conditie data
        """
        logger.debug(
            f"Source conditie transformeren voor compositie  {composition['Name']} for {self.file_pd_ldm}"
        )
        lst_conditions = self._extract_source_conditions_from_composition(composition)
        lst_conditions = self.clean_keys(lst_conditions)

        for i, condition in enumerate(lst_conditions):
            self._process_source_condition(condition, i, dict_attributes, composition)
        composition["SourceConditions"] = lst_conditions
        return composition

    def _extract_source_conditions_from_composition(self, composition: dict):
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
        lst_conditions = self._get_nested(data=composition, keys=path_keys)
        lst_conditions = (
            [lst_conditions] if isinstance(lst_conditions, dict) else lst_conditions
        )
        return lst_conditions

    def _process_source_condition(
        self, condition: dict, index: int, dict_attributes: dict, composition: dict
    ):
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
            condition, keys=["ExtendedAttributesText"]
        ):
            parent_literal = self._extract_value_from_attribute_text(
                extended_attr_text,
                preceded_by="mdde_ParentLiteralValue,",
            )
        lst_components = condition["c:ExtendedCollections"]["o:ExtendedCollection"]
        lst_components = (
            [lst_components] if isinstance(lst_components, dict) else lst_components
        )
        sourceconditionvariable = self._source_condition_components(
            lst_components=lst_components,
            dict_attributes=dict_attributes,
            parent_literal=parent_literal,
        )
        if len(sourceconditionvariable) > 0:
            condition["SourceConditionVariable"] = sourceconditionvariable
        elif parent_literal != "":
            condition["SourceConditionVariable"] = parent_literal
        else:
            logger.warning(
                f"Geen SourceConditionVariable gevonden voor condition {condition.get('Code', '')} in compositie{composition.get('Name', '')} voor {self.file_pd_ldm}"
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

    def _composition_scalar_conditions(
        self, composition: dict, dict_attributes: dict
    ) -> dict:
        """Schoont en verrijkt data van de scalar condities van 1 van de composities

        Args:
            composition (dict): Compositie data
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Een geschoonde en verrijkte versie van de scalar conditie dat gebruikt wordt in de attribute mapping
        """
        logger.debug(
            f"Source conditions transformeren voor compositie  {composition['Name']} voor {self.file_pd_ldm}"
        )
        lst_conditions = self._extract_scalar_conditions_from_composition(
            composition=composition
        )
        lst_conditions = self.clean_keys(lst_conditions)

        for i in range(len(lst_conditions)):
            condition = lst_conditions[i]
            condition["Order"] = i
            self._process_scalar_condition(
                condition=condition, dict_attributes=dict_attributes
            )
            lst_conditions[i] = condition
        composition["ScalarConditions"] = lst_conditions

        sql_expression = composition["Entity"]["SqlExpression"]
        lst_sql_expression_variables = composition["Entity"]["SqlExpressionVariables"]
        dict_scalar_conditions = self._build_scalar_conditions_dict(
            lst_scalar_conditions=composition["ScalarConditions"]
        )

        sql_expression = self._replace_sql_expression_variables(
            sql_expression=sql_expression,
            lst_sql_expression_variables=lst_sql_expression_variables,
            dict_scalar_conditions=dict_scalar_conditions,
        )

        if sql_expression is not None:
            composition["Expression"] = sql_expression
        composition.pop("ScalarConditions")
        return composition

    def _extract_scalar_conditions_from_composition(self, composition: dict) -> list:
        """Haalt de lijst van scalar condities uit de compositie.

        Args:
            composition (dict): De compositie waaruit de scalar condities worden gehaald.

        Returns:
            list: Een lijst met scalar condities uit de compositie.
        """
        lst_conditions = composition["c:ExtendedCompositions"]["o:ExtendedComposition"][
            "c:ExtendedComposition.Content"
        ]["o:ExtendedSubObject"]
        if isinstance(lst_conditions, dict):
            lst_conditions = [lst_conditions]
        return lst_conditions

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
                alias_parent = self._extract_parent_source_object(component=component)
            elif component["Name"] == "mdde_ParentAttribute":
                dict_parent = self._extract_parent_attribute(
                    component=component, dict_attributes=dict_attributes
                )
        return dict_parent, alias_parent

    def _extract_child_attribute(self, component: dict, dict_attributes: dict) -> dict:
        """Haalt het child attribute dictionary op uit het opgegeven component.

        Deze functie zoekt het child attribute op dat wordt gerefereerd in het component en retourneert een kopie van het dictionary uit dict_attributes.

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

    def _extract_parent_source_object(self, component: dict) -> str:
        """Haalt de alias van het parent source object op uit het component.

        Deze functie retourneert de referentie naar het parent source object zoals aanwezig in het component.

        Args:
            component (dict): Het component dat de parent source object referentie bevat.

        Returns:
            str: De alias van het parent source object.
        """
        logger.debug(
            f"ScalarConditionAttribute alias toegevoegd voor {self.file_pd_ldm}"
        )
        return component["c:Content"]["o:ExtendedSubObject"]["@Ref"]

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
        type_entity = [
            value
            for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
            if value in component["c:Content"]
        ][0]
        id_attr = component["c:Content"][type_entity]["@Ref"]
        return dict_attributes[id_attr].copy()

    def _mapping_datasource(self, mapping: dict, dict_datasources: dict) -> dict:
        """Verrijkt de mapping met de datasource die als bron is aangewezen voor de mapping
        ten behoeve van het genereren van de DDL en ETL

        Args:
            mapping (dict): de volledige mapping
            dict_datasources (dict): dictionary met daarin alle beschikbare datasources

        Returns:
            dict: mapping met de zojuist toegevoegde datasource
        """
        datasource_alias_id = mapping["c:DataSource"]["o:DefaultDataSource"]["@Ref"]
        datasource_code = dict_datasources[datasource_alias_id]["Code"]
        mapping["DataSource"] = datasource_code
        return mapping

    def _mapping_update(self, mapping: dict) -> dict:
        """Creëert een expressie string die gebruikt wordt in de attribute mapping
        wanneer een bron attribuut verwijst naar een scalar

        Args:
            mapping (dict): de volledige mapping

        Returns:
            dict: mapping met de zojuist toegevoegde expressie
        """
        if lst_mapping := self._get_nested(data=mapping, keys=["AttributeMapping"]):
            for map in lst_mapping:
                if alias_id := self._get_nested(data=map, keys=["EntityAlias"]):
                    lst_scalarexpression = mapping.get("SourceComposition")
                    # Lookup the alias_id in composition. For the attributemapping we'll replace the entityalias with the expression we've created in the sourcecomposition
                    for composition in lst_scalarexpression:
                        if composition["Id"] == alias_id:
                            mappingexpression = composition.get("Expression")
                    map["Expression"] = mappingexpression
                    map.pop("EntityAlias")
        else:
            logger.warning(
                f"Attributemapping van {mapping['Name']} in '{self.file_pd_ldm}' ontbreekt voor update"
            )
        return mapping

    def _extract_value_from_attribute_text(
        self, extended_attrs_text: str, preceded_by: str
    ) -> str:
        """Extraheert de opgegeven tekst uit een tekst string. Deze tekst kan voorafgegaan
        worden door een specifieke tekst en wordt afgesloten door een \n of het zit aan het einde van de string

        Args:
            extended_attrs_text (str): De tekst dat de waarde bevat waarop gezocht wordt
            preceded_by (str): De tekst die de te vinden tekst voorafgaat

        Returns:
            str: De waarde die geassocieerd wordt met de voorafgaande tekst
        """
        idx_check = extended_attrs_text.find(preceded_by)
        if idx_check > 0:
            logger.info(
                f"'{idx_check}' waardes gevonden in extended_attrs_text bij het gebruik van: '{preceded_by}' in {self.file_pd_ldm}"
            )
            return self._extract_value_with_indices(extended_attrs_text, preceded_by)
        else:
            logger.info(
                f"Geen waardes gevonden in extended_attrs_text voor {self.file_pd_ldm} bij het gebruik van: '{preceded_by}' in {self.file_pd_ldm}"
            )
            return ""

    def _extract_value_with_indices(
        self, extended_attrs_text: str, preceded_by: str
    ) -> str:
        """Hulpmethode om de waarde te extraheren uit de tekst op basis van indices."""
        idx_start = extended_attrs_text.find(preceded_by) + len(preceded_by)
        idx_end = extended_attrs_text.find("\n", idx_start)
        idx_end = idx_end if idx_end > -1 else len(extended_attrs_text) + 1
        value = extended_attrs_text[idx_start:idx_end]
        idx_start = value.find("=") + 1
        return value[idx_start:].upper()
