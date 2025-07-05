import re

from logtools import get_logger

from .pd_transform_object import ObjectTransformer

logger = get_logger(__name__)


class TransformSourceComposition(ObjectTransformer):
    def __init__(self):
        super().__init__()

    def source_composition(
        self,
        lst_attribute_mapping: list,
        dict_objects: dict,
        dict_attributes: dict,
        dict_datasources: dict,
    ) -> dict:
        """Schoont de composities van de bron entiteiten data

            In deze functie starten we met het een lijst van composities omdat elke mapping een voorbeeld compositie bevat die gegeneerd
            wordt door een MDDE extensie. Wij verwijderen deze voorbeelden en doen de aanname dat we te maken hebben met 1 compositie per
            mapping. Daarom wordt er in de functie gewisseld tussen een lijst met composities naar het verwerken van 1 compositie per keer

        Args:
            lst_attribute_mapping (list): De mappings
            dict_objects (dict): Alle (objecten/filters/scalars) (in- en external)
            dict_attributes (dict): Alle attributen (in- en external)
            dict_datasources (dict): Alle mogelijke data sources

        Returns:
            list: Versie van mapping data waar compositie data is geschoond en verrijkt
        """
        # TODO: Review naming of compositions/ composition items
        mapping = lst_attribute_mapping
        logger.debug(f"Starting compositions transform for mapping '{mapping['Name']}'")

        composition = mapping["c:ExtendedCompositions"]["o:ExtendedComposition"]
        if isinstance(composition, dict):
            composition = [composition]

        composition = self.clean_keys(composition)

        # Removing example compositions, assuming one composition left
        composition = self.compositions_remove_mdde_examples(composition)

        # Searching for the composition items (FROM, JOIN, etc clauses)
        lst_composition_items = []
        if (
            "c:ExtendedCollections"
            in composition["c:ExtendedComposition.Content"]["o:ExtendedSubObject"]
        ):
            lst_composition_items = composition["c:ExtendedComposition.Content"][
                "o:ExtendedSubObject"
            ]
        elif "o:ExtendedSubObject" in composition["c:ExtendedComposition.Content"]:
            lst_composition_items = composition["c:ExtendedComposition.Content"][
                "o:ExtendedSubObject"
            ]
        elif "c:ExtendedCollections" in composition["c:ExtendedComposition.Content"]:
            lst_composition_items = composition["c:ExtendedComposition.Content"][
                "c:ExtendedCollections"
            ]
        else:
            logger.warning("Mapping without content")
        if isinstance(lst_composition_items, dict):
            lst_composition_items = [lst_composition_items]

        # Transforming individual composition items
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
                    "c:ExtendedCompositions has been removed from composition_item"
                )
            lst_composition_items[i] = composition_item

        mapping["SourceComposition"] = lst_composition_items
        mapping.pop("c:ExtendedCompositions")
        if "c:DataSource" in mapping:
            mapping = self._mapping_datasource(
                mapping=mapping, dict_datasources=dict_datasources
            )
            mapping.pop("c:DataSource")
        # Additional function to update the attribute mapping for target attributes with a scalar as source
        mapping = self._mapping_update(mapping=mapping)
        lst_source_composition_items = mapping["SourceComposition"]
        # remove all source_composition items where stereotype = mdde_ScalarBusinessRule from mapping
        lst_source_composition_items = [
            item
            for item in lst_source_composition_items
            if item["Entity"]["Stereotype"] != "mdde_ScalarBusinessRule"
        ]
        # we'll replace SourceComposition with the new lst_source_composition_items
        mapping.pop("SourceComposition")
        mapping["SourceComposition"] = lst_source_composition_items
        return mapping

    def compositions_remove_mdde_examples(self, lst_compositions: list) -> dict:
        """Verwijderd de MDDE voorbeeld compositie veronderstelt dat er 1 compositie overblijft

        Args:
            lst_compositions (list): Composities, inclusief MDDE voorbeeld composities

        Returns:
            dict: Composities zonder de MDDE extensie voorbeelden
        """
        composition = {}
        lst_compositions_new = []
        for item in lst_compositions:
            if "ExtendedBaseCollection.CollectionName" in item:
                if (
                    item["ExtendedBaseCollection.CollectionName"]
                    != "mdde_Mapping_Examples"
                ):
                    lst_compositions_new.append(item)
            else:
                logger.warning("No 'ExtendedBaseCollection.CollectionName'")
        # We assume there is only one composition per mapping, which is why we fill lst
        composition = lst_compositions_new[0]
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
                f"Composition {composition['JoinType']} for '{composition['Name']}'"
            )
        else:
            logger.warning("No 'ExtendedAttributesText")

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
            f"Starting entity transform for composition '{composition['Name']}'"
        )

        if "c:ExtendedComposition.Content" in composition:
            root_data = "c:ExtendedComposition.Content"
            entity = composition["c:ExtendedComposition.Content"]["o:ExtendedSubObject"]
        elif "c:ExtendedCollections" in composition:
            root_data = "c:ExtendedCollections"
            entity = composition["c:ExtendedCollections"]["o:ExtendedCollection"]
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
            logger.debug(f"Composition entity '{entity['Name']}'")
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
            f"Join conditions transform for composition '{composition['Name']}'"
        )
        lst_conditions = self._extract_conditions_from_composition(composition)
        lst_conditions = self.clean_keys(lst_conditions)

        for i in range(len(lst_conditions)):
            condition = lst_conditions[i]
            self._process_condition(condition, i, composition, dict_attributes)
            lst_conditions[i] = condition

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
        lst_conditions = composition["c:ExtendedCompositions"]["o:ExtendedComposition"][
            "c:ExtendedComposition.Content"
        ]["o:ExtendedSubObject"]
        if isinstance(lst_conditions, dict):
            lst_conditions = [lst_conditions]
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
        logger.debug(f"Join conditions transform for {index} '{condition['Name']}'")
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
                "There are no c:ExtendedCollections,check in model for invalid mapping "
            )
        lst_components = condition["c:ExtendedCollections"]["o:ExtendedCollection"]
        if isinstance(lst_components, dict):
            lst_components = [lst_components]
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
                    f"Unhandled kind of join item in condition '{type_component}'"
                )

        if len(dict_parent) > 0:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_components["AttributeParent"] = dict_parent
        if len(dict_child) > 0:
            dict_child.update({"EntityAlias": alias_child})
            dict_components["AttributeChild"] = dict_child
        return dict_components

    def _extract_join_child_attribute(self, component, dict_attributes):
        """Haalt het child attribute dictionary op voor een join conditie component.

        Args:
            component (dict): Het component dat de child attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het child attribute dictionary.
        """
        logger.debug("Added child attribute")
        type_entity = [
            value
            for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
            if value in component["c:Content"]
        ][0]
        id_attr = component["c:Content"][type_entity]["@Ref"]
        return dict_attributes[id_attr].copy()

    def _extract_join_parent_source_object(self, component):
        """Haalt de alias van het parent source object op voor een join conditie component.

        Args:
            component (dict): Het component dat de parent source object referentie bevat.

        Returns:
            str: De alias van het parent source object.
        """
        logger.debug("Added parent entity alias")
        return component["c:Content"]["o:ExtendedSubObject"]["@Ref"]

    def _extract_join_parent_attribute(self, component, dict_attributes):
        """Haalt het parent attribute dictionary op voor een join conditie component.

        Args:
            component (dict): Het component dat de parent attribute referentie bevat.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het parent attribute dictionary.
        """
        logger.debug("Added parent attribute")
        type_entity = [
            value
            for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
            if value in component["c:Content"]
        ][0]
        id_attr = component["c:Content"][type_entity]["@Ref"]
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
            f"Source conditions transform for composition  {composition['Name']}"
        )
        lst_conditions = self._extract_source_conditions_from_composition(composition)
        lst_conditions = self.clean_keys(lst_conditions)

        for i in range(len(lst_conditions)):
            condition = lst_conditions[i]
            self._process_source_condition(condition, i, dict_attributes, composition)
            lst_conditions[i] = condition
        composition["SourceConditions"] = lst_conditions
        return composition

    def _extract_source_conditions_from_composition(self, composition: dict):
        """Haalt de lijst van source condities uit de compositie.

        Args:
            composition (dict): De compositie waaruit de source condities worden gehaald.

        Returns:
            list: Een lijst met source condities uit de compositie.
        """
        lst_conditions = composition["c:ExtendedCompositions"]["o:ExtendedComposition"][
            "c:ExtendedComposition.Content"
        ]["o:ExtendedSubObject"]
        if isinstance(lst_conditions, dict):
            lst_conditions = [lst_conditions]
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
        if "ExtendedAttributesText" in condition:
            parent_literal = self._extract_value_from_attribute_text(
                condition["ExtendedAttributesText"],
                preceded_by="mdde_ParentLiteralValue,",
            )
        lst_components = condition["c:ExtendedCollections"]["o:ExtendedCollection"]
        if isinstance(lst_components, dict):
            lst_components = [lst_components]
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
                f"Geen SourceConditionVariable gevonden voor condition {condition.get('Code', '')} in compositie{composition.get('Name', '')}"
            )
        condition.pop("c:ExtendedCollections")

    def _source_condition_components(
        self, lst_components: list, dict_attributes: dict, parent_literal: str
    ) -> dict:
        """Vormt om, schoont en verrijkt component data van 1 source conditie

        Args:
            lst_components (list): source conditie component
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
                logger.debug("Added SourceConditionAttribute alias")
                alias_parent = component["c:Content"]["o:ExtendedSubObject"]["@Ref"]
            elif component["Name"] == "mdde_ParentAttribute":
                logger.debug("Added SourceConditionAttribute")
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
                id_attr = component["c:Content"][type_entity]["@Ref"]
                dict_parent = dict_attributes[id_attr].copy()
        return dict_parent, alias_parent

    def _get_source_child_attribute(
        self, lst_components: list, dict_attributes: dict
    ) -> dict:
        """Haalt het child attribute dictionary op uit de source conditie componenten.

        Args:
            lst_components (list): Lijst van componenten.
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
            f"Source conditions transform for composition  {composition['Name']}"
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

    def _build_scalar_conditions_dict(self, lst_scalar_conditions: list) -> dict:
        """Bouwt een dictionary van scalar condities op basis van hun Id.

        Args:
            lst_scalar_conditions (list): Lijst van scalar conditie dictionaries.

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
        lst_sql_expression_variables: list,
        dict_scalar_conditions: dict,
    ) -> str:
        """Vervangt variabelen in de SQL expressie door de juiste source variabelen.

        Args:
            sql_expression (str): De SQL expressie waarin variabelen vervangen moeten worden.
            lst_sql_expression_variables (list): Lijst van variabelen in de SQL expressie.
            dict_scalar_conditions (dict): Dictionary met scalar conditie variabelen.

        Returns:
            str: De aangepaste SQL expressie.
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
                    logger.info("Er is geen sql_expression gevonden")
        return sql_expression

    def _scalar_condition_components(
        self, lst_components: list, dict_attributes: dict
    ) -> dict:
        """Vormt om, schoont en verrijkt component data van 1 scalar conditie

        Args:
            lst_components (list): scalar conditie component
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
        self, lst_components: list, dict_attributes: dict
    ) -> dict:
        """Haalt het child attribute dictionary op uit de scalar conditie componenten.

        Args:
            lst_components (list): Lijst van componenten.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het child attribute dictionary, of leeg dict als niet gevonden.
        """
        lst_components = self.clean_keys(content=lst_components)
        for component in lst_components:
            if component["Name"] == "mdde_ChildAttribute":
                return self._extract_child_attribute(
                    component=component, dict_attributes=dict_attributes
                )
        return {}

    def _get_scalar_parent_attribute_and_alias(
        self, lst_components: list, dict_attributes: dict
    ) -> tuple:
        """Haalt het parent attribute dictionary en alias op uit de scalar conditie componenten.

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
        logger.debug("Added child attribute")
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
        logger.debug("Added ScalarConditionAttribute alias")
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
        logger.debug("Added ScalarConditionAttribute")
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
        if "AttributeMapping" in mapping:
            lst_mapping = mapping["AttributeMapping"]
            for map in lst_mapping:
                if "EntityAlias" in map:
                    alias_id = map["EntityAlias"]
                    lst_scalarexpression = mapping["SourceComposition"]
                    # Lookup the alias_id in composition. For the attributemapping we'll replace the entityalias with the expression we've created in the sourcecomposition
                    for composition in lst_scalarexpression:
                        if composition["Id"] == alias_id:
                            mappingexpression = composition["Expression"]
                    map["Expression"] = mappingexpression
                    map.pop("EntityAlias")
        else:
            logger.warning(
                f"Attributemapping van {mapping['Name']} ontbreekt voor update"
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
                f"'{idx_check}' values found in extended_attrs_text using: '{preceded_by}'"
            )
            idx_start = extended_attrs_text.find(preceded_by) + len(preceded_by)
            idx_end = extended_attrs_text.find("\n", idx_start)
            idx_end = idx_end if idx_end > -1 else len(extended_attrs_text) + 1
            value = extended_attrs_text[idx_start:idx_end]
            idx_start = value.find("=") + 1
            value = value[idx_start:].upper()
        else:
            logger.info(
                f"no values found in extended_attrs_text using: '{preceded_by}'"
            )
            value = ""
        return value
