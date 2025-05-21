import re

from log_config import logging
from .pd_transform_object import ObjectTransformer

logger = logging.getLogger(__name__)

class TransformSourceComposition(ObjectTransformer):
    def __init__(self):
        super().__init__()

    def source_composition(self, lst_attribute_mapping: list, dict_objects:dict, dict_attributes:dict, dict_datasources:dict) -> dict:
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
            logger.warning("List object is actually dictionary; file: pd_transform_source_composition; object:composition")
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
                    "o:ExtendedSubObject"]
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
            logger.warning("List object is actually dictionary; file:pd_transform_source_composition; object:lst_composition_items")
            lst_composition_items = [lst_composition_items]

        # Transforming individual composition items
        for i, composition_item in enumerate(lst_composition_items):
            composition_item = self.__composition(
                composition_item,
                dict_objects=dict_objects,
                dict_attributes=dict_attributes,
            )
            composition_item["Order"] = i
            if "c:ExtendedCompositions" in composition_item:
                composition_item.pop("c:ExtendedCompositions")
                logger.info("c:ExtendedCompositions has been removed from composition_item")
            lst_composition_items[i] = composition_item

        mapping["SourceComposition"] = lst_composition_items
        mapping.pop("c:ExtendedCompositions")
        if "c:DataSource" in mapping:
            mapping = self.__mapping_datasource(mapping = mapping, dict_datasources=dict_datasources)
            mapping.pop("c:DataSource")
        # Additional function to update the attribute mapping for target attributes with a scalar as source
        mapping = self.__mapping_update(mapping = mapping)
        lst_source_composition_items = mapping["SourceComposition"]
        # remove all source_composition items where stereotype = mdde_ScalarBusinessRule from mapping
        lst_source_composition_items = [item for item in lst_source_composition_items if item["Entity"]["Stereotype"] != "mdde_ScalarBusinessRule"]
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
        for i in range(len(lst_compositions)):
            if "ExtendedBaseCollection.CollectionName" in lst_compositions[i]:
                if (
                    lst_compositions[i]["ExtendedBaseCollection.CollectionName"]
                    != "mdde_Mapping_Examples"
                ):
                    lst_compositions_new.append(lst_compositions[i])
            else:
                logger.warning("No 'ExtendedBaseCollection.CollectionName'")
        # We assume there is only one composition per mapping, which is why we fill lst
        composition = lst_compositions_new[0]
        return composition

    def __composition(
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
        # Determine JoinAlias
        composition["JoinAlias"] = composition['Id']
        # Determine composition clause (FROM/JOIN)
        if "ExtendedAttributesText" in composition:
            composition["JoinAliasName"] = self.__extract_value_from_attribute_text(
                composition["ExtendedAttributesText"],
                preceded_by="mdde_JoinAlias,",
            )
            composition["JoinType"] = self.__extract_value_from_attribute_text(
                composition["ExtendedAttributesText"],
                preceded_by="mdde_JoinType,",
            )
            logger.debug(
                f"Composition {composition['JoinType']} for '{composition['Name']}'"
            )
        else:
            logger.warning("No 'ExtendedAttributesText")
        # Determine entities involved
        composition = self.__composition_entity(
            composition=composition, dict_objects=dict_objects
        )
        # Join conditions (ON clause)
        if "c:ExtendedCompositions" in composition:
            if composition["JoinType"].upper() not in ["FROM",  "APPLY"]:
                composition = self.__composition_join_conditions(
                    composition=composition, dict_attributes=dict_attributes
                )
                #TODO: add additional filter to only direct stereotype = filter to source_conditions
            elif composition["JoinType"].upper() in ["APPLY"]:
                # Only stereotype mdde_FilterBusinessRule qualify as source_condition at the moment
                if composition["Entity"]["Stereotype"] == "mdde_FilterBusinessRule":
                    composition = self.__composition_source_conditions(composition = composition, dict_attributes=dict_attributes)
                if composition["Entity"]["Stereotype"] == "mdde_ScalarBusinessRule":
                    composition = self.__composition_scalar_conditions(composition = composition, dict_attributes = dict_attributes)
            else:
                pass
        return composition

    def __composition_entity(self, composition: dict, dict_objects: dict) -> dict:
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

    def __composition_join_conditions(
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
        lst_conditions = composition["c:ExtendedCompositions"]["o:ExtendedComposition"][
            "c:ExtendedComposition.Content"
        ]["o:ExtendedSubObject"]
        if isinstance(lst_conditions, dict):
            logger.warning("List object is actually dictionary; file:pd_transform_source_composition; object:lst_conditions")
            lst_conditions = [lst_conditions]
        lst_conditions = self.clean_keys(lst_conditions)

        for i in range(len(lst_conditions)):
            condition = lst_conditions[i]
            condition["Order"] = i
            logger.debug(
                f"Join conditions transform for {str(i)}) '{condition['Name']}'"
            )
            # Condition operator and Parent literal (using a fixed value instead of a parent column)
            condition_operator = "="
            parent_literal = ''
            if "ExtendedAttributesText" in condition:
                condition_operator = self.__extract_value_from_attribute_text(
                    condition["ExtendedAttributesText"],
                    preceded_by="mdde_JoinOperator,",
                )
                parent_literal = self.__extract_value_from_attribute_text(
                    condition["ExtendedAttributesText"],
                    preceded_by="mdde_ParentLiteralValue,",
                )
            if condition_operator == "":
                condition["Operator"] = "="
            else:
                condition["Operator"] = condition_operator
            condition["ParentLiteral"] = parent_literal

            # Condition components (i.e. left and right side of the condition operator)
            if "c:ExtendedCollections" not in condition:
                logger.warning("There are no c:ExtendedCollections,check in model for invalid mapping ")             
            lst_components = condition["c:ExtendedCollections"]["o:ExtendedCollection"]
            if isinstance(lst_components, dict):
                logger.warning("List object is actually dictionary; file:pd_transform_source_composition; object:lst_components")
                lst_components = [lst_components]
            condition["JoinConditionComponents"] = self.__join_condition_components(
                lst_components=lst_components, dict_attributes=dict_attributes, alias_child=composition["Id"]
            )
            condition.pop("c:ExtendedCollections")
            lst_conditions[i] = condition


        composition["JoinConditions"] = lst_conditions
        composition.pop("c:ExtendedCompositions")
        return composition

    def __join_condition_components(
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
                # Child attribute
                logger.debug("Added child attribute")
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
                id_attr = component["c:Content"][type_entity]["@Ref"]
                dict_child = dict_attributes[id_attr].copy()
            elif type_component == "mdde_ParentSourceObject":
                # Alias to point to a composition entity
                logger.debug("Added parent entity alias")
                alias_parent = component["c:Content"][
                    "o:ExtendedSubObject"
                ]["@Ref"]
            elif type_component == "mdde_ParentAttribute":
                # Parent attribute
                logger.debug("Added parent attribute")
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
                id_attr = component["c:Content"][type_entity]["@Ref"]
                dict_parent = dict_attributes[id_attr].copy()
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

    def __composition_source_conditions(self, composition: dict, dict_attributes: dict) -> dict:
        """Schoont en verrijkt data van de source (bron) conditie van 1 van de composities

        Args:
            composition (dict): Compositie data
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Een geschoonde en verrijkte versie van source conditie data
        """
        logger.debug(f"Source conditions transform for composition  {composition['Name']}")
        lst_conditions = composition["c:ExtendedCompositions"]["o:ExtendedComposition"]["c:ExtendedComposition.Content"]["o:ExtendedSubObject"]
        if isinstance(lst_conditions, dict):
            lst_conditions = [lst_conditions]
        lst_conditions = self.clean_keys(lst_conditions)

        for i in range(len(lst_conditions)):
            condition = lst_conditions[i]
            condition[ "Order"] = i
            parent_literal = ""
            if "ExtendedAttributesText" in condition:
                parent_literal = self.__extract_value_from_attribute_text(
                    condition["ExtendedAttributesText"],
                    preceded_by="mdde_ParentLiteralValue,",
                )
            lst_components = condition[ "c:ExtendedCollections"]["o:ExtendedCollection"]
            if isinstance(lst_components, dict):
                lst_components=[lst_components]
            sourceconditionvariable = self.__source_condition_components(lst_components=lst_components,dict_attributes=dict_attributes, parent_literal = parent_literal)
            if len(sourceconditionvariable) > 0:
                condition["SourceConditionVariable"] = sourceconditionvariable
            elif parent_literal != "":
                condition["SourceConditionVariable"] = parent_literal
            else:
                logger.warning(f"Geen SourceConditionVariable gevonden voor condition {condition['Code']} in compositie{composition['Name']}")
            condition.pop( "c:ExtendedCollections")
            lst_conditions[i] = condition
        composition["SourceConditions"] = lst_conditions
        return composition

    def __source_condition_components(self, lst_components: list, dict_attributes: dict, parent_literal: str) -> dict:
        """Vormt om, schoont en verrijkt component data van 1 source conditie

        Args:
            lst_components (list): source conditie component
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Geschoonde, omgevormde en verrijkte source conditie component data
        """
        dict_source_condition_attribute = {}
        dict_parent = {}
        dict_child = {}
        alias_parent = None
        lst_components = self.clean_keys(lst_components)
        for component in lst_components:
            type_component = component["Name"]
            if type_component == "mdde_ParentSourceObject":
                logger.debug("Added SourceConditionAttribute alias")
                alias_parent = component["c:Content"]["o:ExtendedSubObject"]["@Ref"]
            elif type_component ==  "mdde_ParentAttribute":
                # Parent attribute
                logger.debug("Added SourceConditionAttribute")
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
                id_attr = component["c:Content"][type_entity]["@Ref"]
                dict_parent = dict_attributes[id_attr].copy()
            elif type_component == "mdde_ChildAttribute":
                # Child attribute
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
                id_attr = component["c:Content"][type_entity]["@Ref"]
                dict_child = dict_attributes[id_attr].copy()
            else:
                logger.warning(
                    f"Unhandled kind of join item in condition '{type_component}'"
                )
        if len(dict_parent) > 0:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_source_condition_attribute["SourceAttribute"] = dict_parent
        if parent_literal != "":
            if len(dict_child) > 0:
                dict_source_condition_attribute["SourceAttribute"] = dict_child
                dict_source_condition_attribute["SourceAttribute"]["Expression"] = parent_literal
        return dict_source_condition_attribute

    def __composition_scalar_conditions(self, composition: dict, dict_attributes: dict) -> dict:
        """Schoont en verrijkt data van de scalar condities van 1 van de composities

        Args:
            composition (dict): Compositie data
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Een geschoonde en verrijkte versie van de scalar conditie dat gebruikt wordt in de attribute mapping
        """
        logger.debug(f"Source conditions transform for composition  {composition['Name']}")
        lst_conditions = composition["c:ExtendedCompositions"]["o:ExtendedComposition"]["c:ExtendedComposition.Content"]["o:ExtendedSubObject"]
        if isinstance(lst_conditions, dict):
            lst_conditions = [lst_conditions]
        lst_conditions = self.clean_keys(lst_conditions)

        for i in range(len(lst_conditions)):
            condition = lst_conditions[i]
            condition[ "Order"] = i
            lst_components = condition[ "c:ExtendedCollections"]["o:ExtendedCollection"]
            if isinstance(lst_components, dict):
                lst_components=[lst_components]
            condition["ScalarConditionVariable"] = self.__scalar_condition_components(lst_components=lst_components,dict_attributes=dict_attributes)
            condition.pop( "c:ExtendedCollections")
            lst_conditions[i] = condition
        composition[ "ScalarConditions"] = lst_conditions
        sql_expression = composition["Entity"]["SqlExpression"]
        lst_sqlexpressionvariables = composition["Entity"]["SqlExpressionVariables"]
        dict_scalarconditions = {}
        lst_scalarconditions = composition["ScalarConditions"]
        for scalarcondition in lst_scalarconditions:
            dict_scalarconditions[scalarcondition["Id"]] = {
                "Id": scalarcondition["Id"],
                "TargetVariable":scalarcondition["ScalarConditionVariable"]["AttributeChild"],
                "SourceVariable":scalarcondition["ScalarConditionVariable"]["SourceAttribute"]
                }
        for condition in dict_scalarconditions:
            targetvariable =  dict_scalarconditions[condition]["TargetVariable"].upper()
            for variable in lst_sqlexpressionvariables:
                variable_compare = variable[1:len(variable)]
                if targetvariable == variable_compare:
                    sourcevariable = dict_scalarconditions[condition]["SourceVariable"]
                    pattern = r''+ variable + r'\b'
                    sql_expression = re.sub(pattern, sourcevariable, sql_expression)
                else:
                    pass
                    logger.warning("Er is geen sql_expression gevonden")
        if sql_expression is not None:
            composition["Expression"] = sql_expression
        composition.pop("ScalarConditions")
        return composition

    def __scalar_condition_components(self, lst_components: list, dict_attributes: dict) -> dict:
        """Vormt om, schoont en verrijkt component data van 1 scalar conditie

        Args:
            lst_components (list): scalar conditie component
            dict_attributes (dict): Alle attributen (in- en external)

        Returns:
            dict: Geschoonde, omgevormde en verrijkte scalar conditie component data
        """
        dict_scalar_condition_attribute = {}
        dict_child = {}
        dict_parent = {}
        lst_components = self.clean_keys(lst_components)
        for component in lst_components:
            type_component = component["Name"]
            if type_component == "mdde_ChildAttribute":
                # Child attribute
                logger.debug("Added child attribute")
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
                id_attr = component["c:Content"][type_entity]["@Ref"]
                dict_child = dict_attributes[id_attr].copy()
            if type_component == "mdde_ParentSourceObject":
                logger.debug("Added ScalarConditionAttribute alias")
                alias_parent = component["c:Content"][
                    "o:ExtendedSubObject"
                ]["@Ref"]
            elif type_component ==  "mdde_ParentAttribute":
                # Parent attribute
                logger.debug("Added ScalarConditionAttribute")
                type_entity = [
                    value
                    for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
                    if value in component["c:Content"]
                ][0]
                id_attr = component["c:Content"][type_entity]["@Ref"]
                dict_parent = dict_attributes[id_attr].copy()
            else:
                logger.warning(
                    f"Unhandled kind of join item in condition '{type_component}'"
                )
        if len(dict_parent) > 0:
            if alias_parent is not None:
                dict_parent.update({"EntityAlias": alias_parent})
            dict_scalar_condition_attribute["SourceAttribute"] = dict_parent[ "IdEntity"] +  "." + dict_parent["Code"]
        if len(dict_child) > 0:
            dict_scalar_condition_attribute["AttributeChild"] = dict_child["Code"]
        return dict_scalar_condition_attribute

    def __mapping_datasource(self, mapping:dict, dict_datasources:dict) -> dict:
        """Verrijkt de mapping met de datasource die als bron is aangewezen voor de mapping ten behoeve van het genereren van de DDL en ETL

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

    def __mapping_update(self, mapping:dict) -> dict:
        """Creëert een expressie string die gebruikt wordt in de attribute mapping wanneer een bron attribuut verwijst naar een scalar

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
                        if  composition["Id"] == alias_id:
                            mappingexpression = composition["Expression"]
                        else:
                            pass
                    map["Expression"] = mappingexpression
                    map.pop("EntityAlias")
                else:
                    pass
        else:
            logger.warning(f"Attributemapping van {mapping['Name']} ontbreekt voor update")
        return mapping

    def __extract_value_from_attribute_text(
        self, extended_attrs_text: str, preceded_by: str
    ) -> str:
        """Extraheert de opgegeven tekst uit een tekst string. Deze tekst kan voorafgegaan worden door een specifieke tekst en wordt afgesloten door een \n of het zit aan het einde van de string

        Args:
            extended_attrs_text (str): De tekst dat de waarde bevat waarop gezocht wordt
            preceded_by (str): De tekst die de te vinden tekst voorafgaat

        Returns:
            str: De waarde die geassocieerd wordt met de voorafgaande tekst
        """
        idx_check = extended_attrs_text.find(preceded_by)
        if idx_check > 0:
            logger.info(f"'{idx_check}' values found in extended_attrs_text using: '{preceded_by}'")
            idx_start = extended_attrs_text.find(preceded_by) + len(preceded_by)
            idx_end = extended_attrs_text.find("\n", idx_start)
            idx_end = idx_end if idx_end > -1 else len(extended_attrs_text) + 1
            value = extended_attrs_text[idx_start:idx_end]
            idx_start = value.find("=") + 1
            value = value[idx_start:].upper()
        else:
            logger.warning(f"no values found in extended_attrs_text using: '{preceded_by}'")
            value = ""
        return value
