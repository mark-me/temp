import re

from logtools import get_logger

from .pd_transform_object import ObjectTransformer

logger = get_logger(__name__)


class TransformStereotype(ObjectTransformer):
    """Verrijken, schonen en transformeren van stereotype objecten (filters, scalars en aggregaten)"""

    def __init__(self):
        super().__init__()

    def domains(self, lst_domains: list) -> dict:
        """Verrijk de stereotypes met domain data

        Args:
            lst_domains (list): Domain data

        Returns:
            dict: Objecten met het opgegeven stereotype zijn verrijkt met domain data
        """
        dict_domains = {}
        if isinstance(lst_domains, dict):
            lst_domains = [lst_domains]
        lst_domains = self.convert_timestamps(lst_domains)
        lst_domains = self.clean_keys(lst_domains)
        for domain in lst_domains:
            dict_domains[domain["Id"]] = domain
        return dict_domains

    def objects(self, lst_objects: list, dict_domains: dict) -> list:
        """Schoont en verrijkt de stereotype objecten die zijn opgenomen in het Power Designer LDM document

        Args:
            lst_objects (list): List van stereotype objecten
            dict_domains (dict): Domain data

        Returns:
            list: Geschoonde en verrijkte stereotype objecten
        """
        lst_objects = self.clean_keys(lst_objects)
        for i in range(len(lst_objects)):
            objects = lst_objects[i]
            logger.debug(f"Start creating object definition for '{objects['Name']}'")
            objects = self._object_variables(object=objects, dict_domains=dict_domains)
            if objects["Stereotype"] == "mdde_AggregateBusinessRule":
                objects = self._object_identifiers(object=objects)
            self._process_sql_expression(objects=objects)
            logger.debug(f"Finished creating object definition for {objects['Name']}")
            lst_objects[i] = objects
        return lst_objects

    def _process_sql_expression(self, objects: dict):
        """Verwerkt de SQL expressie van een stereotype object en verrijkt het object met variabelen.

        Args:
            objects (dict): Het stereotype object dat verrijkt wordt.
        """
        if "ExtendedAttributesText" in objects:
            sqlexpression = self.extract_value_from_attribute_text(
                objects["ExtendedAttributesText"], preceded_by="mdde_SqlExpression,"
            )
            sqlexpression_split = self._split_sql_expression(
                objects=objects, sqlexpression=sqlexpression
            )
            if sqlexpression_split is not None:
                self._assign_sql_expression_fields(
                    objects=objects, sqlexpression_split=sqlexpression_split
                )

    def _split_sql_expression(self, objects: dict, sqlexpression: str) -> list:
        """Splitst de SQL expressie op basis van het stereotype.

        Args:
            objects (dict): Het stereotype object.
            sqlexpression (str): De SQL expressie.

        Returns:
            list or None: De gesplitste SQL expressie of None als niet gesplitst kan worden.
        """
        if objects["Stereotype"] == "mdde_FilterBusinessRule":
            # if '=' in sqlexpression:
            #     lst_expression = sqlexpression.split("=", 1)
            #     lst_expression.insert(1, "=")
            #     return lst_expression
            # else:
            lst_expression = sqlexpression.split(" ", 1)
            return lst_expression
        elif objects["Stereotype"] == "mdde_ScalarBusinessRule":
            return sqlexpression.split("=", 1)
        else:
            logger.error(
                f"SqlExpression_split cannot be determined for SqlExpression '{sqlexpression}' "
            )
            return None

    def _assign_sql_expression_fields(self, objects: dict, sqlexpression_split: list):
        """Wijs de juiste velden toe aan het stereotype object op basis van de gesplitste SQL expressie.

        Args:
            objects (dict): Het stereotype object.
            sqlexpression_split (list): De gesplitste SQL expressie.
        """
        objects["SqlVariable"] = sqlexpression_split[0].strip()
        objects["SqlExpression"] = sqlexpression_split[1].strip()
        if objects["Stereotype"] == "mdde_ScalarBusinessRule":
            sqlexpression = sqlexpression_split[1].strip()
            lst_expression_variables = self._extract_expression_variables(
               sqlexpression=sqlexpression
            )
            if lst_expression_variables is not None:
                objects["SqlExpressionVariables"] = lst_expression_variables

    def _object_variables(self, object: dict, dict_domains: list) -> dict:
        """Schoont de variabelen van een object en verrijkt deze met domain data

        Args:
            object (dict): Stereotype object
            dict_domains (dict): Domain data

        Returns:
            dict: Geschoond en verrijkt stereotype object
        """
        logger.debug(f"Start collecting variables for object:  {object['Name']}")
        lst_variables = self._extract_and_clean_variables(object)
        lst_variables = self._enrich_variables_with_domains(
            lst_variables=lst_variables, dict_domains=dict_domains
        )
        logger.debug(f"Finished collecting variables for object: {object['Name']}")
        object["Variables"] = lst_variables
        object.pop("c:Attributes")
        return object

    def _extract_and_clean_variables(self, object: dict) -> list:
        """Extraheert en schoont de variabelen van een object.

        Args:
            object (dict): Stereotype object

        Returns:
            list: Geschoonde variabelen
        """
        lst_variables = object["c:Attributes"]["o:EntityAttribute"]
        if isinstance(lst_variables, dict):
            lst_variables = [lst_variables]
        lst_variables = self.clean_keys(lst_variables)
        return lst_variables

    def _enrich_variables_with_domains(
        self, lst_variables: list, dict_domains: dict
    ) -> list:
        """Verrijkt de variabelen met domain data.

        Args:
            lst_variables (list): Geschoonde variabelen
            dict_domains (dict): Domain data

        Returns:
            list: Verrijkte variabelen
        """
        for i in range(len(lst_variables)):
            variables = lst_variables[i]
            variables["Order"] = i
            if "c:Domain" in variables:
                id_domain = variables["c:Domain"]["o:Domain"]["@Ref"]
                attr_domain = dict_domains[id_domain]
                keys_domain = {"Id", "Name", "Code", "DataType", "Length", "Precision"}
                attr_domain = {
                    k: attr_domain[k] for k in keys_domain if k in attr_domain
                }
                variables["Domain"] = attr_domain
                variables.pop("c:Domain")
            lst_variables[i] = variables
        return lst_variables

    def _object_identifiers(self, object: dict) -> dict:
        """Schoon de identifier(s) (sleutels) van een stereotype object
        Args:
            object (dict): Stereotype object

        Returns:
            dict: Stereotype object met geschoonde identifier(s)
        """
        dict_vars = {
            d["Id"]: {"Name": d["Name"], "Code": d["Code"]} for d in object["Variables"]
        }
        # Set primary identifiers as an attribute of the identifiers
        has_primary = "c:PrimaryIdentifier" in object
        if has_primary:
            primary_id = object["c:PrimaryIdentifier"]["o:Identifier"]["@Ref"]
        logger.debug(f"Start collecting identifiers for {object['Name']}")
        # Reroute identifiers
        if "c:Identifiers" in object:
            identifiers = object["c:Identifiers"]["o:Identifier"]
            if isinstance(identifiers, dict):
                identifiers = [identifiers]
            identifiers = self.clean_keys(identifiers)
            # Clean and transform identifier data
            for j in range(len(identifiers)):
                identifier = identifiers[j]
                identifier["EntityID"] = object["Id"]
                identifier["EntityName"] = object["Name"]
                identifier["EntityCode"] = object["Code"]
                if "c:Identifier.Attributes" not in identifier:
                    logger.error(
                        f"No attributes included in the identifier {identifier['Name']}'"
                    )
                else:
                    lst_var_id = identifier["c:Identifier.Attributes"][
                        "o:EntityAttribute"
                    ]
                    if isinstance(lst_var_id, dict):
                        lst_var_id = [lst_var_id]
                    lst_var_id = [dict_vars[d["@Ref"]] for d in lst_var_id]
                    identifier["Variables"] = lst_var_id
                    #identifier.pop("c:Identifier.Attributes")
                # Set primary identifier attribute
                if has_primary:
                    identifier["IsPrimary"] = primary_id == identifier["Id"]
                identifiers[j] = identifier
            object["Identifiers"] = identifiers
            #object.pop("c:Identifiers")
            #object.pop("c:PrimaryIdentifier")
            logger.debug(f"Finished collecting identifiers for {object['Name']}")
        return object

    def _extract_expression_variables(self, sqlexpression: str) -> list:
        """Split de sqlexpression van een scalar in 1 of meerdere variabelen
        Args:
            object (dict): Stereotype object
            sqlexpression (string): volledige sqlexpression van de scalar

        Returns:
            list: de individuele variabelen die samen de sqlexpression vormen
        """
        lst_expression_variables = []
        # Count the number of placeholder variables in the SqlExpression
        number_of_variables = sqlexpression.count("@")
        j = 0
        k = number_of_variables
        expression_variable = None
        # Start loop to split the SqlExpression further
        while j < k:
            idx_start = sqlexpression.find("@", 1)
            # Look for the first non word character after idx_start to not find the @ from the variable
            find_non_alpha = re.search("[\W]", sqlexpression[idx_start + 1 :])
            # Idx_end is the start_position plus the position of the first alphanumeric character
            idx_end = idx_start + find_non_alpha.start()
            expression_variable = sqlexpression[idx_start : idx_end + 1]
            lst_expression_variables = (*lst_expression_variables, expression_variable)
            # remove the variable from the sqlexpression string
            sqlexpression = sqlexpression[idx_end + 1 :]
            j += 1
        return lst_expression_variables
