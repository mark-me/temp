import re

from logtools import get_logger

from .transformer_base import TransformerBase

logger = get_logger(__name__)


class TransformStereotype(TransformerBase):
    """Verrijken, schonen en transformeren van stereotype objecten (filters, scalars en aggregaten)"""

    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def transform(self, lst_objects: list[dict], dict_domains: dict) -> list[dict]:
        """Schoont en verrijkt de stereotype objecten die zijn opgenomen in het Power Designer LDM document

        Args:
            lst_objects (list): List van stereotype objecten
            dict_domains (dict): Domain data

        Returns:
            list: Geschoonde en verrijkte stereotype objecten
        """
        lst_objects = self.clean_keys(lst_objects)
        for objects in lst_objects:
            logger.debug(f"Start met het definieren van object '{objects['Name']}' in {self.file_pd_ldm}")
            objects = self._object_variables(object=objects, dict_domains=dict_domains)
            if objects["Stereotype"] == "mdde_AggregateBusinessRule":
                objects = self._object_identifiers(dict_object=objects)
            self._process_sql_expression(objects=objects)
            logger.debug(f"Klaar met het definieren van object {objects['Name']} in {self.file_pd_ldm}")
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

    def _split_sql_expression(self, objects: dict, sqlexpression: str) -> list[str] | None:
        """Splitst de SQL expressie op basis van het stereotype.

        Args:
            objects (dict): Het stereotype object.
            sqlexpression (str): De SQL expressie.

        Returns:
            list[str] or None: De gesplitste SQL expressie of None als niet gesplitst kan worden.
        """
        # pattern_incorrect = r"@\w+\s*=\s*@\w+"
        # matches_incorrect = re.findall(
        #     string=sqlexpression, pattern=pattern_incorrect
        # )
        # if matches_incorrect:
        #     logger.error(
        #         f"Invalide expressie gevonden {', '.join(matches_incorrect)} in {objects["Name"]} in bestand {self.file_pd_ldm}"
        #     )
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
                f"SqlExpression_split kan niet bepaald worden voor SqlExpression '{sqlexpression}' in {self.file_pd_ldm} "
            )
            return None

    def _assign_sql_expression_fields(self, objects: dict, sqlexpression_split: list[str]):
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

    def _object_variables(self, object: dict, dict_domains: dict) -> dict:
        """Schoont de variabelen van een object en verrijkt deze met domain data

        Args:
            object (dict): Stereotype object
            dict_domains (dict): Domain data

        Returns:
            dict: Geschoond en verrijkt stereotype object
        """
        logger.debug(f"Start met het verzamelen van variabelen voor object in {self.file_pd_ldm}:  {object['Name']}")
        lst_variables = self._extract_and_clean_variables(object)
        lst_variables = self._enrich_variables_with_domains(
            lst_variables=lst_variables, dict_domains=dict_domains
        )
        logger.debug(f"Klaar met het verzamelen van variabelen voor object in {self.file_pd_ldm}: {object['Name']}")
        object["Variables"] = lst_variables
        object.pop("c:Attributes")
        return object

    def _extract_and_clean_variables(self, object: dict) -> list[dict]:
        """Extraheert en schoont de variabelen van een object.

        Args:
            object (dict): Stereotype object

        Returns:
            list[dict]: Geschoonde variabelen
        """
        lst_variables = object["c:Attributes"]["o:EntityAttribute"]
        if isinstance(lst_variables, dict):
            lst_variables = [lst_variables]
        lst_variables = self.clean_keys(lst_variables)
        return lst_variables

    def _enrich_variables_with_domains(
        self, lst_variables: list[dict], dict_domains: dict
    ) -> list[dict]:
        """Verrijkt de variabelen met domain data.

        Args:
            lst_variables (list[dict]): Geschoonde variabelen
            dict_domains (dict): Domain data

        Returns:
            list[dict]: Verrijkte variabelen
        """
        for i, variables in enumerate(lst_variables):
            variables["Order"] = i
            path_keys = ["c:Domain", "o:Domain", "@Ref"]
            if id_domain := self._get_nested(data=variables, keys=path_keys):
                attr_domain = dict_domains[id_domain]
                keys_domain = {"Id", "Name", "Code", "DataType", "Length", "Precision"}
                attr_domain = {
                    k: attr_domain[k] for k in keys_domain if k in attr_domain
                }
                variables["Domain"] = attr_domain
                variables.pop("c:Domain")
        return lst_variables

    def _object_identifiers(self, dict_object: dict) -> dict:
        """Schoon de identifier(s) (sleutels) van een stereotype object
        Args:
            object (dict): Stereotype object

        Returns:
            dict: Stereotype object met geschoonde identifier(s)
        """
        dict_vars = {
            d["Id"]: {"Name": d["Name"], "Code": d["Code"]} for d in dict_object["Variables"]
        }
        has_primary, primary_id = self._check_primary_identifier(dict_object)
        logger.debug(f"Start met het verzamelen van identifiers voor object {dict_object['Name']} for {self.file_pd_ldm}")
        if identifiers := self._extract_identifiers(dict_object):
            identifiers = self.clean_keys(identifiers)
            self._process_identifiers(
                identifiers, dict_object, dict_vars, has_primary, primary_id
            )
            dict_object["Identifiers"] = identifiers
            logger.debug(f"Klaar met het verzamelen van identifiers voor {dict_object['Name']} in {self.file_pd_ldm}")
        return dict_object


    def _check_primary_identifier(self, dict_object: dict) -> tuple[bool, str | None]:
        """Controleert of er een primaire identifier aanwezig is en retourneert de status en het id."""
        has_primary = "c:PrimaryIdentifier" in dict_object
        primary_id = None
        if has_primary:
            primary_id = dict_object["c:PrimaryIdentifier"]["o:Identifier"]["@Ref"]
        return has_primary, primary_id

    def _extract_identifiers(self, dict_object: dict) -> list | None:
        """Extraheert de identifiers uit het object."""
        path_keys = ["c:Identifiers", "o:Identifier"]
        if identifiers := self._get_nested(data=dict_object, keys=path_keys):
            if isinstance(identifiers, dict):
                identifiers = [identifiers]
            return identifiers
        return None

    def _process_identifiers(
        self,
        identifiers: list[dict],
        dict_object: dict,
        dict_vars: dict,
        has_primary: bool,
        primary_id: str | None,
    ):
        """Verwerkt en verrijkt de identifiers met entiteit- en variabele-informatie.

        Deze functie vult de identifiers aan met entiteit-informatie, koppelt variabelen en markeert de primaire identifier.

        Args:
            identifiers (list[dict]): Lijst van identifier dictionaries.
            dict_object (dict): Het stereotype object waartoe de identifiers behoren.
            dict_vars (dict): Dictionary met variabelen op basis van hun Id.
            has_primary (bool): Of er een primaire identifier aanwezig is.
            primary_id (str | None): De Id van de primaire identifier, indien aanwezig.
        """
        for identifier in identifiers:
            identifier["EntityID"] = dict_object["Id"]
            identifier["EntityName"] = dict_object["Name"]
            identifier["EntityCode"] = dict_object["Code"]
            path_keys = ["c:Identifier.Attributes", "o:EntityAttribute"]
            if lst_var_id := self._get_nested(data=identifier, keys=path_keys):
                if isinstance(lst_var_id, dict):
                    lst_var_id = [lst_var_id]
                lst_var_id = [dict_vars[d["@Ref"]] for d in lst_var_id]
                identifier["Variables"] = lst_var_id
            else:
                logger.error(
                    f"Geen attributen toegevoegd in identifier {identifier['Name']} voor {self.file_pd_ldm}'"
                )
            if has_primary:
                identifier["IsPrimary"] = primary_id == identifier["Id"]


    def _extract_expression_variables(self, sqlexpression: str) -> list[str]:
        """Split de sqlexpression van een scalar in 1 of meerdere variabelen
        Args:
            object (dict): Stereotype object
            sqlexpression (string): volledige sqlexpression van de scalar

        Returns:
            list[str]: de individuele variabelen die samen de sqlexpression vormen
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
