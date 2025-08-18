import re

from logtools import get_logger

from .base_transformer import BaseTransformer

logger = get_logger(__name__)


class StereotypeTransformer(BaseTransformer):
    """Verrijken, schonen en transformeren van stereotype objecten (filters, scalars en aggregaten)"""

    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def transform(self, stereo_types: list[dict], dict_domains: dict) -> list[dict]:
        """Schoont en verrijkt de stereotype objecten die zijn opgenomen in het Power Designer LDM document

        Args:
            stereo_types (list[dict]): List van stereotype objecten
            dict_domains (dict): Domain data

        Returns:
            list: Geschoonde en verrijkte stereotype objecten
        """
        stereo_types = self.clean_keys(stereo_types)
        for stereo_type in stereo_types:
            logger.debug(f"Start met het definiëren van object '{stereo_type['Name']}' in {self.file_pd_ldm}")
            stereo_type = self._object_variables(stereo_type=stereo_type, dict_domains=dict_domains)
            if stereo_type["Stereotype"] == "mdde_AggregateBusinessRule":
                stereo_type = self._object_identifiers(stereo_type=stereo_type)
            self._process_sql_expression(stereo_type=stereo_type)
            logger.debug(f"Klaar met het definiëren van object {stereo_type['Name']} in {self.file_pd_ldm}")
        return stereo_types

    def _process_sql_expression(self, stereo_type: dict):
        """Verwerkt de SQL expressie van een stereotype object en verrijkt het object met variabelen.

        Args:
            stereo_type (dict): Het stereotype object dat verrijkt wordt.
        """
        if "ExtendedAttributesText" in stereo_type:
            sql_expression = self.extract_value_from_attribute_text(
                stereo_type["ExtendedAttributesText"], preceded_by="mdde_SqlExpression,"
            )
            sql_expression_split = self._split_sql_expression(
                stereo_type=stereo_type, sql_expression=sql_expression
            )
            if sql_expression_split is not None:
                self._assign_sql_expression_fields(
                    stereo_type=stereo_type, sql_expression_split=sql_expression_split
                )

    def _split_sql_expression(self, stereo_type: dict, sql_expression: str) -> list[str] | None:
        """Splitst de SQL expressie op basis van het stereotype.

        Args:
            stereo_type (dict): Het stereotype object.
            sql_expression (str): De SQL expressie.

        Returns:
            list[str] or None: De gesplitste SQL expressie of None als niet gesplitst kan worden.
        """
        # pattern_incorrect = r"@\w+\s*=\s*@\w+"
        # matches_incorrect = re.findall(
        #     string=sql_expression, pattern=pattern_incorrect
        # )
        # if matches_incorrect:
        #     logger.error(
        #         f"Invalide expressie gevonden {', '.join(matches_incorrect)} in {objects["Name"]} in bestand {self.file_pd_ldm}"
        #     )
        if stereo_type["Stereotype"] == "mdde_FilterBusinessRule":
            # if '=' in sql_expression:
            #     lst_expression = sql_expression.split("=", 1)
            #     lst_expression.insert(1, "=")
            #     return lst_expression
            # else:
            lst_expression = sql_expression.split(" ", 1)
            return lst_expression
        elif stereo_type["Stereotype"] == "mdde_ScalarBusinessRule":
            return sql_expression.split("=", 1)
        else:
            logger.error(
                f"SqlExpression_split kan niet bepaald worden voor SqlExpression '{sql_expression}' in {self.file_pd_ldm} "
            )
            return None

    def _assign_sql_expression_fields(self, stereo_type: dict, sql_expression_split: list[str]):
        """Wijs de juiste velden toe aan het stereotype object op basis van de gesplitste SQL expressie.

        Args:
            stereo_type (dict): Het stereotype object.
            sql_expression_split (list): De gesplitste SQL expressie.
        """
        stereo_type["SqlVariable"] = sql_expression_split[0].strip()
        stereo_type["SqlExpression"] = sql_expression_split[1].strip()
        if stereo_type["Stereotype"] == "mdde_ScalarBusinessRule":
            sql_expression = sql_expression_split[1].strip()
            lst_expression_variables = self._extract_expression_variables(
                sql_expression=sql_expression
            )
            if lst_expression_variables is not None:
                stereo_type["SqlExpressionVariables"] = lst_expression_variables

    def _object_variables(self, stereo_type: dict, dict_domains: dict) -> dict:
        """Schoont de variabelen van een object en verrijkt deze met domain data

        Args:
            stereo_type (dict): Stereotype object
            dict_domains (dict): Domain data

        Returns:
            dict: Geschoond en verrijkt stereotype object
        """
        logger.debug(f"Start met het verzamelen van variabelen voor object in {self.file_pd_ldm}:  {stereo_type['Name']}")
        lst_variables = self._extract_and_clean_variables(stereo_type)
        lst_variables = self._enrich_variables_with_domains(
            variables=lst_variables, dict_domains=dict_domains
        )
        logger.debug(f"Klaar met het verzamelen van variabelen voor object in {self.file_pd_ldm}: {stereo_type['Name']}")
        stereo_type["Variables"] = lst_variables
        stereo_type.pop("c:Attributes")
        return stereo_type

    def _extract_and_clean_variables(self, stereo_type: dict) -> list[dict]:
        """Extraheert en schoont de variabelen van een object.

        Args:
            stereo_type (dict): Stereotype object

        Returns:
            list[dict]: Geschoonde variabelen
        """
        lst_variables = stereo_type["c:Attributes"]["o:EntityAttribute"]
        if isinstance(lst_variables, dict):
            lst_variables = [lst_variables]
        lst_variables = self.clean_keys(lst_variables)
        return lst_variables

    def _enrich_variables_with_domains(
        self, variables: list[dict], dict_domains: dict
    ) -> list[dict]:
        """Verrijkt de variabelen met domain data.

        Args:
            variables (list[dict]): Geschoonde variabelen
            dict_domains (dict): Domain data

        Returns:
            list[dict]: Verrijkte variabelen
        """
        for i, variable in enumerate(variables):
            variable["Order"] = i
            path_keys = ["c:Domain", "o:Domain", "@Ref"]
            if id_domain := self._get_nested(data=variable, keys=path_keys):
                attr_domain = dict_domains[id_domain]
                keys_domain = {"Id", "Name", "Code", "DataType", "Length", "Precision"}
                attr_domain = {
                    k: attr_domain[k] for k in keys_domain if k in attr_domain
                }
                variable["Domain"] = attr_domain
                variable.pop("c:Domain")
        return variables

    def _object_identifiers(self, stereo_type: dict) -> dict:
        """Schoon de identifier(s) (sleutels) van een stereotype object
        Args:
            stereo_type (dict): Stereotype object

        Returns:
            dict: Stereotype object met geschoonde identifier(s)
        """
        dict_vars = {
            d["Id"]: {"Name": d["Name"], "Code": d["Code"]} for d in stereo_type["Variables"]
        }
        has_primary, primary_id = self._check_primary_identifier(stereo_type)
        logger.debug(f"Start met het verzamelen van identifiers voor object {stereo_type['Name']} for {self.file_pd_ldm}")
        if identifiers := self._extract_identifiers(stereo_type):
            identifiers = self.clean_keys(identifiers)
            self._process_identifiers(
                identifiers, stereo_type, dict_vars, has_primary, primary_id
            )
            stereo_type["Identifiers"] = identifiers
            logger.debug(f"Klaar met het verzamelen van identifiers voor {stereo_type['Name']} in {self.file_pd_ldm}")
        return stereo_type


    def _check_primary_identifier(self, stereo_type: dict) -> tuple[bool, str | None]:
        """Controleert of een object een primaire identifier heeft en retourneert deze indien aanwezig.

        Deze functie bepaalt of het stereotype object een primaire identifier bevat en geeft de Id van deze identifier terug.

        Args:
            stereo_type (dict): Stereotype object

        Returns:
            tuple[bool, str | None]: Een tuple met een boolean die aangeeft of er een primaire identifier is en de Id van de primaire identifier (of None).
        """
        has_primary = "c:PrimaryIdentifier" in stereo_type
        primary_id = None
        if has_primary:
            primary_id = stereo_type["c:PrimaryIdentifier"]["o:Identifier"]["@Ref"]
        return has_primary, primary_id

    def _extract_identifiers(self, stereo_type: dict) -> list | None:
        """Extraheert de identifiers van een stereotype object.

        Deze functie zoekt naar identifiers in het stereotype object en retourneert deze als een lijst.

        Args:
            stereo_type (dict): Stereotype object

        Returns:
            list | None: Lijst van identifiers of None als er geen identifiers zijn gevonden.
        """
        path_keys = ["c:Identifiers", "o:Identifier"]
        if identifiers := self._get_nested(data=stereo_type, keys=path_keys):
            if isinstance(identifiers, dict):
                identifiers = [identifiers]
            return identifiers
        return None

    def _process_identifiers(
        self,
        identifiers: list[dict],
        stereo_type: dict,
        dict_vars: dict,
        has_primary: bool,
        primary_id: str | None,
    ):
        """Verwerkt en verrijkt de identifiers met entiteit- en variabele-informatie.

        Deze functie vult de identifiers aan met entiteit-informatie, koppelt variabelen en markeert de primaire identifier.

        Args:
            identifiers (list[dict]): Lijst van identifier dictionaries.
            stereo_type (dict): Het stereotype object waartoe de identifiers behoren.
            dict_vars (dict): Dictionary met variabelen op basis van hun Id.
            has_primary (bool): Of er een primaire identifier aanwezig is.
            primary_id (str | None): De Id van de primaire identifier, indien aanwezig.
        """
        for identifier in identifiers:
            identifier["EntityID"] = stereo_type["Id"]
            identifier["EntityName"] = stereo_type["Name"]
            identifier["EntityCode"] = stereo_type["Code"]
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


    def _extract_expression_variables(self, sql_expression: str) -> list[str]:
        """Split de sql expressie van een scalar in 1 of meerdere variabelen
        Args:
            sql_expression (string): volledige sql-expressie van de scalar

        Returns:
            list[str]: de individuele variabelen die samen de sql-expressie vormen
        """
        expression_variables = []
        k = sql_expression.count("@") # Count the number of placeholder variables in the SqlExpression
        expression_variable = None
        for _ in range(k):
            idx_start = sql_expression.find("@", 1)
            # Look for the first non word character after idx_start to not find the @ from the variable
            find_non_alpha = re.search("[\W]", sql_expression[idx_start + 1 :])
            # Idx_end is the start_position plus the position of the first alphanumeric character
            idx_end = idx_start + find_non_alpha.start()
            expression_variable = sql_expression[idx_start : idx_end + 1]
            expression_variables = (*expression_variables, expression_variable)
            # remove the variable from the sql_expression string
            sql_expression = sql_expression[idx_end + 1 :]
        return expression_variables
