from logtools import get_logger

from .base_transformer import TransformerBase

logger = get_logger(__name__)


class TransformModelInternal(TransformerBase):
    """Hangt om en schoont elk onderdeel van de metadata van het model dat omgezet wordt naar DDL en ETL generatie"""

    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def transform(self, content: dict) -> dict:
        """Model generieke data

        Args:
            content (dict): Power Designer data op het niveau van het model

        Returns:
            dict: Geschoonde versie van de data op het niveau van het model
        """
        content = self.convert_timestamps(content)
        path_keys = ["c:GenerationOrigins", "o:Shortcut"]
        model = self._get_nested(data=content, keys=path_keys)
        if not model:
            lst_include = [
                "@Id",
                "@a:ObjectID",
                "a:Name",
                "a:Code",
                "a:CreationDate",
                "a:Creator",
                "a:ModificationDate",
                "a:Modifier",
                "a:PackageOptionsText",
                "a:ModelOptionsText",
                "a:Author",
                "a:Version",
                "a:RepositoryFilename",
                "a:ExtendedAttributesText",
            ]
            model = {item: content[item] for item in content if item in lst_include}
        model = self.clean_keys(model)
        model["IsDocumentModel"] = True
        return model

    def transform_datasources(self, lst_datasources: list[dict]) -> dict:
        """Datasource gerelateerde data

        Args:
            lst_datasources (list): Datasource data

        Returns:
            dict: Geschoonde datasource data (Id, naam en code) te gebruiken in model en mapping
        """
        lst_datasources = (
            [lst_datasources] if isinstance(lst_datasources, dict) else lst_datasources
        )
        lst_datasources = self.clean_keys(lst_datasources)
        dict_datasources = {
            datasource["Id"]: {
                "Id": datasource.get("Id"),
                "Name": datasource.get("Name"),
                "Code": datasource.get("Code"),
            }
            for datasource in lst_datasources
        }
        return dict_datasources

    def transform_entities(self, lst_entities: list[dict], dict_domains: dict) -> list:
        """Omvormen van data van interne entiteiten en verrijkt de attributen met domain data

        Args:
            lst_entities (list): Het deel van het PowerDesigner document dat entiteiten beschrijft
            dict_domains (dict): Alle domains (oftewel datatypes gebruikt voor attributen)

        Returns:
            list: Alle entities
        """
        lst_entities = self.clean_keys(lst_entities)
        for entity in lst_entities:
            # Reroute attributes
            entity = self._entity_attributes(entity=entity, dict_domains=dict_domains)
            # Create subset of attributes to enrich identifier attributes
            dict_attrs = {
                d["Id"]: {"Name": d["Name"], "Code": d["Code"]}
                for d in entity["Attributes"]
            }

            # Identifiers and primary identifier
            entity = self._entity_identifiers(entity=entity, dict_attrs=dict_attrs)

            # Reroute default mapping
            if "c:DefaultMapping" in entity:
                entity.pop("c:DefaultMapping")
            if "c:AttachedKeywords" in entity:
                entity.pop("c:AttachedKeywords")
        return lst_entities

    def _entity_attributes(self, entity: dict, dict_domains: dict) -> dict:
        """Omvormen van attribuut data voor de interne entiteiten en verrijkt deze met domain data

        Args:
            entity (dict): Interne entiteiten inclusief aggregaten
            dict_domains (list): Alle domains

        Returns:
            dict: Entiteit met geschoonde attribuut data
        """
        lst_attrs = self._extract_entity_attributes(entity)
        lst_attrs = self.clean_keys(lst_attrs)
        for i in range(len(lst_attrs)):
            attr = lst_attrs[i]
            attr["Order"] = i
            attr = self._enrich_attribute_with_domain(attr, dict_domains)
            lst_attrs[i] = attr
        entity["Attributes"] = lst_attrs
        if "c:Attributes" in entity:
            entity.pop("c:Attributes")
        elif "Variables" in entity:
            entity.pop("Variables")
        return entity

    def _extract_entity_attributes(self, entity: dict):
        """Extraheert de lijst van attributen uit een entiteit.

        Args:
            entity (dict): De entiteit waaruit attributen worden gehaald.

        Returns:
            list: Lijst van attributen.
        """
        if "c:Attributes" in entity:
            lst_attrs = entity["c:Attributes"]["o:EntityAttribute"]
        elif "Variables" in entity:
            lst_attrs = entity["Variables"]
        else:
            lst_attrs = []
        if isinstance(lst_attrs, dict):
            lst_attrs = [lst_attrs]
        return lst_attrs

    def _enrich_attribute_with_domain(self, attr: dict, dict_domains: dict) -> dict:
        """Verrijkt een attribuut met domein data indien aanwezig.

        Args:
            attr (dict): Het attribuut dat verrijkt moet worden.
            dict_domains (dict): Dictionary met alle domeinen.

        Returns:
            dict: Het verrijkte attribuut.
        """
        if id_domain := self._get_nested(
            data=attr, keys=["c:Domain", "o:Domain", "@Ref"]
        ):
            attr_domain = dict_domains[id_domain]
            keys_domain = {
                "Id",
                "Name",
                "Code",
                "DataType",
                "Length",
                "Precision",
            }
            attr_domain = {
                k: attr_domain.get(k) for k in keys_domain if k in attr_domain
            }
            attr["Domain"] = attr_domain
            attr.pop("c:Domain")
        return attr

    def _entity_identifiers(self, entity: dict, dict_attrs: dict) -> dict:
        """Omvormen en schoonmaken van de index en primary key van een interne entiteit.

        Deze functie verwerkt de identifiers van een entiteit, verrijkt deze met attribuutdata en splitst ze in primaire en vreemde sleutels.

        Args:
            entity (dict): Entiteit die verwerkt moet worden.
            dict_attrs (dict): Alle entiteit attributen.

        Returns:
            dict: Entiteit met geschoonde en gestructureerde identifier (sleutel) informatie.
        """

        primary_key = None
        lst_foreign_keys = []

        has_primary, primary_id = self._extract_primary_identifier(entity)
        if not has_primary:
            logger.error(
                f"Entiteit '{entity['Name']}' uit '{self.file_pd_ldm}' heeft geen primaire sleutel"
            )

        if "c:Identifiers" not in entity:
            return entity

        identifiers = self._prepare_identifiers(entity)

        for identifier in identifiers:
            if not self._has_identifier_attributes(identifier, entity):
                continue

            self._enrich_identifier_with_attributes(identifier, dict_attrs)

            if has_primary and primary_id == identifier["Id"]:
                primary_key = identifier
            else:
                lst_foreign_keys.append(identifier)

        entity["KeyPrimary"] = primary_key
        entity["KeysForeign"] = lst_foreign_keys
        entity.pop("c:Identifiers")

        return entity

    def _extract_primary_identifier(self, entity: dict) -> tuple[bool, str]:
        """Haalt de primaire identifier (primary key) van een entiteit op.

        Deze functie controleert of een entiteit een primaire sleutel heeft, haalt het ID op en verwijdert de sleutel uit de entiteit.

        Args:
            entity (dict): De entiteit waarvan de primaire identifier wordt opgehaald.

        Returns:
            tuple[bool, str]: Een tuple met een boolean of de primaire sleutel aanwezig is en het ID van de primaire sleutel (of None).
        """
        has_primary = "c:PrimaryIdentifier" in entity
        primary_id = None
        msg_missing = f"Entiteit '{entity['Name']}' heeft geen primary key voor {self.file_pd_ldm}."
        if has_primary:
            primary_id = entity["c:PrimaryIdentifier"]["o:Identifier"]["@Ref"]
            entity.pop("c:PrimaryIdentifier")
        elif "Stereotype" in entity:
            logger.warning(msg_missing)
        else:
            logger.error(msg_missing)
        return has_primary, primary_id

    def _prepare_identifiers(self, entity: dict) -> list[dict]:
        """Bereidt de lijst van identifiers voor en maakt deze schoon.

        Deze functie normaliseert de structuur van identifiers en past key-cleaning toe voor verdere verwerking.

        Args:
            entity (dict): De entiteit met identifier data.

        Returns:
            list[dict]: Een geschoonde lijst van identifier dictionaries.
        """

        identifiers = self._get_nested(
            data=entity, keys=["c:Identifiers", "o:Identifier"]
        )
        identifiers = [identifiers] if isinstance(identifiers, dict) else identifiers
        return self.clean_keys(identifiers)

    def _has_identifier_attributes(self, identifier: dict, entity: dict) -> bool:
        """Controleert of de identifier attribuut-informatie bevat.

        Geeft True terug als de identifier attribuut-data heeft, logt anders een foutmelding en geeft False terug.

        Args:
            identifier (dict): De identifier die gecontroleerd wordt op attributen.
            entity (dict): De entiteit waartoe de identifier behoort.

        Returns:
            bool: True als attributen aanwezig zijn, anders False.
        """

        if "c:Identifier.Attributes" not in identifier:
            logger.error(
                f"Geen attributes gevonden voor de identifier '{identifier['Name']}' in entiteit '{entity['Name']} in {self.file_pd_ldm}'"
            )
            return False
        return True

    def _enrich_identifier_with_attributes(self, identifier: dict, dict_attrs: dict):
        """Verrijkt een identifier met attribuut-informatie op basis van een attribuut-dictionary.

        Deze functie voegt de bijbehorende attributen toe aan de identifier zodat deze eenvoudig verder verwerkt kan worden.

        Args:
            identifier (dict): De identifier die verrijkt moet worden.
            dict_attrs (dict): Dictionary met referenties naar attribuut-data.
        """

        lst_attr_id = self._get_nested(
            data=identifier, keys=["c:Identifier.Attributes", "o:EntityAttribute"]
        )
        lst_attr_id = [lst_attr_id] if isinstance(lst_attr_id, dict) else lst_attr_id
        lst_attr_id = [dict_attrs[d["@Ref"]] for d in lst_attr_id if "@Ref" in d]
        identifier["Attributes"] = lst_attr_id
        identifier.pop("c:Identifier.Attributes")
