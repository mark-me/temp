from logtools import get_logger

from .transformer_base import TransformerBase

logger = get_logger(__name__)


class TransformModelInternal(TransformerBase):
    """Hangt om en schoont elk onderdeel van de metadata van het model dat omgezet wordt naar DDL en ETL generatie"""

    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)

    def model(self, content: dict) -> dict:
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

    def domains(self, lst_domains: list[dict]) -> dict:
        """Verwerkt en schoont domein data uit het Power Designer model.

        Deze functie converteert timestamps, maakt de domein data schoon en retourneert een dictionary met domeinen.

        Args:
            lst_domains (list[dict]): Lijst van domeinen uit het Power Designer model.

        Returns:
            dict: Dictionary met domeinen, waarbij de sleutel het domein-ID is.
        """
        if isinstance(lst_domains, dict):
            lst_domains = [lst_domains]
        lst_domains = self.convert_timestamps(lst_domains)
        lst_domains = self.clean_keys(lst_domains)
        dict_domains = {
            domain["Id"]: domain for domain in lst_domains if "Id" in domain
        }
        return dict_domains

    def datasources(self, lst_datasources: list[dict]) -> dict:
        """Datasource gerelateerde data

        Args:
            lst_datasources (list): Datasource data

        Returns:
            dict: Geschoonde datasource data (Id, naam en code) te gebruiken in model en mapping
        """

        if isinstance(lst_datasources, dict):
            lst_datasources = [lst_datasources]
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

    def entities(self, lst_entities: list[dict], dict_domains: dict) -> list:
        """Omvormen van data van interne entiteiten en verrijkt de attributen met domain data

        Args:
            lst_entities (list): Het deel van het PowerDesigner document dat entiteiten beschrijft
            dict_domains (dict): Alle domains (oftewel datatypes gebruikt voor attributen)

        Returns:
            list: Alle entities
        """
        lst_entities = self.clean_keys(lst_entities)
        for i in range(len(lst_entities)):
            entity = lst_entities[i]

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

            lst_entities[i] = entity
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

    def relationships(
        self, lst_relationships: list[dict], lst_entity: list[dict]
    ) -> list[dict]:
        """Vormt om en verrijkt relatie data

        Deze functie verwerkt een lijst van relaties tussen entiteiten, verrijkt deze met entiteit-, attribuut- en identifier-informatie,
        en filtert relaties naar externe modellen uit.

        Args:
            lst_relationships (list[dict]): Power Designer items die een relatie beschrijven tussen entiteiten
            lst_entity (dict): Bevat alle entiteiten

        Returns:
            list[dict]: Relaties tussen model entiteiten
        """
        dict_entities = {
            entity["Id"]: entity for entity in lst_entity if "Id" in entity
        }
        dict_attributes = self._build_attributes_dict(lst_entity)
        dict_identifiers = self._build_identifiers_dict(lst_entity)
        lst_relationships = self.clean_keys(lst_relationships)
        lst_relationships = (
            [lst_relationships]
            if isinstance(lst_relationships, dict)
            else lst_relationships
        )
        for i, relationship in enumerate(lst_relationships):
            relationship = self._process_relationship(
                relationship=relationship,
                dict_entities=dict_entities,
                dict_attributes=dict_attributes,
                dict_identifiers=dict_identifiers,
            )
            lst_relationships[i] = relationship
        lst_relationships = [x for x in lst_relationships if x is not None]
        return lst_relationships

    def _build_attributes_dict(self, lst_entity: list[dict]) -> dict:
        """Bouwt een dictionary van attributen op basis van hun Id."""
        return {
            attr["Id"]: attr
            for entity in lst_entity
            if "Attributes" in entity
            for attr in entity["Attributes"]
            if "Id" in attr
        }

    def _process_relationship(
        self,
        relationship: dict,
        dict_entities: dict,
        dict_attributes: dict,
        dict_identifiers: dict,
    ) -> dict:
        """Verwerkt een enkele relatie: entiteiten, joins en identifiers."""
        if relationship := self._relationship_entities(
            relationship=relationship, dict_entities=dict_entities
        ):
            relationship = self._relationship_join(
                relationship=relationship, dict_attributes=dict_attributes
            )
            relationship = self._relationship_identifiers(
                relationship=relationship, dict_identifiers=dict_identifiers
            )
            return relationship
        return None

    def _build_identifiers_dict(self, lst_entity: list[dict]) -> dict:
        """Bouwt een dictionary van identifiers (primaire en vreemde sleutels) op basis van hun Id."""
        return {
            entity["KeyPrimary"]["Id"]: entity["KeyPrimary"]
            for entity in lst_entity
            if "KeyPrimary" in entity
        } | {
            ids["Id"]: ids
            for entity in lst_entity
            if "KeysForeign" in entity
            for ids in entity["KeysForeign"]
        }

    def _relationship_entities(self, relationship: dict, dict_entities: dict) -> dict:
        """Vormt om en hernoemt de entiteiten beschreven in de relatie

        Args:
            relationship (dict): Het Power Designer document onderdeel dat de relaties beschrijft
            dict_entities (dict): Alle entiteiten

        Returns:
            dict: De geschoonde versie van de relatie data
        """
        relationship_with_external = False
        id_entity = self._get_nested(
            data=relationship, keys=["c:Object1", "o:Entity", "@Ref"]
        )
        if id_entity in dict_entities:
            relationship["Entity1"] = dict_entities[id_entity]
            relationship.pop("c:Object1")
        else:
            relationship_with_external = True

        if "o:Entity" in relationship["c:Object2"]:
            id_entity = relationship["c:Object2"]["o:Entity"]["@Ref"]
        elif "o:Shortcut" in relationship["c:Object2"]:
            id_entity = relationship["c:Object2"]["o:Shortcut"]["@Ref"]
        if id_entity in dict_entities:
            relationship["Entity2"] = dict_entities[id_entity]
            relationship.pop("c:Object2")
        else:
            relationship_with_external = True

        if relationship_with_external:
            logger.warning(
                f"Relatie '{relationship['Name']}' beschrijft een relatie naar een extern model in '{self.file_pd_ldm}', wordt niet verwerkt"
            )
            relationship = None
        return relationship

    def _relationship_join(self, relationship: dict, dict_attributes: dict) -> dict:
        """Vormt om en voegt attributen van de entiteiten toe aan joins

        Args:
            relationship (dict): De relaties die de join(s) bevatten
            dict_attributes (dict): Attributen die gebruikt worden om de set te verrijken

        Returns:
            dict: De geschoonde versie van de join(s) behorende bij de relatie
        """
        if lst_joins := self._get_nested(
            data=relationship, keys=["c:Joins", "o:RelationshipJoin"]
        ):
            lst_joins = [lst_joins] if isinstance(lst_joins, dict) else lst_joins
            lst_joins = self.clean_keys(lst_joins)
            for i in range(len(lst_joins)):
                join = self._build_join_dict(
                    join_data=lst_joins[i],
                    order=i,
                    relationship=relationship,
                    dict_attributes=dict_attributes,
                )
                lst_joins[i] = join
            relationship["Joins"] = lst_joins
            relationship.pop("c:Joins")
        else:
            logger.warning(
                f"Relationship:{relationship['Name']}, join relatie ontbreekt in {self.file_pd_ldm}"
            )
        return relationship

    def _build_join_dict(
        self, join_data: dict, order: int, relationship: dict, dict_attributes: dict
    ) -> dict:
        """Bouwt een join dictionary op basis van de join data en verrijkt deze met attributen."""
        join = {"Order": order}
        entity1_attr = self._get_entity1_attribute(
            join_data=join_data,
            relationship=relationship,
            dict_attributes=dict_attributes,
        )
        if entity1_attr:
            join |= {"Entity1Attribute": entity1_attr}
        if entity2_attr := self._get_entity2_attribute(
            join_data=join_data, dict_attributes=dict_attributes
        ):
            join |= {"Entity2Attribute": entity2_attr}
        return join

    def _get_entity1_attribute(
        self, join_data: dict, relationship: dict, dict_attributes: dict
    ):
        """Haalt het attribuut voor Entity1 op en logt indien niet gevonden."""
        id_attr = self._extract_entity1_attribute_id(join_data, relationship)
        if id_attr is None:
            logger.warning(
                "id_attr is None voor join[Entity1Attribute], attribuut ontbreekt mogelijk in join_data"
            )
            return None
        if id_attr in dict_attributes:
            return dict_attributes[id_attr]
        logger.warning(f"{id_attr} voor join[Entity1Attribute] niet in dict_attributes")
        return None

    def _get_entity2_attribute(self, join_data: dict, dict_attributes: dict):
        """Haalt het attribuut voor Entity2 op en logt indien niet gevonden."""
        id_attr = self._get_nested(
            data=join_data, keys=["c:Object2", "o:EntityAttribute", "@Ref"]
        )
        if id_attr in dict_attributes:
            return dict_attributes[id_attr]
        logger.warning(
            f"{id_attr} voor join[Entity2Attribute] niet in dict_acttributes in {self.file_pd_ldm}"
        )
        return None

    def _extract_entity1_attribute_id(
        self, join_data: dict, relationship: dict
    ) -> str | None:
        """Extraheert het attribuut-ID voor Entity1 uit de join data."""
        if join_entity := self._get_nested(
            join_data, keys=["c:Object1", "o:EntityAttribute", "@Ref"]
        ):
            return join_entity
        elif join_entity := self._get_nested(
            data=join_data, keys=["c:Object1", "o:Shortcut", "@Ref"]
        ):
            logger.warning(
                f"Relationship:{relationship['Name']}, join relatie ontbreekt in {self.file_pd_ldm}"
            )
            return join_entity
        else:
            logger.warning(
                f"{relationship['Name']} join relatie ontbreekt in {self.file_pd_ldm}"
            )
            return None

    def _relationship_identifiers(
        self, relationship: dict, dict_identifiers: dict
    ) -> dict:
        """Schoont, vormt om identifiers (sleutels) die onderdeel zijn van de relatie en voegt deze toe

        Args:
            relationship (dict): Relaties
            dict_identifiers (dict): Alle identifiers

        Returns:
            dict: Relatie verrijkt met geschoonde identifier data
        """
        path_keys = ["c:ParentIdentifier", "o:Identifier", "@Ref"]
        if lst_identifier_id := self._get_nested(data=relationship, keys=path_keys):
            if isinstance(lst_identifier_id, str):
                lst_identifier_id = [lst_identifier_id]
            relationship["Identifiers"] = [
                dict_identifiers[id] for id in lst_identifier_id
            ]
            relationship.pop("c:ParentIdentifier")
        else:
            logger.warning(
                f"{relationship['Name']} : parent mist voor relatie join in {self.file_pd_ldm}"
            )
        return relationship
