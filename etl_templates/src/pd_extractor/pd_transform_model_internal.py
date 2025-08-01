from logtools import get_logger

from .pd_transform_object import ObjectTransformer

logger = get_logger(__name__)


class TransformModelInternal(ObjectTransformer):
    """Hangt om en schoont elk onderdeel van de metadata van het model dat omgezet wordt naar DDL en ETL generatie"""

    def __init__(self, file_pd_ldm: str):
        super().__init__(file_pd_ldm)
        self.file_pd_ldm = file_pd_ldm

    def model(self, content: dict) -> dict:
        """Model generieke data

        Args:
            content (dict): Power Designer data op het niveau van het model

        Returns:
            dict: Geschoonde versie van de data op het niveau van het model
        """
        content = self.convert_timestamps(content)
        if "c:GenerationOrigins" in content:
            model = content["c:GenerationOrigins"]["o:Shortcut"]  # Document model
        else:
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
        """Domain (list) gerelateerde data

        Input:
            lst_domains (list): Power Designer domain data

        Returns:
            dict: Geschoonde en omgehangen domains (data-types)
        """
        dict_domains = {}
        if isinstance(lst_domains, dict):
            lst_domains = [lst_domains]
        lst_domains = self.convert_timestamps(lst_domains)
        lst_domains = self.clean_keys(lst_domains)
        for domain in lst_domains:
            dict_domains[domain["Id"]] = domain
        return dict_domains

    def datasources(self, lst_datasources: list[dict]) -> dict:
        """Datasource gerelateerde data

        Args:
            lst_datasources (list): Datasource data

        Returns:
            dict: Geschoonde datasource data (Id, naam en code) te gebruiken in model en mapping
        """
        dict_datasources = {}
        if isinstance(lst_datasources, dict):
            lst_datasources = [lst_datasources]
        lst_datasources = self.clean_keys(lst_datasources)
        for datasource in lst_datasources:
            dict_datasources[datasource["Id"]] = {
                "Id": datasource["Id"],
                "Name": datasource["Name"],
                "Code": datasource["Code"],
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
        if "c:Attributes" in entity:
            lst_attrs = entity["c:Attributes"]["o:EntityAttribute"]
        elif "Variables" in entity:
            lst_attrs = entity["Variables"]
        if isinstance(lst_attrs, dict):
            lst_attrs = [lst_attrs]
        lst_attrs = self.clean_keys(lst_attrs)
        for i in range(len(lst_attrs)):
            # Change domain data
            attr = lst_attrs[i]
            attr["Order"] = i
            if "c:Domain" in attr:
                # Reroute domain data
                if "o:Domain" in attr["c:Domain"]:
                    id_domain = attr["c:Domain"]["o:Domain"]["@Ref"]

                    # Add matching domain data
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
                        k: attr_domain[k] for k in keys_domain if k in attr_domain
                    }
                    attr["Domain"] = attr_domain
                    attr.pop("c:Domain")
                else:
                    logger.error(f"[o:Domain] niet gevonden in attribuut voor {attr['Code']} in {self.file_pd_ldm}")
            lst_attrs[i] = attr
            entity["Attributes"] = lst_attrs
        if "c:Attributes" in entity:
            entity.pop("c:Attributes")
        elif "Variables" in entity:
            entity.pop("Variables")
        return entity

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

    def _extract_primary_identifier(self, entity: dict):
        """Haalt de primaire identifier uit een entiteit en verwijdert deze.

        Bepaalt of de entiteit een primaire identifier heeft en geeft de referentie terug.
        Verwijdert de primaire identifier uit de entiteit indien aanwezig, en logt een:
          * Waarschuwing bij entiteiten met een Stereotype
          * Fout bij een 'reguliere' entiteit

        Args:
            entity (dict): De entiteit waaruit de primaire identifier wordt gehaald.

        Returns:
            tuple: (has_primary, primary_id) waarbij has_primary een bool is en primary_id de identifier referentie of None.
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

        identifiers = entity["c:Identifiers"]["o:Identifier"]
        if isinstance(identifiers, dict):
            identifiers = [identifiers]
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

        lst_attr_id = identifier["c:Identifier.Attributes"]["o:EntityAttribute"]
        if isinstance(lst_attr_id, dict):
            lst_attr_id = [lst_attr_id]
        lst_attr_id = [dict_attrs[d["@Ref"]] for d in lst_attr_id]
        identifier["Attributes"] = lst_attr_id
        identifier.pop("c:Identifier.Attributes")

    def relationships(self, lst_relationships: list[dict], lst_entity: list[dict]) -> list[dict]:
        """Vormt om en verrijkt relatie data

        Args:
            lst_relationships (list[dict]): Power Designer items die een relatie beschrijven tussen entiteiten
            lst_entity (dict): Bevat alle entiteiten

        Returns:
            list[dict]: Relaties tussen model entiteiten
        """
        # Creating dictionaries to simplify adding data to relationships
        dict_entities = {entity["Id"]: entity for entity in lst_entity}

        dict_attributes = {
            attr["Id"]: attr for entity in lst_entity for attr in entity["Attributes"]
        }
        dict_identifiers = {
            entity["KeyPrimary"]["Id"]: entity["KeyPrimary"]
            for entity in lst_entity
            if "KeyPrimary" in entity
        } | {
            ids["Id"]: ids
            for entity in lst_entity
            if "KeysForeign" in entity
            for ids in entity["KeysForeign"]
        }
        # Processing relationships
        lst_relationships = self.clean_keys(lst_relationships)
        if isinstance(lst_relationships, dict):
            lst_relationships = [lst_relationships]
        for i in range(len(lst_relationships)):
            relationship = lst_relationships[i]
            # Add entity data
            self._relationship_entities(
                relationship=relationship, dict_entities=dict_entities
            )
            # Add attribute data
            relationship = self._relationship_join(
                relationship=relationship, dict_attributes=dict_attributes
            )
            # Add identifier data
            relationship = self._relationship_identifiers(
                relationship=relationship, dict_identifiers=dict_identifiers
            )
            lst_relationships[i] = relationship

        return lst_relationships

    def _relationship_entities(self, relationship: dict, dict_entities: dict) -> dict:
        """Vormt om en hernoemt de entiteiten beschreven in de relatie

        Args:
            relationship (dict): Het Power Designer document onderdeel dat de relaties beschrijft
            dict_entities (dict): Alle entiteiten

        Returns:
            dict: De geschoonde versie van de relatie data
        """
        id_entity = relationship["c:Object1"]["o:Entity"]["@Ref"]
        if id_entity in dict_entities:
            relationship["Entity1"] = dict_entities[id_entity]
            relationship.pop("c:Object1")
        else:
            logger.warning(
                f"{id_entity} voor relationship[Entity1] niet in dict_entities in {self.file_pd_ldm}"
            )
        if "o:Entity" in relationship["c:Object2"]:
            id_entity = relationship["c:Object2"]["o:Entity"]["@Ref"]
        elif "o:Shortcut" in relationship["c:Object2"]:
            id_entity = relationship["c:Object2"]["o:Shortcut"]["@Ref"]
        if id_entity in dict_entities:
            relationship["Entity2"] = dict_entities[id_entity]
            relationship.pop("c:Object2")
        else:
            logger.warning(
                f"{id_entity} voor relationship[Entity2] niet in dict_entities in {self.file_pd_ldm} "
            )
        return relationship

    def _relationship_join(self, relationship: dict, dict_attributes: dict) -> dict:
        """Vormt om en voegt attributen van de entiteiten toe aan joins

        Args:
            relationship (dict): De relaties die de join(s) bevatten
            dict_attributes (dict): Attributen die gebruikt worden om de set te verrijken

        Returns:
            dict: De geschoonde versie van de join(s) behorende bij de relatie
        """
        if "c:Joins" in relationship:
            lst_joins = relationship["c:Joins"]["o:RelationshipJoin"]
            if isinstance(lst_joins, dict):
                lst_joins = [lst_joins]
            lst_joins = self.clean_keys(lst_joins)
            for i in range(len(lst_joins)):
                join = {"Order": i}
                if "o:EntityAttribute" in lst_joins[i]["c:Object1"]:
                    id_attr = lst_joins[i]["c:Object1"]["o:EntityAttribute"]["@Ref"]
                elif "o:Shortcut" in lst_joins[i]["c:Object1"]:
                    id_attr = lst_joins[i]["c:Object1"]["o:Shortcut"]["@Ref"]
                    logger.warning(
                        f"Relationship:{relationship['Name']}, join relatie ontbreekt in {self.file_pd_ldm}"
                    )
                else:
                    logger.warning(f"{relationship['Name']} join relatie ontbreekt in {self.file_pd_ldm}")
                if id_attr in dict_attributes:
                    join |= {"Entity1Attribute": dict_attributes[id_attr]}
                else:
                    logger.warning(
                        f"{id_attr} voor join[Entity1Attribute] niet in dict_acttributes"
                    )
                id_attr = lst_joins[i]["c:Object2"]["o:EntityAttribute"]["@Ref"]
                if id_attr in dict_attributes:
                    join |= {"Entity2Attribute": dict_attributes[id_attr]}
                    lst_joins[i] = join
                else:
                    logger.warning(
                        f"{id_attr} voor join[Entity2Attribute] niet in dict_acttributes in {self.file_pd_ldm}"
                    )
            relationship["Joins"] = lst_joins
            relationship.pop("c:Joins")
        else:
            logger.warning(
                f"Relationship:{relationship['Name']}, join relatie ontbreekt in {self.file_pd_ldm}"
            )
        return relationship

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
        if "c:ParentIdentifier" in relationship:
            if "o:Identifier" in relationship["c:ParentIdentifier"]:
                lst_identifier_id = relationship["c:ParentIdentifier"]["o:Identifier"]
                if lst_identifier_id["@Ref"] in dict_identifiers:
                    if isinstance(lst_identifier_id, dict):
                        lst_identifier_id = [lst_identifier_id]
                    relationship["Identifiers"] = [
                        dict_identifiers[id["@Ref"]] for id in lst_identifier_id
                    ]
                else:
                    logger.warning(
                        f"{lst_identifier_id} voor relationship[Identifiers] niet in dict_acttributes in {self.file_pd_ldm}"
                    )
            else:
                logger.warning(
                    f"{relationship['Name']} : identifier mist voor relatie in {self.file_pd_ldm}"
                )
            relationship.pop("c:ParentIdentifier")
        else:
            logger.warning(
                f"{relationship['Name']} : parent mist voor relatie join in {self.file_pd_ldm}"
            )
        return relationship
