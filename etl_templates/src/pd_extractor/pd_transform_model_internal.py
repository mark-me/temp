from log_config import logging
from .pd_transform_object import ObjectTransformer


logger = logging.getLogger(__name__)


class TransformModelInternal(ObjectTransformer):
    """ Hangt om en schoont elk onderdeel van de metadata van het model dat omgezet wordt naar DDL en ETL generatie
    """
    def __init__(self):
        super().__init__()

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
            model = self.clean_keys(model)
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

    def domains(self, lst_domains: list) -> dict:
        """Domain (data-type) gerelateerde data

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
    
    def datasources(self, lst_datasources: list) -> dict:
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
                "Code": datasource["Code"]
            }
        return dict_datasources
    
    def entities(self, lst_entities: list, dict_domains: dict) -> list:
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
            entity = self.__entity_attributes(entity=entity, dict_domains=dict_domains)
            # Create subset of attributes to enrich identifier attributes
            dict_attrs = {
                d["Id"]: {"Name": d["Name"], "Code": d["Code"]}
                for d in entity["Attributes"]
            }

            # Identifiers and primary identifier
            entity = self.__entity_identifiers(entity=entity, dict_attrs=dict_attrs)

            # Reroute default mapping
            # TODO: research role DefaultMapping
            if "c:DefaultMapping" in entity:
                entity.pop("c:DefaultMapping")
            else:
                pass
            lst_entities[i] = entity
        return lst_entities

    def __entity_attributes(self, entity: dict, dict_domains: dict) -> dict:
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
                    keys_domain = {"Id", "Name", "Code", "DataType", "Length", "Precision"}
                    attr_domain = {
                        k: attr_domain[k] for k in keys_domain if k in attr_domain
                    }
                    attr["Domain"] = attr_domain
                    attr.pop("c:Domain")
                else:
                    logger.error(f"[o:Domain] not found in attr for {attr['Code']}")
            lst_attrs[i] = attr
            entity["Attributes"] = lst_attrs
        if "c:Attributes" in entity:
            entity.pop("c:Attributes")
        elif "Variables" in entity:
            entity.pop("Variables")  
        return entity

    def __entity_identifiers(self, entity: dict, dict_attrs: dict) -> dict:
        """Omvormen van de index en primary key van een interne entiteit

        Args:
            entity (dict): Entiteit
            dict_attrs (dict): Alle entiteit attributen

        Returns:
            dict: Entiteit met geschoonde identifier (sleutels) informatie
        """
        # Set primary identifiers as an attribute of the identifiers
        has_primary = "c:PrimaryIdentifier" in entity
        if has_primary:
            primary_id = entity["c:PrimaryIdentifier"]["o:Identifier"]["@Ref"]

        # Reroute identifiers
        if "c:Identifiers" in entity:
            identifiers = entity["c:Identifiers"]["o:Identifier"]
            if isinstance(identifiers, dict):
                identifiers = [identifiers]
            identifiers = self.clean_keys(identifiers)
            # Clean and transform identifier data
            for j in range(len(identifiers)):
                identifier = identifiers[j]
                identifier["EntityID"] = entity["Id"]
                identifier["EntityName"] = entity["Name"]
                identifier["EntityCode"] = entity["Code"]
                if "c:Identifier.Attributes" not in identifier:
                    logger.error(
                        f"No attributes included in the identifier '{identifier['Name']}'"
                    )
                else:
                    lst_attr_id = identifier["c:Identifier.Attributes"][
                        "o:EntityAttribute"
                    ]
                    if isinstance(lst_attr_id, dict):
                        lst_attr_id = [lst_attr_id]
                    lst_attr_id = [dict_attrs[d["@Ref"]] for d in lst_attr_id]
                    identifier["Attributes"] = lst_attr_id
                    identifier.pop("c:Identifier.Attributes")
                # Set primary identifier attribute
                if has_primary:
                    identifier["IsPrimary"] = primary_id == identifier["Id"]
                identifiers[j] = identifier
            entity["Identifiers"] = identifiers
            if "c:Identifiers" in entity:
                entity.pop("c:Identifiers")
            if "c:PrimaryIdentifier" in entity:
                entity.pop("c:PrimaryIdentifier")
        return entity

    def relationships(self, lst_relationships: list, lst_entity: list, lst_aggregates: list) -> list:
        # TODO: added lst_aggregates as input because of reference issues due to relationships between entity and objects
        """Vormt om en verrijkt relatie data

        Args:
            lst_relationships (list): Power Designer items die een relatie beschrijven tussen entiteiten
            lst_entity (dict): Bevat alle entiteiten

        Returns:
            list: Relaties tussen model entiteiten
        """
        # TODO: Added lst_relationship_entity because of reference issues. Combined the lst_entity and lst_aggregates
        lst_relationship_entity = lst_entity #+ lst_aggregates

        # Creating dictionaries to simplify adding data to relationships
        dict_entities = {entity["Id"]: entity for entity in lst_relationship_entity}

        # TODO: added dict_variables because of reference issues.
        # dict_variables = {
        #     variables["Id"]: variables for aggregate in lst_aggregates for variables in aggregate["Variables"]
        # }
        dict_attributes = {
            attr["Id"]: attr for entity in lst_entity for attr in entity["Attributes"]
        }
        # TODO: dict_attributes is a combination of attributes (from entities) and variables (from objects)
        # dict_attributes = dict_attributes | dict_variables
        # TODO: if we support relationships between entities and objects we'll have to add the identifiers from objects as well
        dict_identifiers = {
            ids["Id"]: ids
            for entity in lst_relationship_entity
            if "Identifiers" in entity
            for ids in entity["Identifiers"]
        }

        # Processing relationships
        lst_relationships = self.clean_keys(lst_relationships)
        if isinstance(lst_relationships, dict):
            lst_relationships = [lst_relationships]
        for i in range(len(lst_relationships)):
            relationship = lst_relationships[i]
            # Add entity data
            self.__relationship_entities(
                relationship=relationship, dict_entities=dict_entities
            )
            # Add attribute data
            relationship = self.__relationship_join(
                relationship=relationship, dict_attributes=dict_attributes
            )
            # Add identifier data
            relationship = self.__relationship_identifiers(
                relationship=relationship, dict_identifiers=dict_identifiers
            )
            lst_relationships[i] = relationship

        return lst_relationships

    def __relationship_entities(self, relationship: dict, dict_entities: dict) -> dict:
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
            logger.warning(f"{id_entity} for relationship[Entity1] not in dict_entities")
        if "o:Entity" in relationship["c:Object2"]:
            id_entity = relationship["c:Object2"]["o:Entity"]["@Ref"]
        elif "o:Shortcut" in relationship["c:Object2"]:
            id_entity = relationship["c:Object2"]["o:Shortcut"]["@Ref"]
        if id_entity in dict_entities:
            relationship["Entity2"] = dict_entities[id_entity]
            relationship.pop("c:Object2")
        else:
            logger.warning(f"{id_entity} for relationship[Entity2] not in dict_entities")
        return relationship

    def __relationship_join(self, relationship: dict, dict_attributes: dict) -> dict:
        """Vormt om en voegt attributen van de entiteiten toe aan joins

        Args:
            relationship (dict): De relaties die de join(s) bevatten
            dict_attributes (dict): Attributen die gebruikt worden om de set te verrijken

        Returns:
            dict: De geschoonde versie van de join(s) behorende bij de relatie
        """
        if "c:Joins" in  relationship:
            lst_joins = relationship["c:Joins"]["o:RelationshipJoin"]
            if isinstance(lst_joins, dict):
                lst_joins = [lst_joins]
            lst_joins = self.clean_keys(lst_joins)
            for i in range(len(lst_joins)):
                join = {}
                join["Order"] = i
                if "o:EntityAttribute" in lst_joins[i]["c:Object1"]:
                    id_attr = lst_joins[i]["c:Object1"]["o:EntityAttribute"]["@Ref"]
                elif "o:Shortcut" in lst_joins[i]["c:Object1"]:
                    id_attr = lst_joins[i]["c:Object1"]["o:Shortcut"]["@Ref"]
                    logger.warning(f"Relationship:{relationship['Name']} , missing relationship join")
                else:
                    logger.warning(f"{relationship['Name']} missing relationship join")
                if id_attr in dict_attributes:
                    join["Entity1Attribute"] = dict_attributes[id_attr]
                else:
                    logger.warning(f"{id_attr} for join[Entity1Attribute] not in dict_acttributes")
                id_attr = lst_joins[i]["c:Object2"]["o:EntityAttribute"]["@Ref"]
                if id_attr in dict_attributes:
                    join["Entity2Attribute"] = dict_attributes[id_attr]
                    lst_joins[i] = join
                else:
                    logger.warning(f"{id_attr} for join[Entity2Attribute] not in dict_acttributes")
            relationship["Joins"] = lst_joins
            relationship.pop("c:Joins")
        else:
            logger.warning(f"Relationship:{relationship['Name']} , missing relationship join")           
        return relationship

    def __relationship_identifiers(self, relationship: dict, dict_identifiers: dict) -> dict:
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
                    logger.warning(f"{lst_identifier_id} for relationship[Identifiers] not in dict_acttributes")          
            else:
                logger.warning(f"{relationship['Name']} : missing identifier for relationship")
            relationship.pop("c:ParentIdentifier") 
        else:
            logger.warning(f"{relationship['Name']} : missing parent for relationship join")
        return relationship
