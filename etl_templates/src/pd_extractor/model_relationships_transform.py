from logtools import get_logger

from .base_transformer import BaseTransformer

logger = get_logger(__name__)


class RelationshipsTransformer(BaseTransformer):
    def __init__(self, file_pd_ldm: str, lst_entities: list[dict]):
        super().__init__(file_pd_ldm)
        self.lst_entities = lst_entities

    def transform(self, lst_relationships: list[dict]) -> list[dict]:
        """Vormt om en verrijkt relatie data

        Deze functie verwerkt een lijst van relaties tussen entiteiten, verrijkt deze met entiteit-, attribuut- en identifier-informatie,
        en filtert relaties naar externe modellen uit.

        Args:
            lst_relationships (list[dict]): Power Designer items die een relatie beschrijven tussen entiteiten
            lst_entity (dict): Bevat alle entiteiten

        Returns:
            list[dict]: Relaties tussen model entiteiten
        """

        lst_relationships = self.clean_keys(lst_relationships)
        lst_relationships = (
            [lst_relationships]
            if isinstance(lst_relationships, dict)
            else lst_relationships
        )
        for i, relationship in enumerate(lst_relationships):
            relationship = self._process_relationship(relationship=relationship)
            lst_relationships[i] = relationship
        lst_relationships = [x for x in lst_relationships if x is not None]
        return lst_relationships

    def _build_attributes_dict(self) -> dict:
        """Bouwt een dictionary van attributen op basis van hun Id."""
        return {
            attr["Id"]: attr
            for entity in self.lst_entities
            if "Attributes" in entity
            for attr in entity["Attributes"]
            if "Id" in attr
        }

    def _process_relationship(self, relationship: dict) -> dict:
        """Verwerkt een enkele relatie: entiteiten, joins en identifiers."""
        dict_entities = {
            entity["Id"]: entity for entity in self.lst_entities if "Id" in entity
        }
        dict_attributes = self._build_attributes_dict()
        dict_identifiers = self._build_identifiers_dict()
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

    def _build_identifiers_dict(self) -> dict:
        """Bouwt een dictionary van identifiers (primaire en vreemde sleutels) op basis van hun Id."""
        return {
            entity["KeyPrimary"]["Id"]: entity["KeyPrimary"]
            for entity in self.lst_entities
            if "KeyPrimary" in entity
        } | {
            ids["Id"]: ids
            for entity in self.lst_entities
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
