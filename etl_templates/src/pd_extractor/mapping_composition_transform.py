from logtools import get_logger

from .base_transformer import BaseTransformer
from .mapping_composition_joinconditions_transform import JoinConditionsTransformer

logger = get_logger(__name__)


class SourceCompositionTransformer(BaseTransformer):
    def __init__(self, file_pd_ldm: str, mapping: dict):
        """Transformator voor source composition data.

        Args:
            mapping (dict): De mapping die getransformeerd moet worden.
        """
        super().__init__(file_pd_ldm)
        self.mapping = mapping

    def transform(
        self, dict_objects: dict, dict_attributes: dict, dict_datasources: dict
    ) -> dict:
        """Transformeert en verrijkt de mapping met source composition data.

        Deze functie verwerkt de mapping, haalt compositie-items op, verrijkt deze, filtert specifieke items en voegt de resultaten toe aan de mapping.

        Args:
            dict_objects (dict): Alle objecten (entiteiten, filters, scalars, aggregaten).
            dict_attributes (dict): Alle attributen.
            dict_datasources (dict): Alle datasources.

        Returns:
            dict: De getransformeerde mapping met verrijkte source composition data.
        """
        composition = self._extract_composition(self.mapping)
        composition_items = self._extract_composition_items(composition)
        composition_items = self._transform_composition_items(
            composition_items=composition_items,
            dict_objects=dict_objects,
            dict_attributes=dict_attributes,
        )

        self._mapping_enrich_datasource(dict_datasources=dict_datasources)
        return self.mapping

    def _extract_composition(self, mapping: dict) -> list[dict]:
        """Haalt de lijst van composities op uit de mapping."""
        path_keys = ["c:ExtendedCompositions", "o:ExtendedComposition"]
        composition = self._get_nested(data=mapping, keys=path_keys)
        composition = self.clean_keys(composition)
        composition = [composition] if isinstance(composition, dict) else composition
        composition = self._compositions_remove_mdde_examples(composition)
        return composition

    def _extract_composition_items(self, composition: dict) -> list[dict]:
        lst_composition_items = []
        content = composition.get("c:ExtendedComposition.Content")
        if (
            "c:ExtendedCollections" in content["o:ExtendedSubObject"]
            or "o:ExtendedSubObject" in content
        ):
            lst_composition_items = content["o:ExtendedSubObject"]
        elif "c:ExtendedCollections" in content:
            lst_composition_items = content["c:ExtendedCollections"]
        else:
            logger.warning(f"Mapping zonder inhoud voor {self.file_pd_ldm}")
        if isinstance(lst_composition_items, dict):
            lst_composition_items = [lst_composition_items]
        return lst_composition_items

    def _transform_composition_items(
        self,
        composition_items: list[dict],
        dict_objects: dict,
        dict_attributes: dict,
    ) -> list[dict]:
        """Transformeert en verrijkt individuele compositie-items."""
        for i, composition_item in enumerate(composition_items):
            composition_item = self._enrich_composition_item(
                composition_item,
                dict_objects=dict_objects,
                dict_attributes=dict_attributes,
            )
            composition_item["Order"] = i
            if "c:ExtendedCompositions" in composition_item:
                composition_item.pop("c:ExtendedCompositions")
                logger.info(
                    f"c:ExtendedCompositions is verwijderd van composition_item voor {self.file_pd_ldm}"
                )
            composition_items[i] = composition_item
        composition_items = [
            item
            for item in composition_items
            if item["Entity"]["Stereotype"] != "mdde_ScalarBusinessRule"
        ]
        self.mapping["SourceComposition"] = composition_items
        return composition_items

    def _compositions_remove_mdde_examples(self, lst_compositions: list[dict]) -> dict:
        """Verwijderd de MDDE voorbeeld compositie veronderstelt dat er 1 compositie overblijft

        Args:
            lst_compositions (list[dict]): Composities, inclusief MDDE voorbeeld composities

        Returns:
            dict: Composities zonder de MDDE extensie voorbeelden
        """
        composition = {}
        compositions_new = []
        for item in lst_compositions:
            if "ExtendedBaseCollection.CollectionName" in item:
                if (
                    item["ExtendedBaseCollection.CollectionName"]
                    != "mdde_Mapping_Examples"
                ):
                    compositions_new.append(item)
            else:
                logger.warning(
                    f"Geen 'ExtendedBaseCollection.CollectionName' voor {self.file_pd_ldm}"
                )
        # We assume there is only one composition per mapping, which is why we fill lst
        composition = compositions_new[0] if compositions_new else None
        return composition

    def _enrich_composition_item(
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
        trf_join_conditions = JoinConditionsTransformer(
            file_pd_ldm=self.file_pd_ldm, mapping=self.mapping, composition=composition
        )
        composition = trf_join_conditions.transform(dict_attributes=dict_attributes)
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
                f"Compositie {composition['JoinType']} voor '{composition['Name']}' in {self.file_pd_ldm}"
            )
        else:
            logger.warning(
                f"Geen Join type gevonden in de 'ExtendedAttributesText' voor '{composition['Name']}' in {self.file_pd_ldm}"
            )

    def _composition_entity(self, composition: dict, dict_objects: dict) -> dict:
        """Vormt om en verrijkt de compositie met entiteit data

        Args:
            composition (dict): Compositie data
            dict_objects (dict): Alle entiteiten/filters (in- en external)

        Returns:
            dict: Een geschoonde en verrijkte versie van compositie data
        """
        logger.debug(
            f"Start met transformeren entiteit voor compositie '{composition['Name']} for {self.file_pd_ldm}'"
        )

        path_keys_1 = ["c:ExtendedComposition.Content", "o:ExtendedSubObject"]
        path_keys_2 = ["c:ExtendedCollections", "o:ExtendedCollection"]
        if entity := self._get_nested(data=composition, keys=path_keys_1):
            root_data = "c:ExtendedComposition.Content"
        elif entity := self._get_nested(data=composition, keys=path_keys_2):
            root_data = "c:ExtendedCollections"
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
            logger.debug(
                f"Composition entiteit '{entity['Name']}'voor {self.file_pd_ldm}"
            )
        composition["Entity"] = entity
        composition.pop(root_data)
        return composition

    def _scalar_condition_components(
        self, lst_components: list[dict], dict_attributes: dict
    ) -> dict:
        """Vormt om, schoont en verrijkt component data van 1 scalar conditie

        Args:
            lst_components (list[dict]): scalar conditie component
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
        self, lst_components: list[dict], dict_attributes: dict
    ) -> dict:
        """Haalt het child attribute dictionary op uit de scalar conditie componenten.

        Args:
            lst_components (list[dict]): Lijst van componenten.
            dict_attributes (dict): Dictionary met alle beschikbare attributen.

        Returns:
            dict: Een kopie van het child attribute dictionary, of leeg dict als niet gevonden.
        """
        lst_components = self.clean_keys(content=lst_components)
        return next(
            (
                self._extract_child_attribute(
                    component=component, dict_attributes=dict_attributes
                )
                for component in lst_components
                if component["Name"] == "mdde_ChildAttribute"
            ),
            {},
        )

    def _get_scalar_parent_attribute_and_alias(
        self, lst_components: list[dict], dict_attributes: dict
    ) -> tuple:
        """Haalt het parent attribute dictionary en alias op uit de scalar conditie componenten.

        Args:
            lst_components (list[dict]): Lijst van componenten.
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
        logger.debug(f"Child attribute toegevoegd voor {self.file_pd_ldm}")
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
        logger.debug(
            f"ScalarConditionAttribute alias toegevoegd voor {self.file_pd_ldm}"
        )
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
        logger.debug(f"ScalarConditionAttribute toegevoegd voor {self.file_pd_ldm}")
        type_entity = [
            value
            for value in ["o:Entity", "o:Shortcut", "o:EntityAttribute"]
            if value in component["c:Content"]
        ][0]
        id_attr = component["c:Content"][type_entity]["@Ref"]
        return dict_attributes[id_attr].copy()

    def _mapping_enrich_datasource(self, dict_datasources: dict) -> None:
        """Verrijkt de mapping met de datasource die als bron is aangewezen voor de mapping
        ten behoeve van het genereren van de DDL en ETL

        Args:
            dict_datasources (dict): dictionary met daarin alle beschikbare datasources
        """
        if "c:DataSource" in self.mapping:
            datasource_alias_id = self.mapping["c:DataSource"]["o:DefaultDataSource"][
                "@Ref"
            ]
            datasource_code = dict_datasources[datasource_alias_id]["Code"]
            self.mapping["DataSource"] = datasource_code
            self.mapping.pop("c:DataSource")

