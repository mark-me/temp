from enum import Enum, auto

from logtools import get_logger
import igraph as ig

from .dag_builder import DagBuilder, NoFlowError, VertexType

logger = get_logger(__name__)


class InvalidDeadlockPrevention(Exception):
    """Exception raised when an invalid deadlock prevention method is selected.

    This exception is used to indicate that the provided deadlock prevention strategy is not supported or recognized.
    """

    pass


class DeadlockPrevention(Enum):
    """Enumeration for deadlock prevention strategies in ETL DAG execution.

    This enum defines the available methods for preventing deadlocks, such as by source or target.
    """

    SOURCE = auto()
    TARGET = auto()


class DagImplementation(DagBuilder):
    def __init__(self):
        super().__init__()

    def build_dag(self, files_RETW):
        super().build_dag(files_RETW)
        self._add_dag_derived()

    def _add_dag_derived(self):
        self._add_model_to_mapping()

        # Create BKeys on first derived level
        vs_entity_source = self.dag.vs.select(etl_level_eq=1)
        lst_attrs_before = [
            vx.attributes()
            for vx in self.dag.vs.select(etl_level_eq=1)
            if vx["type"] == VertexType.ENTITY.name and vx["CodeModel"] == "DA_Central"
        ]
        for vx_entity in vs_entity_source:
            metadata_bkeys = self._create_source_bkeys(vx_entity=vx_entity)
            self._replace_entity_keys_with_bkeys(
                vx_entity=vx_entity, metadata_bkeys=metadata_bkeys
            )
        lst_attrs_after = [
            vx.attributes()
            for vx in self.dag.vs.select(etl_level_eq=1)
            if vx["type"] == VertexType.ENTITY.name and vx["CodeModel"] == "DA_Central"
        ]
        pass

    def _add_model_to_mapping(self):
        vs_mappings = self.dag.vs.select(type_eq=VertexType.MAPPING.name)
        for vx in vs_mappings:
            if vs_target_entity := [
                self.dag.vs[idx]
                for idx in self.dag.neighbors(vx, mode="out")
                if self.dag.vs[idx]["type"] == VertexType.ENTITY.name
            ]:
                vx["CodeModel"] = vs_target_entity[0]["CodeModel"]
                vx["NameModel"] = vs_target_entity[0]["NameModel"]

    def _create_source_bkeys(self, vx_entity: ig.Vertex):
        """
            Verzamelt identifier-informatie uit de entiteitconfiguratie.

            Doorloopt per entiteit alle identifiers, en genereert een dictionary met metadatastring per identifier voor gebruik in DDL-generatie.

        Args:
            entity (dict): Entiteit

        Returns:
            dict: Een dictionary met een metadata string van de businesskey per identifier
        """
        metadata_bkeys = {}

        def get_name_business_key(identifier):
            return (
                identifier["EntityCode"]
                if identifier["IsPrimary"]
                else identifier["Code"]
            )

        def get_identifier_def_primary(name_business_key):
            return f"[{name_business_key}BKey] nvarchar(200) NOT NULL"

        for identifier in vx_entity["Identifiers"]:
            name_business_key = get_name_business_key(identifier)
            metadata_bkey = get_identifier_def_primary(name_business_key)

            metadata_bkeys[identifier["Id"]] = {
                "IdentifierID": identifier["Id"],
                "IdentifierName": identifier["Name"],
                "IdentifierCode": identifier["Code"],
                "EntityId": identifier["EntityID"],
                "EntityCode": identifier["EntityCode"],
                "IsPrimary": identifier["IsPrimary"],
                "MetadataBkey": metadata_bkey,
            }
        return metadata_bkeys

    def _replace_entity_keys_with_bkeys(
        self, vx_entity: ig.Vertex, metadata_bkeys: dict
    ):
        """Vervangt alle key kolommen met business key kolommen.

        Args:
            metadata_bkeys (dict): Alle bkey metadata definities
            entity (dict): Entiteit
        """
        mapped_identifiers = []
        identifier_mapped = []

        if vx_entity["Stereotype"] is None:
            """
                We doen niks met eventuele identifiers van Aggregators. Dit moet geen error opleveren.
                Alleen identifiers van echte entiteiten worden gebruikt en moet aanwezig zijn.
                Deze entiteiten hebben hier geen Stereotype
                """
            logger.info(
                f"Identifier voor entiteit '{vx_entity['Code']}' niet nodig vanwege stereotype Aggregaat"
            )
            return
        elif vx_entity["Stereotype"] is not None:
            for identifier in vx_entity["Identifiers"]:
                if "Id" not in identifier:
                    logger.error(
                        f"Identifier voor entiteit '{vx_entity['Code']}' niet gevonden in identifiers"
                    )
                    continue
                identifier_id = identifier["Id"]
                if identifier_id in metadata_bkeys:
                    metadata_bkey = metadata_bkeys[identifier_id]["MetadataBkey"]
                    identifier_name = metadata_bkeys[identifier_id]["IdentifierName"]
                    identifier_mapped.append(metadata_bkey)
                    mapped_identifiers.append(identifier_name)

            vx_entity["Identifiers"] = identifier_mapped

            def is_not_mapped_identifier(attribute):
                return attribute["Code"] not in mapped_identifiers

            attributes = [
                attribute
                for attribute in vx_entity["Attributes"]
                if is_not_mapped_identifier(attribute)
            ]
            #vx_entity.pop("Attributes")
            vx_entity["Attributes"] = None
            vx_entity["Attributes"] = attributes

    def get_run_config(self, deadlock_prevention: DeadlockPrevention) -> list:
        """Geeft een gesorteerde lijst van mappings terug op basis van run level en deadlock-preventie.

        Bepaalt de uitvoeringsvolgorde van mappings in de ETL-DAG, verrijkt met run level en stage,
        en sorteert deze zodat de juiste volgorde voor uitvoering of visualisatie beschikbaar is.

        Args:
            deadlock_prevention (DeadlockPrevention): Methode voor deadlock-preventie (SOURCE of TARGET).

        Returns:
            list: Een gesorteerde lijst van mappings met relevante attributen voor uitvoering.
        """
        lst_mappings = []
        try:
            if deadlock_prevention not in [
                DeadlockPrevention.SOURCE,
                DeadlockPrevention.TARGET,
            ]:
                raise InvalidDeadlockPrevention("No valid Deadlock prevention selected")
            self._dag_run_level_stages(deadlock_prevention=deadlock_prevention)
        except NoFlowError:
            logger.error(
                "There are no mappings, so there is no mapping order to generate!"
            )
            return []
        for vx in self.dag.vs:
            if vx["type"] == VertexType.MAPPING.name:
                successors = self.dag.vs[self.dag.successors(vx)[0]]
                dict_successors = {
                    key: successors[key] for key in successors.attribute_names()
                }
                dict_mapping = {key: vx[key] for key in vx.attribute_names()}
                dict_mapping["RunLevel"] = vx["run_level"]
                dict_mapping["RunLevelStage"] = vx["run_level_stage"]
                dict_mapping["NameModel"] = dict_successors["NameModel"]
                dict_mapping["CodeModel"] = dict_successors["CodeModel"]
                dict_mapping["SourceViewName"] = (
                    f"vw_src_{vx['Name'].replace(' ', '_')}"
                )
                dict_mapping["TargetName"] = dict_successors["Code"]
                lst_mappings.append(dict_mapping)
        # Sort the list of mappings by run level and the run level stage
        lst_mappings = sorted(
            lst_mappings,
            key=lambda mapping: (mapping["RunLevel"], mapping["RunLevelStage"]),
        )
        return lst_mappings

    def _dag_ETL_run_order(
        self, dag: ig.Graph, deadlock_prevention: DeadlockPrevention
    ) -> ig.Graph:
        """Verrijk de ETL DAG met de volgorde waarmee de mappings uitgevoerd moeten worden.

        Args:
            dag (ig.Graph): ETL DAG met entiteiten en mappings

        Returns:
            ig.Graph: ETL DAG waar de knopen verrijkt worden met het attribuut 'run_level',
            entiteit knopen krijgen de waarde -1, omdat de executievolgorde niet van toepassing is op entiteiten.
        """

        if deadlock_prevention not in [
            DeadlockPrevention.SOURCE,
            DeadlockPrevention.TARGET,
        ]:
            raise InvalidDeadlockPrevention("No valid Deadlock prevention selected")
        dag = self._dag_run_level_stages(
            dag=dag, deadlock_prevention=deadlock_prevention
        )
        return dag

    def _dag_run_level_stages(
        self, deadlock_prevention: DeadlockPrevention
    ) -> ig.Graph:
        """Bepaalt en wijst de uitvoeringsstages toe aan mappings op basis van run levels en deadlock-preventie.

        Voor elke run level worden mappings gegroepeerd en conflicten bepaald, waarna een unieke stage wordt toegekend
        aan elke mapping om gelijktijdige uitvoering zonder conflicten mogelijk te maken.

        Args:
            dag (ig.Graph): De igraph DAG met mappings en entiteiten.
            deadlock_prevention (DeadlockPrevention): Methode voor deadlock-preventie (SOURCE of TARGET).

        """
        dict_level_stages = {}
        # All mapping nodes
        vs_mapping = self.dag.vs.select(type_eq=VertexType.MAPPING.name)

        # Determine run stages of mappings by run level
        run_levels = list({node["run_level"] for node in vs_mapping})
        for run_level in run_levels:
            # Find run_level mappings and corresponding source entities
            if deadlock_prevention == DeadlockPrevention.SOURCE:
                mappings = [
                    {
                        "mapping": mapping["name"],
                        "entity": self.dag.predecessors(mapping),
                    }
                    for mapping in vs_mapping.select(run_level_eq=run_level)
                ]
            elif deadlock_prevention == DeadlockPrevention.TARGET:
                mappings = [
                    {"mapping": mapping["name"], "entity": self.dag.successors(mapping)}
                    for mapping in vs_mapping.select(run_level_eq=run_level)
                ]
            # Create graph of mapping conflicts (mappings that draw on the same sources)
            graph_conflicts = self._dag_ETL_run_levels_conflicts_graph(mappings)
            # Determine unique sorting for conflicts
            order = graph_conflicts.vertex_coloring_greedy(method="colored_neighbors")
            # Apply them back to the DAG
            dict_level_stages |= dict(zip(graph_conflicts.vs["name"], order))
            for k, v in dict_level_stages.items():
                self.dag.vs.select(name=k)["run_level_stage"] = v

    def _dag_ETL_run_levels_conflicts_graph(self, mapping_sources: dict) -> ig.Graph:
        """Genereert een conflictgrafiek voor mappings op basis van gedeelde entiteiten.

        Maakt een niet-gerichte grafiek waarin mappings verbonden zijn als ze dezelfde entiteiten delen,
        wat gebruikt kan worden om conflicten of afhankelijkheden tussen mappings te visualiseren.

        Args:
            mapping_sources (dict): Een lijst van dictionaries met mapping-namen en hun bijbehorende entiteiten.

        Returns:
            ig.Graph: Een igraph-object dat de conflicten tussen mappings weergeeft.
        """
        lst_vertices = [{"name": mapping["mapping"]} for mapping in mapping_sources]
        lst_edges = []
        for a in mapping_sources:
            for b in mapping_sources:
                if a["mapping"] < b["mapping"]:
                    qty_common = len(set(a["entity"]) & set(b["entity"]))
                    if qty_common > 0:
                        lst_edges.append(
                            {"source": a["mapping"], "target": b["mapping"]}
                        )
        graph_conflicts = ig.Graph.DictList(
            vertices=lst_vertices, edges=lst_edges, directed=False
        )
        return graph_conflicts

    def get_mappings(self) -> list:
        """Geeft een lijst terug van alle mapping-knopen in de huidige DAG.

        Deze functie selecteert en retourneert alle knopen van het type MAPPING,
        zodat deze eenvoudig kunnen worden geraadpleegd of verwerkt.

        Returns:
            list: Een lijst van mapping-knopen in de DAG.
        """
        vs_mappings = [
            vx.attributes()
            for vx in self.dag.vs
            if vx["type"] == VertexType.MAPPING.name
        ]
        return vs_mappings

    def get_entities(self) -> list:
        """Geeft een lijst terug van alle entiteit-knopen in de huidige DAG.

        Deze functie selecteert en retourneert alle knopen van het type ENTITY,
        zodat deze eenvoudig kunnen worden geraadpleegd of verwerkt.

        Returns:
            list: Een lijst van entiteit-knopen in de DAG.
        """
        vs_entities = [
            vx.attributes()
            for vx in self.dag.vs
            if vx["type"] == VertexType.ENTITY.name
        ]
        return vs_entities
