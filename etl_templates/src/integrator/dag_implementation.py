from enum import Enum, auto
from typing import List

from logtools import get_logger
import igraph as ig

from .dag_builder import DagBuilder, NoFlowError, VertexType, MappingRef, EntityRef

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
        """Bouwt de DAG op basis van de aangeleverde RETW-bestanden en verrijkt deze met afgeleide gegevens.

        Deze functie initialiseert de DAG-structuur en voegt extra modelinformatie en hashkeys toe.

        Args:
            files_RETW: De inputbestanden voor het opbouwen van de DAG.
        """
        super().build_dag(files_RETW)
        self._add_dag_derived()

    def _add_dag_derived(self):
        """Verrijkt de DAG met extra informatie voor entiteiten en mappings.

        Deze functie voegt entiteittype, modelinformatie en hashkeys toe aan de knopen in de DAG.
        """
        for vx in self.dag.vs:
            # Add data to entities
            if vx["type"] == VertexType.ENTITY.name:
                self._set_entity_type(vx_entity=vx)
            # Add data to mappings
            if vx["type"] == VertexType.MAPPING.name:
                self._mappings_add_model(vx_mapping=vx)
                self._mappings_add_hashkey(vx_mapping=vx)

    def _mappings_add_model(self, vx_mapping: ig.Vertex):
        """Voegt modelinformatie toe aan een mapping op basis van de doelentiteit.

        Deze functie zoekt de doelentiteit van een mapping en vult de mapping aan met de bijbehorende CodeModel en NameModel attributen.

        Args:
            vx_mapping (ig.Vertex): De mapping waarvoor modelinformatie wordt toegevoegd.
        """
        if vs_target_entity := [
            self.dag.vs[idx]
            for idx in self.dag.neighbors(vx_mapping, mode="out")
            if self.dag.vs[idx]["type"] == VertexType.ENTITY.name
        ]:
            vx_mapping["CodeModel"] = vs_target_entity[0]["CodeModel"]
            vx_mapping["NameModel"] = vs_target_entity[0]["NameModel"]

    def _mappings_add_hashkey(self, vx_mapping: ig.Vertex):
        """Voegt een hashkey toe aan een mapping op basis van de attributenmapping ten behoeve van delta bepaling

        Deze functie genereert een hashkey-expressie voor de mapping, gebaseerd op de opgegeven attributen en datasources.

        Args:
            vx_mapping (ig.Vertex): De mapping waarvoor de hashkey wordt toegevoegd.
        """

        def build_hash_attrib(attr_mapping: list, separator: str) -> str:
            """Bouwt een hash-attribuutstring op basis van de attributenmapping en een scheidingsteken.

            Deze functie genereert een stringrepresentatie van een attribuut voor opname in een hashkey-expressie.

            Args:
                attr_mapping (list): De mapping van het attribuut.
                separator (str): Het scheidingsteken voor concatenatie.

            Returns:
                str: De stringrepresentatie van het attribuut voor de hashkey.
            """
            hash_attrib = f"{separator}DA_MDDE.fn_IsNull("
            if "Expression" in attr_mapping:
                return f"{hash_attrib}{attr_mapping['Expression']})"
            entity_alias = attr_mapping["AttributesSource"]["EntityAlias"]
            attr_source = attr_mapping["AttributesSource"]["Code"]
            return f"{hash_attrib}{entity_alias}.[{attr_source}])"

        x_hashkey = "[X_HashKey] = CHECKSUM(CONCAT(N'',"
        for i, attr_mapping in enumerate(vx_mapping["AttributeMapping"]):
            separator = "" if i == 0 else ","
            hash_attrib = build_hash_attrib(
                attr_mapping=attr_mapping, separator=separator
            )
            x_hashkey = x_hashkey + hash_attrib
        vx_mapping["X_Hashkey"] = f"{x_hashkey},'{vx_mapping['DataSource']}'))"

    def _set_entity_type(self, vx_entity: ig.Vertex) -> None:
        """
        Bepaalt en stelt het entiteittype in op basis van het 'Stereotype' attribuut.

        Deze functie wijst het type 'Regular' toe als er geen stereotype is, anders 'Aggregate'.

        Args:
            vx_entity (ig.Vertex): De entiteit waarvoor het type wordt bepaald.
        """
        if (
            "Stereotype" not in vx_entity.attributes()
            or vx_entity["Stereotype"] is None
        ):
            vx_entity["type_entity"] = "Regular"
        else:
            vx_entity["type_entity"] = "Aggregate"

    def get_run_config(self, deadlock_prevention: DeadlockPrevention) -> List[dict]:
        """Bepaalt de uitvoeringsvolgorde en run-levels van mappings in de ETL-DAG.

        Deze functie berekent de run-levels en stages voor mappings op basis van de gekozen deadlock-preventiestrategie,
        en retourneert een gesorteerde lijst van mappings met hun uitvoeringsvolgorde en relevante metadata.

        Args:
            deadlock_prevention (DeadlockPrevention): De gekozen strategie voor deadlock-preventie.

        Returns:
            list: Een gesorteerde lijst van dictionaries met run-level, stage en mappinginformatie.

        Raises:
            InvalidDeadlockPrevention: Indien een ongeldige deadlock-preventiestrategie is opgegeven.
            NoFlowError: Indien er geen mappings zijn en dus geen uitvoeringsvolgorde kan worden bepaald.
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
        vs_mappings = self.dag.vs.select(type_eq=VertexType.MAPPING.name)
        for vx in vs_mappings:
            dict_mapping = {
                "RunLevel": vx["run_level"],
                "RunLevelStage": vx["run_level_stage"],
                "NameModel": vx["NameModel"],
                "CodeModel": vx["CodeModel"],
                "MappingName": vx["Name"],
                "SourceViewName": f"vw_src_{vx['Name']}",
                "TargetName": vx["EntityTarget"]["Code"],
            }
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
        run_levels = sorted({node["run_level"] for node in vs_mapping})
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

    def get_load_dependencies(self, vertex_type: VertexType = VertexType.MAPPING) -> List[dict]:
        """Geeft van iedere knoop in het ETL-proces alle voorliggende (predecessors) of opvolgende (successors) vergelijkbare typen knopen terug

        Args:
            vertex_type (VertexType, optional): Geeft aan welk type (mapping/entiteiten) afhankelijkheden moet worden teruggegeven.
            Defaults naar VertexType.MAPPING.

        Raises:
            NoFlowError: Indien er geen ETL flow met mappings is

        Returns:
            list: Lijst met dictionaries met voor iedere ETL-knoop de voorliggende en opvolgende knopen.
        """
        lst_dependencies = []
        if not self.dag:
            raise NoFlowError("There are no mappings, so there is no ETL flow!")

        vs_of_type = self.dag.vs.select(type_eq=vertex_type.name)
        for vx in vs_of_type:
            lst_dependencies.extend(self._get_predecessors_for_vertex(vx, vertex_type))
            lst_dependencies.extend(self._get_successors_for_vertex(vx, vertex_type))
        return lst_dependencies

    def _get_predecessors_for_vertex(self, vx: ig.Vertex, vertex_type: VertexType) -> List[dict]:
        """Geeft de predecessors van een specifieke knoop van het opgegeven type terug.

        Args:
            vx (ig.Vertex): De knoop waarvoor predecessors worden gezocht.
            vertex_type (VertexType): Het type knoop.

        Returns:
            list: Lijst met dictionaries van predecessors.
        """
        vs = [
            self.dag.vs[vx_in]
            for vx_in in self.dag.subcomponent(vx, mode="in")
            if vx_in != vx.index
        ]
        return [
            {
                "model": vx["CodeModel"],
                "name": vx["Name"],
                "type_relation": "predecessor",
                "model_related": vx_in["CodeModel"],
                "name_related": vx_in["Name"],
            }
            for vx_in in vs
            if vx_in["type"] == vertex_type.name
        ]

    def _get_successors_for_vertex(self, vx: ig.Vertex, vertex_type: VertexType) -> List[dict]:
        """Geeft de successors van een specifieke knoop van het opgegeven type terug.

        Args:
            vx (ig.Vertex): De knoop waarvoor successors worden gezocht.
            vertex_type (VertexType): Het type knoop.

        Returns:
            list: Lijst met dictionaries van successors.
        """
        vs_out = [
            self.dag.vs[vx_out]
            for vx_out in self.dag.subcomponent(vx, mode="out")
            if vx_out != vx.index
        ]
        return [
            {
                "model": vx["CodeModel"],
                "name": vx["Name"],
                "type_relation": "successor",
                "model_related": vx_out["CodeModel"],
                "name_related": vx_out["Name"],
            }
            for vx_out in vs_out
            if vx_out["type"] == vertex_type.name
        ]

    def get_mappings(self) -> List[dict]:
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

    def get_entities(self) -> List[dict]:
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
