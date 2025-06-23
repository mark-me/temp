from enum import Enum, auto

from logtools import get_logger
import igraph as ig

from .dag_generator import DagBuilder, NoFlowError, VertexType

logger = get_logger(__name__)


class InvalidDeadlockPrevention(Exception):
    pass


class DeadlockPrevention(Enum):
    SOURCE = auto()
    TARGET = auto()


class DagImplementation(DagBuilder):
    def __init__(self):
        super().__init__()

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

    def _dag_etl_coloring(self, dag: ig.Graph) -> ig.Graph:
        """Kleurt de knopen in de ETL-DAG op basis van hun type en model.

        Wijs kleuren toe aan mappings, entiteiten en andere knopen zodat de visualisatie
        van de ETL-DAG duidelijk onderscheid maakt tussen verschillende typen en modellen.
        """
        # Build model coloring dictionary
        colors_model = {
            model: self.colors_discrete[i]
            for i, model in enumerate(list(set(dag.vs["CodeModel"])))
            if model is not None
        }
        # Color vertices
        for vx in self.dag.vs:
            if vx["type"] == VertexType.MAPPING.name:
                vx["color"] = self.node_type_color[vx["type"]]
            elif vx["type"] == VertexType.ENTITY.name:
                vx["color"] = colors_model[vx["CodeModel"]]
            elif "position" in vx.attribute_names():
                vx["color"] = self.color_node_position[vx["position"]]
        return dag

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
                    {"mapping": mapping["name"], "entity": self.dag.predecessors(mapping)}
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
        vs_mappings = [vx for vx in self.dag.vs if vx["type"] == VertexType.MAPPING.name]