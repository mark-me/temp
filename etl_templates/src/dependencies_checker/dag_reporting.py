import os
from collections import deque
from enum import Enum, auto
from pathlib import Path

import igraph as ig
import networkx as nx
from logtools import get_logger
from pyvis.network import Network

from .dag_generator import DagGenerator, EntityRef, NoFlowError, VertexType

logger = get_logger(__name__)


class InvalidDeadlockPrevention(Exception):
    pass


class ObjectPosition(Enum):
    START = auto()
    INTERMEDIATE = auto()
    END = auto()
    UNDETERMINED = auto()


class DeadlockPrevention(Enum):
    SOURCE = auto()
    TARGET = auto()


class DagReporting(DagGenerator):
    """Extends the DagGenerator class to provide reporting and visualization functionalities.

    This class inherits from DagGenerator and adds functionalities for visualizing DAGs using pyvis,
    setting node attributes for visualization, converting between igraph and networkx graph formats,
    and determining node hierarchy levels for visualization.
    """

    def __init__(self):
        """Initializes a new instance of the DagReporting class.

        Initializes color palettes, node shapes, and node colors for visualization.
        It also calls the constructor of the parent class (DagGenerator).
        """
        super().__init__()
        self.colors_discrete = [
            "#ff595e",
            "#ff924c",
            "#ffca3a",
            "#c5ca30",
            "#8ac926",
            "#52a675",
            "#1982c4",
            "#4267ac",
            "#6a4c93",
            "#b5a6c9",
        ]
        self.color_node_position = {
            ObjectPosition.START.name: "gold",
            ObjectPosition.INTERMEDIATE.name: "yellowgreen",
            ObjectPosition.END.name: "lawngreen",
            ObjectPosition.UNDETERMINED.name: "red",
        }

        self.node_type_shape = {
            VertexType.ENTITY.name: "database",
            VertexType.FILE_RETW.name: "square",
            VertexType.MAPPING.name: "hexagon",
            VertexType.ERROR.name: "star",
        }
        self.node_type_color = {
            VertexType.ENTITY.name: "#fbed8f",
            VertexType.FILE_RETW.name: "#73c4e5",
            VertexType.MAPPING.name: "#8962ad",
            VertexType.ERROR.name: "red",
        }

    def _create_output_dir(self, file_path: str) -> None:
        parent_directory = os.path.dirname(file_path)
        Path(parent_directory).mkdir(parents=True, exist_ok=True)

    def _dag_ETL_run_order(
        self, dag: ig.Graph, deadlock_prevention: DeadlockPrevention
    ) -> ig.Graph:
        """Verrijk de ETL DAG met de volgorder waarmee de mappings uitgevoerd moeten worden.

        Args:
            dag (ig.Graph): ETL DAG met entiteiten en mappings

        Returns:
            ig.Graph: ETL DAG waar de knopen verrijkt worden met het attribuut 'run_level',
            entiteit knopen krijgen de waarde -1, omdat de executievolgorde niet van toepassing is op entiteiten.
        """
        # For each node calculate the number of mapping nodes before the current node
        dag_mappings = self.get_dag_mappings()
        lst_mapping_order = [
            len(dag.subcomponent(dag.vs[i], mode="in")) - 1
            for i in range(dag.vcount())
        ]
        # Assign valid run order to mappings only
        lst_run_level = []
        lst_run_level.extend(
            run_level if role == VertexType.MAPPING.name else -1
            for run_level, role in zip(lst_mapping_order, dag.vs["type"])
        )
        dag.vs["run_level1"] = lst_run_level

        # Start traversing ETL
        # Find first mappings
        vs_mappings_start = [i for i, x in enumerate(lst_run_level) if x == 0]

        # FIXME: Stop making lists, and start iterating through dag starting by 0 vertices and iterating through out neighbors

        lst_run_level2 = []
        for i in range(dag.vcount()):
            if dag.vs[i]["type"] == VertexType.MAPPING.name:
                lst_mapping_level = []
                entities_input = dag.neighbors(dag.vs[i], mode="in")
                for entity_input in entities_input:
                    for vx in dag.neighbors(dag.vs[entity_input], mode="in"):
                        lst_mapping_level.append(dag.vs[vx]["run_level1"])
                    if lst_mapping_level:
                        max_level = max(lst_mapping_level)
                    else:
                        max_level = 0
                    lst_run_level2.append(max_level + 1)


        lst_run_level = self._make_increasing_with_duplicates(lst=lst_run_level)
        dag.vs["run_level"] = lst_run_level
        df_vertices = dag.get_vertex_dataframe()
        df_vertices = (
            df_vertices.loc[df_vertices["type"] == "MAPPING"]
            .loc[df_vertices["Name"].str.contains("SL_KIS_")]
            .filter(["Name", "run_level1", "run_level"])
        )

        if deadlock_prevention not in [
            DeadlockPrevention.SOURCE,
            DeadlockPrevention.TARGET,
        ]:
            raise InvalidDeadlockPrevention("No valid Deadlock prevention selected")
        dag = self._dag_run_level_stages(
            dag=dag, deadlock_prevention=deadlock_prevention
        )
        return dag

    def _make_increasing_with_duplicates(self, lst: list) -> list:
        result = []
        current = lst[0] if lst else 0

        for i in range(len(lst)):
            if i == 0:
                result.append(current)
            else:
                if lst[i] == lst[i - 1]:
                    # duplicaat → zelfde waarde
                    result.append(result[-1])
                else:
                    # verhoog vorige met 1
                    result.append(result[-1] + 1)
        return result

    def _dag_run_level_stages(
        self, dag: ig.Graph, deadlock_prevention: DeadlockPrevention
    ) -> ig.Graph:
        """Determine mapping stages for each run level

        Args:
            dag (ig.Graph): DAG describing the ETL

        Returns:
            ig.Graph: ETL stages for a level added in the mapping vertex attribute 'stage'
        """
        dict_level_stages = {}
        # All mapping nodes
        vs_mapping = dag.vs.select(type_eq=VertexType.MAPPING.name)

        # Determine run stages of mappings by run level
        run_levels = list({node["run_level"] for node in vs_mapping})
        for run_level in run_levels:
            # Find run_level mappings and corresponding source entities
            if deadlock_prevention == DeadlockPrevention.SOURCE:
                mappings = [
                    {"mapping": mapping["name"], "entity": dag.predecessors(mapping)}
                    for mapping in vs_mapping.select(run_level_eq=run_level)
                ]
            elif deadlock_prevention == DeadlockPrevention.TARGET:
                mappings = [
                    {"mapping": mapping["name"], "entity": dag.successors(mapping)}
                    for mapping in vs_mapping.select(run_level_eq=run_level)
                ]
            # Create graph of mapping conflicts (mappings that draw on the same sources)
            graph_conflicts = self._dag_ETL_run_levels_conflicts_graph(mappings)
            # Determine unique sorting for conflicts
            order = graph_conflicts.vertex_coloring_greedy(method="colored_neighbors")
            # Apply them back to the DAG
            dict_level_stages |= dict(zip(graph_conflicts.vs["name"], order))
            for k, v in dict_level_stages.items():
                dag.vs.select(name=k)["run_level_stage"] = v
        return dag

    def _dag_ETL_run_levels_conflicts_graph(self, mapping_sources: dict) -> ig.Graph:
        """Generate a graph expressing which mappings share sources

        Args:
            mapping_sources (dict): Mappings with a list of source node ids for each of them

        Returns:
            ig.Graph: Expressing mapping sharing source entities
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

    def _igraph_to_networkx(self, graph: ig.Graph) -> nx.DiGraph:
        """Converts an igraph into a networkx graph

        Args:
            dag (ig.Graph): igraph graph

        Returns:
            nx.DiGraph: networkx graph
        """
        dag_nx = nx.DiGraph()
        # Convert nodes
        lst_nodes_igraph = graph.get_vertex_dataframe().to_dict("records")
        lst_nodes = []
        lst_nodes.extend((node["name"], node) for node in lst_nodes_igraph)
        dag_nx.add_nodes_from(lst_nodes)

        # Convert edges
        lst_edges_igraph = graph.get_edge_dataframe().to_dict("records")
        lst_edges = []
        lst_edges.extend((edge["source"], edge["target"]) for edge in lst_edges_igraph)
        dag_nx.add_edges_from(lst_edges)
        return dag_nx

    def _set_node_tooltip(self, node: ig.Vertex) -> None:
        """Set the tooltip for a node in the pyvis visualization.

        Constructs the HTML tooltip content for a given node based on its type and attributes.
        The tooltip includes information such as
        * File: file path, order in adding, creation/modification dates
        * Entity/mapping details, such as ETL flow ordering information.

        Args:
            node (ig.Vertex): The node to set the tooltip for.
        """
        if node["type"] == VertexType.FILE_RETW.name:
            node["title"] = f"""FileRETW: {node["FileRETW"]}
                    Order: {node["Order"]}
                    """
        if node["type"] in [VertexType.MAPPING.name, VertexType.ENTITY.name]:
            node["title"] = f"""Name: {node["Name"]}
                        Code: {node["Code"]}
                    """
        if node["type"] == VertexType.ENTITY.name:
            node["title"] = node["title"] + f"Model: {node['CodeModel']}\n\n"
        if (
            node["type"] == VertexType.MAPPING.name
            and "run_level" in node.attribute_names()
        ):
            node["title"] = (
                node["title"]
                + f"""
                    Run level: {str(node["run_level"])}
                    Run level stage: {str(node["run_level_stage"])}\n
                """
            )

        lst_attr_labels = [
            ("Created", "CreationDate"),
            ("Creator", "Creator"),
            ("Modified", "ModificationDate"),
            ("Modifier", "Modifier"),
        ]
        for label, attr in lst_attr_labels:
            if attr in node.attribute_names():
                node["title"] = node["title"] + f"{label}: {node[attr]}\n"

    def _set_visual_attributes(self, dag: ig.Graph) -> ig.Graph:
        """Set attributes for pyvis visualization.

        Sets the shape, shadow, color, and tooltip for each node in the graph
        based on their type and other properties. Also sets the shadow for edges.

        Args:
            graph (ig.Graph): The igraph graph to set attributes for.

        Returns:
            ig.Graph: The graph with attributes set for pyvis visualization.
        """
        logger.info("Setting graphical attributes of the graph")
        for node in dag.vs:
            node["shape"] = self.node_type_shape[node["type"]]
            node["shadow"] = True
            node["color"] = self.node_type_color[node["type"]]
            self._set_node_tooltip(node)
        return dag

    def plot_graph_html(self, dag: ig.Graph, file_html: str) -> None:
        """Create a html file with a graphical representation of a networkx graph

        Args:
            dag (nx.DiGraph): Networkx DAG
            file_html (str): file path that the result should be written to
        """
        self._create_output_dir(file_path=file_html)
        net = Network("900px", "1917px", directed=True, layout=True)
        dag = self._igraph_to_networkx(graph=dag)
        net.from_nx(dag)
        net.options.layout.hierarchical.sortMethod = "directed"
        net.options.physics.solver = "hierarchicalRepulsion"
        net.options.edges.smooth = False
        net.options.interaction.navigationButtons = True
        net.toggle_physics(True)
        for edge in net.edges:
            edge["shadow"] = True
        net.show(file_html, notebook=False)

    def _dag_node_hierarchy_level(self, dag: ig.Graph) -> ig.Graph:
        """Enrich the DAG with the level in the hierarchy where vertices should be plotted.

        Determines and sets the 'level' attribute for each vertex in the DAG, used for visualization.

        Args:
            dag (ig.Graph): DAG that describes entities and mappings.

        Returns:
            ig.Graph: DAG where the vertices are enriched with the attribute 'level'.
        """
        dag = self._calculate_node_levels(dag)
        dag = self._set_max_end_node_level(dag)
        return dag

    def _calculate_node_levels(self, dag: ig.Graph) -> ig.Graph:
        """Calculate and assign a level to each node in the DAG.

        Calculates the level of each node in the DAG based on its predecessors,
        and adds a 'level' attribute to each vertex.

        Args:
            dag (ig.Graph): The DAG to process.

        Returns:
            ig.Graph: The DAG with node levels calculated and set.
        """
        # Getting the number of preceding nodes to determine where to start
        for i in range(dag.vcount()):
            dag.vs[i]["qty_predecessors"] = len(dag.subcomponent(dag.vs[i], mode="in"))

        # Calculating levels
        # FIXME: Iterates through nodes that have multiple incoming connections multiple times
        id_vertices = deque(
            [(vtx, 0) for vtx in dag.vs.select(qty_predecessors_eq=1).indices]
        )
        while id_vertices:
            id_vx, level = id_vertices.popleft()
            dag.vs[id_vx]["level"] = level
            id_vertices.extend(
                [(vtx, level + 1) for vtx in dag.neighbors(id_vx, mode="out")]
            )
        return dag

    def _dag_node_position_category(self, dag: ig.Graph) -> ig.Graph:
        """Determine and set the position category (start, intermediate, end) of each node in the DAG.

        Determines if entities are start, intermediate, or end nodes based on their in-degree and out-degree,
        and adds a 'position' attribute to the DAG vertices.

        Args:
            dag (ig.Graph): The DAG to process.

        Returns:
            ig.Graph: The DAG with node positions set.
        """
        dag.vs["qty_out"] = dag.degree(dag.vs, mode="out")
        dag.vs["qty_in"] = dag.degree(dag.vs, mode="in")
        lst_entity_position = []
        for qty_in, qty_out in zip(dag.vs["qty_in"], dag.vs["qty_out"]):
            if qty_in == 0 and qty_out > 0:
                position = ObjectPosition.START.name
            elif qty_in > 0 and qty_out > 0:
                position = ObjectPosition.INTERMEDIATE.name
            elif qty_in > 0 and qty_out == 0:
                position = ObjectPosition.END.name
            else:
                position = ObjectPosition.UNDETERMINED.name
            lst_entity_position.append(position)
        dag.vs["position"] = lst_entity_position
        return dag

    def _set_max_end_node_level(self, dag: ig.Graph) -> ig.Graph:
        """Set the level of all end nodes to the maximum level.

        Args:
            dag (ig.Graph): The DAG to process.

        Returns:
            ig.Graph: The DAG with end node levels adjusted.
        """
        dag = self._dag_node_position_category(dag=dag)
        end_levels = [
            dag.vs[vtx]["level"]
            for vtx in range(
                dag.vcount()
            )  # Iterate over all vertices to find the true max level.
            if dag.vs[vtx]["position"] == ObjectPosition.END.name
        ]
        level_max = max(end_levels, default=0)
        for i in range(dag.vcount()):
            if dag.vs[i]["position"] == ObjectPosition.END.name:
                dag.vs[i]["level"] = level_max
        return dag

    def plot_graph_total(self, file_html: str) -> None:
        """Plot the total graph and save it to an HTML file.

        Builds the total graph, sets pyvis attributes, and visualizes it in an HTML file.

        Args:
            file_html (str): The path to the HTML file where the plot will be saved.

        Returns:
            None
        """
        logger.info(
            f"Create a network plot, '{file_html}', for files, entities and mappings"
        )
        dag = self.get_dag_total()
        dag = self._set_visual_attributes(dag=dag)
        self.plot_graph_html(dag=dag, file_html=file_html)

    def plot_graph_retw_file(self, file_retw: str, file_html: str) -> None:
        """Plot the graph for a specific RETW file.

        Builds the total graph, selects the subgraph related to a specific RETW file,
        sets pyvis attributes, and visualizes it in an HTML file.

        Args:
            file_retw (str): Path to the RETW file.
            file_html (str): Path to the output HTML file.

        Returns:
            None
        """
        logger.info(
            f"Creating a network plot, '{file_html}', for entities and mappings of a single RETW file"
        )
        dag = self.get_dag_single_retw_file(file_retw=file_retw)
        dag = self._set_visual_attributes(dag=dag)
        self.plot_graph_html(dag=dag, file_html=file_html)

    def plot_file_dependencies(
        self, file_html: str, include_entities: bool = True
    ) -> None:
        """Plot the dependencies between RETW files.

        Generates and visualizes a graph showing dependencies between RETW files,
        optionally including entities in the visualization.

        Args:
            file_html (str): Path to the output HTML file.
            include_entities (bool, optional): Whether to include entities in the plot. Defaults to True.

        Returns:
            None
        """
        logger.info(
            f"Creating a network plot, '{file_html}', showing RETW file dependencies"
        )
        dag_files = self.get_dag_file_dependencies(include_entities=include_entities)
        dag_files = self._set_visual_attributes(dag=dag_files)
        self.plot_graph_html(dag=dag_files, file_html=file_html)

    def plot_entity_journey(self, entity: EntityRef, file_html: str) -> None:
        """Plot the journey of an entity through the DAG.

        Generates and visualizes a graph showing the complete journey of a specific entity,
        including all its dependencies and related mappings.

        Args:
            entity (EntityRef): The entity to plot the journey for.
            file_html (str): Path to the output HTML file.

        Returns:
            None
        """
        logger.info(
            f"Creating a network plot, '{file_html}', for all dependencies of entity '{entity[0]}.{entity[1]}'."
        )
        dag = self.get_dag_of_entity(entity=entity)  # Visualization
        dag = self._set_visual_attributes(dag=dag)
        # Recolor requested entity
        id_entity = self.get_entity_id(entity_ref=entity)
        vx_entity = dag.vs.select(name=id_entity)[0]
        dag.vs[vx_entity.index]["color"] = "#f296bf"
        self.plot_graph_html(dag=dag, file_html=file_html)

    def get_entities_without_definition(self) -> list:
        """Identifies entities without a definition in the DAG.

        This function checks for entities that do not have any incoming connections from a RETW file,
        indicating they lack a definition within the current scope.

        Returns:
            list: A list of dictionaries, where each dictionary represents an entity without a definition
                  and contains its attributes.
        """
        lst_entities = []
        dag = self.get_dag_total()
        vs_entities = dag.vs.select(type_eq=VertexType.ENTITY.name)
        for vx_entity in vs_entities:
            vs_in = dag.vs(dag.neighbors(vx_entity, mode="in"))
            if not [vx for vx in vs_in if vx["type"] == VertexType.FILE_RETW.name]:
                lst_entities.append(vx_entity.attributes())
        return lst_entities

    def get_mapping_order(self, deadlock_prevention: DeadlockPrevention) -> list:
        """Returns mappings and order of running (could be parallel,
        in which case other sub-sorting should be implemented if needed)

        Returns:
            list: List of mappings with order
        """
        lst_mappings = []
        try:
            dag = self.get_dag_ETL()
            dag = self._dag_ETL_run_order(
                dag=dag, deadlock_prevention=deadlock_prevention
            )
        except NoFlowError:
            logger.error(
                "There are no mappings, so there is no mapping order to generate!"
            )
            return []
        for node in dag.vs:
            if node["type"] == VertexType.MAPPING.name:
                successors = dag.vs[dag.successors(node)[0]]
                dict_successors = {
                    key: successors[key] for key in successors.attribute_names()
                }
                dict_mapping = {key: node[key] for key in node.attribute_names()}
                dict_mapping["RunLevel"] = node["run_level"]
                dict_mapping["RunLevelStage"] = node["run_level_stage"]
                dict_mapping["NameModel"] = dict_successors["NameModel"]
                dict_mapping["CodeModel"] = dict_successors["CodeModel"]
                dict_mapping["SourceViewName"] = (
                    f"vw_src_{node['Name'].replace(' ', '_')}"
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
        """Helper function to color nodes in the ETL DAG based on their type and model.

        Assigns colors to the nodes in the ETL DAG for visualization purposes.
        Mappings are colored based on their type, entities are colored based on their model,
        and other nodes are colored based on their position (start, intermediate, end).

        Args:
            dag (ig.Graph): The ETL DAG to color.

        Returns:
            ig.Graph: The colored ETL DAG.
        """
        # Build model coloring dictionary
        colors_model = {
            model: self.colors_discrete[i]
            for i, model in enumerate(list(set(dag.vs["CodeModel"])))
            if model is not None
        }
        # Color vertices
        for vx in dag.vs:
            if vx["type"] == VertexType.MAPPING.name:
                vx["color"] = self.node_type_color[vx["type"]]
            elif vx["type"] == VertexType.ENTITY.name:
                vx["color"] = colors_model[vx["CodeModel"]]
            elif "position" in vx.attribute_names():
                vx["color"] = self.color_node_position[vx["position"]]
        return dag

    def _format_etl_dag(self, dag: ig.Graph) -> ig.Graph:
        """Format the ETL DAG for visualization.

        Prepares the ETL DAG for visualization by calculating node levels, setting node hierarchy levels,
        setting visual attributes, and coloring the nodes.

        Args:
            dag (ig.Graph): The ETL DAG to format.

        Returns:
            ig.Graph: The formatted ETL DAG.
        """
        dag = self._calculate_node_levels(dag=dag)
        dag = self._dag_node_hierarchy_level(dag=dag)
        dag = self._set_visual_attributes(dag=dag)
        dag = self._dag_etl_coloring(dag=dag)
        return dag

    def plot_etl_dag(self, file_html: str) -> None:
        """Create a html file with a graphical representation of the ETL DAG

        Args:
            file_html (str): file path that the result should be written to
        """
        try:
            dag = self.get_dag_ETL()
        except NoFlowError:
            logger.error("There are no mappings, so there is no ETL flow to plot!")
            return
        dag = self._format_etl_dag(dag=dag)
        self.plot_graph_html(dag=dag, file_html=file_html)
