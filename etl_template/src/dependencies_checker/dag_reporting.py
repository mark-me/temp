import os
from collections import deque
from enum import Enum, auto
from pathlib import Path

import igraph as ig
import networkx as nx
from pyvis.network import Network

from .dag_generator import DagGenerator, EntityRef, NoFlowError, VertexType
from logtools import get_logger

logger = get_logger(__name__)


class ObjectPosition(Enum):
    START = auto()
    INTERMEDIATE = auto()
    END = auto()
    UNDETERMINED = auto()


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
        dag = self.get_dag_entity(entity=entity)  # Visualization
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

    def get_mapping_order(self) -> list:
        """Returns mappings and order of running (could be parallel,
        in which case other sub-sorting should be implemented if needed)

        Returns:
            list: List of mappings with order
        """
        lst_mappings = []
        try:
            dag = self.get_dag_ETL()
        except NoFlowError:
            logger.error(
                "There are no mappings, so there is no mapping order to generate!"
            )
            return []
        for node in dag.vs:
            if node["type"] == VertexType.MAPPING.name:
                successors = dag.vs[dag.successors(node)[0]]
                dict_successors = {key: successors[key] for key in successors.attribute_names()}
                dict_mapping = {key: node[key] for key in node.attribute_names()}
                dict_mapping["RunLevel"] = node["run_level"]
                dict_mapping["RunLevelStage"] = node["run_level_stage"]
                dict_mapping['NameModel'] = dict_successors["NameModel"]
                dict_mapping['CodeModel'] = dict_successors["CodeModel"]
                dict_mapping["SourceViewName"] = f"vw_src_{node['Name'].replace(' ', '_')}"
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
        # Build model colouring dictionary
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
