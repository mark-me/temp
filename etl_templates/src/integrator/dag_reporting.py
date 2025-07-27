import os
from collections import deque
from enum import Enum, auto
from pathlib import Path

import igraph as ig
import networkx as nx
from logtools import get_logger
from pyvis.network import Network

from .dag_builder import EntityRef, MappingRef, NoFlowError, VertexType
from .dag_implementation import DagImplementation, DeadlockPrevention

logger = get_logger(__name__)


class ObjectPosition(Enum):
    """Definieert de mogelijke posities van een knoop in de grafiek.

    Elke positie geeft aan of een knoop een startpunt, eindpunt, tussenliggend punt of onbepaalde positie heeft in de DAG.
    """
    START = auto()
    INTERMEDIATE = auto()
    END = auto()
    UNDETERMINED = auto()


class DagReporting(DagImplementation):
    """Extends the DagGenerator class to provide reporting and visualization functionalities.

    This class inherits from DagGenerator and adds functionalities for visualizing DAGs using pyvis,
    setting node attributes for visualization, converting between igraph and networkx graph formats,
    and determining node hierarchy levels for visualization.
    """

    def __init__(self):
        """Initialiseert een nieuwe instantie van de DagReporting klasse.

        Stelt de standaardkleuren, vormen en posities in voor knopen in de grafiek,
        en roept de initialisatie van de bovenliggende klasse aan.
        """
        super().__init__()
        self._progress_description = "Rapporteren ETL afhankelijkheden"
        self.colors_discrete = [
            "#008cf9",
            "#00bbad",
            "#ebac23",
            "#d163e6",
            "#c44542",
            "#ff9287",
            "#00c6f8",
            "#878500",
            "#00a76c",
            "#bdbdbd",
            "#264653",
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
        """Maakt de output directory aan als deze nog niet bestaat.

        Deze functie zorgt ervoor dat de bovenliggende directorystructuur voor het opgegeven bestandspad wordt aangemaakt,
        zodat bestanden veilig kunnen worden weggeschreven.

        Args:
            file_path (str): Het pad naar het bestand waarvoor de directory moet worden aangemaakt.

        Returns:
            None
        """
        parent_directory = os.path.dirname(file_path)
        Path(parent_directory).mkdir(parents=True, exist_ok=True)

    def _igraph_to_networkx(self, graph: ig.Graph) -> nx.DiGraph:
        """Converteert een igraph.Graph naar een networkx.DiGraph.

        Zet de knopen en randen van een igraph-object om naar een networkx DiGraph,
        zodat deze gebruikt kan worden voor verdere analyse of visualisatie.

        Args:
            graph (ig.Graph): De igraph grafiek die geconverteerd moet worden.

        Returns:
            nx.DiGraph: De geconverteerde networkx DiGraph.
        """
        dag_nx = nx.DiGraph()
        # Convert nodes
        lst_nodes_igraph = graph.get_vertex_dataframe().to_dict("records")
        lst_nodes = []
        lst_nodes.extend((node["name"], node) for node in lst_nodes_igraph)
        dag_nx.add_nodes_from(lst_nodes)

        # Convert edges
        lst_edges = []
        lst_edges.extend((edge["source"], edge["target"]) for edge in graph.es)
        dag_nx.add_edges_from(lst_edges)
        return dag_nx

    def _set_node_tooltip(self, node: ig.Vertex) -> None:
        """Stelt de tooltip in voor een knoop op basis van het type en de attributen.

        Genereert een tooltip-string voor de opgegeven knoop, inclusief relevante informatie zoals naam, code,
        model, run level en aanmaak-/wijzigingsmetadata, afhankelijk van het type en de beschikbare attributen.

        Args:
            node (ig.Vertex): De knoop waarvoor de tooltip wordt ingesteld.

        Returns:
            None
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
            if "etl_level" in node.attribute_names():
                node["title"] = (
                    node["title"]
                    + f"""
                        Hierarchy level: {str(node["etl_level"])}\n
                    """
                )
        if (
            node["type"] == VertexType.MAPPING.name
            and "run_level" in node.attribute_names()
        ):
            node["title"] = (
                node["title"]
                + f"""
                    Run level: {str(node["run_level"])}\n
                """
            )
            if "run_level_stage" in node.attribute_names():
                node["title"] = (
                    node["title"]
                    + f"""
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
        """Stelt de visuele attributen in voor alle knopen in de grafiek.

        Deze functie wijst vormen, kleuren en schaduwen toe aan knopen op basis van hun type,
        en stelt de tooltip in voor elke knoop voor visualisatiedoeleinden.

        Args:
            dag (ig.Graph): De grafiek waarvan de knopen visueel worden opgemaakt.

        Returns:
            ig.Graph: De grafiek met ingestelde visuele attributen.
        """
        logger.info("Setting graphical attributes of the graph")
        for node in dag.vs:
            #test_attr = node.attributes()
            node["shape"] = self.node_type_shape[node["type"]]
            node["shadow"] = True
            node["color"] = self.node_type_color[node["type"]]
            self._set_node_tooltip(node)
        return dag

    def plot_graph_html(self, dag: ig.Graph, file_html: str) -> None:
        """Genereert en slaat een interactieve HTML-visualisatie van de grafiek op.

        Zet de grafiek om naar een networkx-formaat, stelt de visualisatieopties in,
        en slaat het resultaat op als een HTML-bestand.

        Args:
            dag (ig.Graph): De grafiek die gevisualiseerd moet worden.
            file_html (str): Het pad naar het HTML-bestand waarin de visualisatie wordt opgeslagen.

        Returns:
            None
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
        net.save_graph(file_html)

    def _dag_node_hierarchy_level(self, dag: ig.Graph) -> ig.Graph:
        """Bepaalt en stelt de hiërarchieniveaus in voor alle knopen in de DAG.

        Deze functie berekent het niveau van elke knoop op basis van zijn voorgangers
        en past het maximale niveau toe op eindknopen voor een consistente visualisatie.

        Args:
            dag (ig.Graph): De DAG waarvan de hiërarchieniveaus bepaald moeten worden.

        Returns:
            ig.Graph: De DAG met ingestelde hiërarchieniveaus voor alle knopen.
        """
        dag = self._calculate_node_levels(dag)
        dag = self._set_max_end_node_level(dag)
        return dag

    def _calculate_node_levels(self, dag: ig.Graph) -> ig.Graph:
        """Berekent het hiërarchieniveau voor elke knoop in de DAG.

        Deze functie bepaalt het niveau van elke knoop op basis van het aantal voorgangers,
        zodat de hiërarchische structuur van de grafiek inzichtelijk wordt voor visualisatie of verdere verwerking.

        Args:
            dag (ig.Graph): De DAG waarvan de knoopniveaus berekend moeten worden.

        Returns:
            ig.Graph: De DAG met toegevoegde 'level' attributen voor alle knopen.
        """
        # Getting the number of preceding nodes to determine where to start
        for i in range(dag.vcount()):
            dag.vs[i]["qty_predecessors"] = len(dag.subcomponent(dag.vs[i], mode="in"))

        # Calculating levels
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
        """Bepaalt de positiecategorie van elke knoop in de DAG op basis van inkomende en uitgaande verbindingen.

        Deze functie classificeert knopen als START, INTERMEDIATE, END of UNDETERMINED,
        afhankelijk van het aantal inkomende en uitgaande verbindingen, en voegt deze categorie toe als attribuut.

        Args:
            dag (ig.Graph): De DAG waarvan de knoopposities gecategoriseerd moeten worden.

        Returns:
            ig.Graph: De DAG met toegevoegde 'position' attributen voor alle knopen.
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
        """Stelt het maximale hiërarchieniveau in voor alle eindknopen in de DAG.

        Deze functie zoekt alle knopen met de positie END en wijst het hoogste gevonden niveau toe aan deze knopen,
        zodat eindknopen op hetzelfde hiërarchische niveau worden weergegeven in de visualisatie.

        Args:
            dag (ig.Graph): De DAG waarvan de eindknopen het maximale niveau moeten krijgen.

        Returns:
            ig.Graph: De DAG met bijgewerkte niveaus voor eindknopen.
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
        """Genereert en slaat een netwerkvisualisatie op van alle bestanden, entiteiten en mappings.

        Bouwt de volledige grafiek, stelt de visuele attributen in en slaat het resultaat op als een HTML-bestand.

        Args:
            file_html (str): Het pad naar het HTML-bestand waarin de visualisatie wordt opgeslagen.

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
        """Genereert en slaat een netwerkvisualisatie op van de afhankelijkheden tussen RETW-bestanden.

        Bouwt een grafiek van de RETW-bestandsafhankelijkheden, stelt de visuele attributen in,
        en slaat het resultaat op als een HTML-bestand.

        Args:
            file_html (str): Het pad naar het HTML-bestand waarin de visualisatie wordt opgeslagen.
            include_entities (bool, optional): Of entiteiten moeten worden opgenomen in de visualisatie. Standaard True.

        Returns:
            None
        """
        logger.info(
            f"Creating a network plot, '{file_html}', showing RETW file dependencies"
        )
        dag_files = self.get_dag_file_dependencies(include_entities=include_entities)
        dag_files = self._set_visual_attributes(dag=dag_files)
        self.plot_graph_html(dag=dag_files, file_html=file_html)

    def plot_mappings(self, file_html: str) -> None:
        """Genereert en slaat een netwerkvisualisatie op van de mappings.

        Bouwt een grafiek van de mappings, stelt de visuele attributen in,
        en slaat het resultaat op als een HTML-bestand.

        Args:
            file_html (str): Het pad naar het HTML-bestand waarin de visualisatie wordt opgeslagen.

        Returns:
            None
        """
        logger.info( f"Creating a network plot, '{file_html}', showing only mappings")
        dag = self.get_dag_mappings()
        dag = self._set_visual_attributes(dag=dag)
        self.plot_graph_html(dag=dag, file_html=file_html)

    def plot_entity_journey(self, entity: EntityRef, file_html: str) -> None:
        """Genereert en slaat een netwerkvisualisatie op van alle afhankelijkheden van een specifieke entiteit.

        Bouwt een grafiek van de afhankelijkheden van de opgegeven entiteit, stelt de visuele attributen in,
        markeert de entiteit, en slaat het resultaat op als een HTML-bestand.

        Args:
            entity (EntityRef): De entiteit waarvan de afhankelijkheden gevisualiseerd moeten worden.
            file_html (str): Het pad naar het HTML-bestand waarin de visualisatie wordt opgeslagen.

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

    def get_entities_without_definition(self) -> list[dict]:
        """Geeft een lijst van entiteiten terug die geen definitie in een RETW-bestand hebben.

        Doorloopt alle entiteiten in de totale grafiek en selecteert die zonder inkomende RETW-bestandsknoop,
        zodat ontbrekende definities eenvoudig opgespoord kunnen worden.

        Returns:
            list[dict]: Een lijst van attributen van entiteiten zonder definitie in een RETW-bestand.
        """
        lst_entities = []
        dag = self.get_dag_total()
        vs_entities = dag.vs.select(type_eq=VertexType.ENTITY.name)
        for vx_entity in vs_entities:
            vs_in = dag.vs(dag.neighbors(vx_entity, mode="in"))
            if not [vx for vx in vs_in if vx["type"] == VertexType.FILE_RETW.name]:
                lst_entities.append(vx_entity.attributes())
        return lst_entities

    def _format_etl_dag(self, dag: ig.Graph) -> ig.Graph:
        """Formatteert de ETL-DAG voor visualisatie door niveaus, hiërarchie, visuele attributen en kleuren toe te voegen.

        Deze functie verrijkt de ETL-DAG stapsgewijs zodat deze geschikt is voor grafische weergave,
        inclusief hiërarchische niveaus, visuele kenmerken en kleurcodering.

        Args:
            dag (ig.Graph): De ETL-DAG die geformatteerd moet worden.

        Returns:
            ig.Graph: De geformatteerde ETL-DAG gereed voor visualisatie.
        """
        dag = self._calculate_node_levels(dag=dag)
        dag = self._dag_node_hierarchy_level(dag=dag)
        dag = self._set_visual_attributes(dag=dag)
        dag = self._dag_etl_coloring(dag=dag)
        return dag

    def _dag_etl_coloring(self, dag: ig.Graph) -> ig.Graph:
        """Kleurt de knopen in de ETL-DAG op basis van hun type en model.

        Wijs kleuren toe aan mappings, entiteiten en andere knopen zodat de visualisatie
        van de ETL-DAG duidelijk onderscheid maakt tussen verschillende typen en modellen.
        """
        if vs_entities := dag.vs.select(type_eq=VertexType.ENTITY.name):
            colors_model = {
                model: self.colors_discrete[i]
                for i, model in enumerate(list(set(vs_entities["CodeModel"])))
                if model is not None
            }

        # Color vertices
        # Priority: position > type (MAPPING) > type (ENTITY/CodeModel)
        for vx in dag.vs:
            if vx["type"] == VertexType.MAPPING.name:
                vx["color"] = self.node_type_color[vx["type"]]
            elif vx["type"] == VertexType.ENTITY.name:
                vx["color"] = colors_model[vx["CodeModel"]]
            elif "position" in vx.attribute_names():
                vx["color"] = self.color_node_position[vx["position"]]
        return dag

    def plot_etl_dag(self, file_html: str) -> None:
        """Genereert en slaat een netwerkvisualisatie op van de ETL-DAG.

        Bouwt de ETL-grafiek, verrijkt deze voor visualisatie en slaat het resultaat op als een HTML-bestand.

        Args:
            file_html (str): Het pad naar het HTML-bestand waarin de visualisatie wordt opgeslagen.

        Returns:
            None
        """
        try:
            dag = self.get_dag_ETL()
        except NoFlowError:
            logger.error("There are no mappings, so there is no ETL flow to plot!")
            return
        dag = self._format_etl_dag(dag=dag)
        self.plot_graph_html(dag=dag, file_html=file_html)
