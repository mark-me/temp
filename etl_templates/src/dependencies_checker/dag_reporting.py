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
        """Initialiseert een nieuwe instantie van de DagReporting klasse.

        Stelt de standaardkleuren, vormen en posities in voor knopen in de grafiek,
        en roept de initialisatie van de bovenliggende klasse aan.
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
            mapping_sources (dict): Een lijst van dictionaries met mappingnamen en hun bijbehorende entiteiten.

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
        lst_edges_igraph = graph.get_edge_dataframe().to_dict("records")
        lst_edges = []
        lst_edges.extend((edge["source"], edge["target"]) for edge in lst_edges_igraph)
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
        net.show(file_html, notebook=False)

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

    def get_entities_without_definition(self) -> list:
        """Geeft een lijst van entiteiten terug die geen definitie in een RETW-bestand hebben.

        Doorloopt alle entiteiten in de totale grafiek en selecteert die zonder inkomende RETW-bestandsknoop,
        zodat ontbrekende definities eenvoudig opgespoord kunnen worden.

        Returns:
            list: Een lijst van attributen van entiteiten zonder definitie in een RETW-bestand.
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
            self._dag_run_level_stages(
                deadlock_prevention=deadlock_prevention
            )
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
