from collections import deque
from copy import deepcopy
from enum import Enum, auto

import igraph as ig
from logtools import get_logger

from .dag_implementation import (
    DagImplementation,
    DeadlockPrevention,
    MappingRef,
    VertexType,
)

logger = get_logger(__name__)


class FailureStrategy(Enum):
    ONLY_SUCCESSORS = "Only successors"
    SIBLINGS_OF_MAPPINGS = "Sibling mappings"
    SIBLINGS_OF_AGGREGATES = "Sibling aggregates"
    ALL_OF_SHARED_TARGET = "All shared targets"
    WHOLE_SUBCOMPONENT = "Whole subcomponent"
    RUN_LEVEL = "Run level"


class MappingStatus(Enum):
    NOK = "Failed"
    OK = "Success"
    DNR = "Did not run"
    OKR = "Success, but needs restoring"

class ObjectPosition(Enum):
    START = auto()
    INTERMEDIATE = auto()
    END = auto()
    UNDETERMINED = auto()

class EtlSimulator(DagImplementation):
    def __init__(self):
        """Initialiseert een nieuwe EtlSimulator instantie voor het simuleren van ETL-DAG's.

        Zet de standaardwaarden voor de simulatie-DAG, statuskleuren, entiteitkleur en gefaalde mappings.

        Returns:
            None
        """
        super().__init__()
        self.dag_simulation: ig.Graph = None
        self.colors_status = {
            MappingStatus.OK: "limegreen",
            MappingStatus.NOK: "red",
            MappingStatus.DNR: "gold",
            MappingStatus.OKR: "orange",
        }
        self.color_entity = "lemonchiffon"
        self.vs_mapping_failed: list[MappingRef] = []

    def build_dag(self, files_RETW):
        """Bouwt de ETL-DAG op basis van de opgegeven RETW-bestanden.

        Initialiseert de simulatie-DAG en wijst hiërarchieniveaus en standaardstatussen toe aan de knooppunten.

        Args:
            files_RETW: De RETW-bestanden die gebruikt worden om de DAG te bouwen.

        Returns:
            None
        """
        super().build_dag(files_RETW)
        self._dag_run_level_stages(deadlock_prevention=DeadlockPrevention.TARGET)
        self.dag_simulation = self.dag_simulation = self.get_dag_ETL()
        self._dag_node_hierarchy_level()
        for vx in self.dag_simulation.vs.select(type_eq=VertexType.MAPPING.name):
            vx["run_status"] = MappingStatus.DNR
            vx["is_aggregate"] = (
                vx["EntityTarget"]["Stereotype"] == "mdde_AggregateBusinessRule"
            )
    def _dag_node_hierarchy_level(self) -> None:
        """Bepaalt en stelt de hiërarchieniveaus in voor alle knopen in de DAG.

        Deze functie berekent het niveau van elke knoop op basis van zijn voorgangers
        en past het maximale niveau toe op eindknopen voor een consistente visualisatie.
        """
        self._calculate_node_levels()
        self._set_max_end_node_level()

    def _calculate_node_levels(self) -> None:
        """Berekent het hiërarchieniveau voor elke knoop in de DAG.

        Deze functie bepaalt het niveau van elke knoop op basis van het aantal voorgangers,
        zodat de hiërarchische structuur van de grafiek inzichtelijk wordt voor visualisatie of verdere verwerking.

        Args:
            dag (ig.Graph): De DAG waarvan de knoopniveaus berekend moeten worden.

        Returns:
            None
        """
        # Getting the number of preceding nodes to determine where to start
        for vx in self.dag_simulation.vs:
            vx["qty_predecessors"] = len(self.dag_simulation.subcomponent(vx, mode="in"))

        # Calculating levels
        id_vertices = deque(
            [(vtx, 0) for vtx in self.dag_simulation.vs.select(qty_predecessors_eq=1).indices]
        )
        while id_vertices:
            id_vx, level = id_vertices.popleft()
            self.dag_simulation.vs[id_vx]["level"] = level
            id_vertices.extend(
                [(vtx, level + 1) for vtx in self.dag_simulation.neighbors(id_vx, mode="out")]
            )

    def _set_max_end_node_level(self) -> None:
        """Stelt het maximale hiërarchieniveau in voor alle eindknopen in de DAG.

        Deze functie zoekt alle knopen met de positie END en wijst het hoogste gevonden niveau toe aan deze knopen,
        zodat eindknopen op hetzelfde hiërarchische niveau worden weergegeven in de visualisatie.

        Args:
            dag (ig.Graph): De DAG waarvan de eindknopen het maximale niveau moeten krijgen.

        Returns:
            ig.Graph: De DAG met bijgewerkte niveaus voor eindknopen.
        """
        self._dag_node_position_category()
        end_levels = [
            self.dag_simulation.vs[vtx]["level"]
            for vtx in range(
                self.dag_simulation.vcount()
            )  # Iterate over all vertices to find the true max level.
            if self.dag_simulation.vs[vtx]["position"] == ObjectPosition.END.name
        ]
        level_max = max(end_levels, default=0)
        for vx in self.dag_simulation.vs:
            if vx["position"] == ObjectPosition.END.name:
                vx["level"] = level_max

    def _dag_node_position_category(self) -> None:
        """Bepaalt de positiecategorie van elke knoop in de DAG op basis van inkomende en uitgaande verbindingen.

        Deze functie classificeert knopen als START, INTERMEDIATE, END of UNDETERMINED,
        afhankelijk van het aantal inkomende en uitgaande verbindingen, en voegt deze categorie toe als attribuut.

        Args:
            dag (ig.Graph): De DAG waarvan de knoopposities gecategoriseerd moeten worden.

        Returns:
            ig.Graph: De DAG met toegevoegde 'position' attributen voor alle knopen.
        """
        self.dag_simulation.vs["qty_out"] = self.dag_simulation.degree(self.dag_simulation.vs, mode="out")
        self.dag_simulation.vs["qty_in"] = self.dag_simulation.degree(self.dag_simulation.vs, mode="in")
        lst_entity_position = []
        for qty_in, qty_out in zip(self.dag_simulation.vs["qty_in"], self.dag_simulation.vs["qty_out"]):
            if qty_in == 0 and qty_out > 0:
                position = ObjectPosition.START.name
            elif qty_in > 0 and qty_out > 0:
                position = ObjectPosition.INTERMEDIATE.name
            elif qty_in > 0 and qty_out == 0:
                position = ObjectPosition.END.name
            else:
                position = ObjectPosition.UNDETERMINED.name
            lst_entity_position.append(position)
        self.dag_simulation.vs["position"] = lst_entity_position

    def set_mappings_failed(self, mapping_refs: list[MappingRef]) -> None:
        """Markeert opgegeven mappings als gefaald in de ETL-DAG en registreert de impact.

        Deze functie zoekt de opgegeven mappings in de ETL-DAG, markeert ze als gefaald,
        en bepaalt welke downstream componenten hierdoor worden beïnvloed. De impact wordt opgeslagen
        voor rapportage en visualisatie.

        Args:
            mapping_refs (list): Een lijst van MappingRef tuples, elk representerend een gefaalde mapping.

        Returns:
            None
        """
        for mapping_ref in mapping_refs:
            try:
                id_mapping = self.get_mapping_id(mapping_ref)
                self.vs_mapping_failed.append(
                    self.dag_simulation.vs.select(name=id_mapping)[0]
                )
            except (ValueError, IndexError):
                code_model, code_entity = mapping_ref
                logger.error(
                    f"Can't find entity '{code_model}.{code_entity}' in ETL flow!"
                )
                continue

    def start_etl(self):
        run_levels = {vx["run_level"] for vx in self.dag_simulation.vs.select(type_eq=VertexType.MAPPING.name)}
        test = [vx.attributes() for vx in self.dag_simulation.vs.select(type_eq=VertexType.MAPPING.name)]
        # Progress by run level
        for run_level in run_levels:
            vs_run_level = self.dag_simulation.vs.select(run_level_eq=run_level)
            run_stages = {vx["run_level_stage"] for vx in vs_run_level}
            # Progress by run level stages
            for run_stage in run_stages:
                vs_run_stage = self.dag_simulation.vs.select(run_level_stage_eq=run_stage)
                for vx_run in vs_run_stage:
                    if vx_run in self.vs_mapping_failed:
                        vx_run["run_status"] = MappingStatus.NOK
                    else:
                        vx_run["run_status"] = MappingStatus.OK
                    test = vx_run.attributes()
                pass
        pass

    def _apply_failure_strategy(self, strategy: FailureStrategy):
        """Past de geselecteerde faalstrategie toe op de ETL-DAG-simulatie.

        Werkt de run-status van knooppunten in de DAG bij op basis van de opgegeven faalstrategie,
        waarbij gefaalde mappings en hun getroffen componenten worden gemarkeerd.

        Args:
            strategy (FailureStrategy): De toe te passen faalstrategie.

        Returns:
            None
        """
        for vx in self.dag_simulation.vs.select(type_eq=VertexType.MAPPING.name):
            vx["run_status"] = MappingStatus.OK
        for vx in self.vs_mapping_failed:
            vx["run_status"] = MappingStatus.NOK
        if strategy == FailureStrategy.ONLY_SUCCESSORS:
            self._apply_strategy_only_successors()

    def _apply_strategy_only_successors(self):
        """Markeert alleen de opvolgers van gefaalde mappings als 'Did not run' in de ETL-DAG.

        Doorloopt alle gefaalde mappings en wijzigt de run-status van hun directe en indirecte opvolgers,
        met uitzondering van de gefaalde mapping zelf, naar 'Did not run'.

        Returns:
            None
        """
        for vx in self.vs_mapping_failed:
            id_vs = self.dag_simulation.subcomponent(vx, mode="out")
            id_vs = [id_vx for id_vx in id_vs if id_vx != vx.index]
            for id_vx in id_vs:
                self.dag_simulation.vs[id_vx]["run_status"] = MappingStatus.DNR

    def _apply_strategy_shared_target(self):
        for vx in self.vs_mapping_failed:
            id_vs_successors = self.dag_simulation.subcomponent(vx, mode="out")
            id_vs_successors = [id_vx for id_vx in id_vs_successors if id_vx != vx.index]
            for id_successor in id_vs_successors:
                self.dag_simulation.vs[id_successor]["run_status"] = MappingStatus.DNR
                id_vs_predecessors = self.dag_simulation.subcomponent(self.dag_simulation.vs[id_successor], mode="out")
                id_vs_predecessors = [id_vx for id_vx in id_vs_predecessors if id_vx != vx.index]
                for id_predecessor in id_vs_predecessors:
                    run_status = self.dag_simulation.vs[id_predecessor]["run_status"]
                    self.dag_simulation.vs[id_predecessor]["run_status"] = MappingStatus.DNR

    def _format_failure_impact(self, dag: ig.Graph) -> None:
        """Formatteert de impact van falen in de ETL-DAG voor visualisatie.

        Wijzigt labels, kleuren en vormen van knooppunten in de opgegeven DAG op basis van
        hun status en type, zodat de impact van falen duidelijk zichtbaar is in de visualisatie.

        Args:
            dag (ig.Graph): De ETL-DAG die geformatteerd moet worden.

        Returns:
            None
        """
        for vx in dag.vs:
            vx["label"] = f"{vx['CodeModel']}\n{vx['Code']}"
            if vx["type"] == VertexType.MAPPING.name:
                vx["color"] = self.colors_status[vx["run_status"]]
                if vx["is_aggregate"]:
                    vx["shape"] = "triangle-down"
            elif vx["type"] == VertexType.ENTITY.name:
                vx["shape"] = "square"
                vx["color"] = self.color_entity

    def plot_etl_fallout(
        self, failure_strategy: FailureStrategy, file_png: str
    ) -> None:
        """Visualiseert de impact van een faalstrategie op de ETL-DAG en slaat het resultaat op als HTML-bestand.

        Past de opgegeven faalstrategie toe, bepaalt de getroffen componenten en genereert een visualisatie van
        de ETL-DAG met de impact van falen.

        Args:
            failure_strategy (FailureStrategy): De toe te passen faalstrategie.
            file_html (str): Het pad naar het HTML-bestand waarin de visualisatie wordt opgeslagen.

        Returns:
            None
        """
        self._apply_failure_strategy(strategy=failure_strategy)
        dag_report = self._get_affected_components()
        self._format_failure_impact(dag=dag_report)
        hierarchy = [vx["level"] for vx in dag_report.vs]
        layout = dag_report.layout_sugiyama(layers=hierarchy)
        visual_style = {
            "vertex_label": dag_report.vs["label"],
            "vertex_color": dag_report.vs["color"],
            "vertex_label_dist": 2,
            "vertex_label_angle": 0,
        }
        ig.plot(
            dag_report,
            layout=layout,
            target=file_png,
            bbox=(0, 0, 1920, 1080),
            **visual_style,
        )

    def _get_affected_components(self):
        """Bepaalt en retourneert het subgraaf van de ETL-DAG met alleen de getroffen componenten.

        Selecteert alle knooppunten die direct of indirect zijn beïnvloed door falen en retourneert een subgraaf met alleen deze componenten.

        Returns:
            ig.Graph: De subgraaf met getroffen componenten.
        """
        vs_affected = [
            vx for vx in self.dag_simulation.vs if vx["run_status"] in [MappingStatus.NOK, MappingStatus.DNR]
        ]
        id_vs_dag = [vx.index for vx in self.dag_simulation.vs]
        id_vs_components = []
        for vx in vs_affected:
            ids_affected = self.dag_simulation.subcomponent(vx, mode="all")
            id_vs_components.extend(ids_affected)

        ids_delete = list(set(id_vs_dag) - set(id_vs_components))
        dag = deepcopy(self.dag_simulation)
        dag.delete_vertices(ids_delete)
        return dag
