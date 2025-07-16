from enum import Enum, auto

import igraph as ig
from logtools import get_logger

from .dag_reporting import DagReporting, MappingRef, NoFlowError, VertexType

logger = get_logger(__name__)

class FailureStrategy(Enum):
    ONLY_SUCCESSORS = auto()
    SIBLINGS_OF_MAPPINGS = auto()
    SIBLINGS_OF_AGGREGATES = auto()
    ALL_OF_SHARED_TARGET = auto()
    WHOLE_SUBCOMPONENT = auto()
    RUN_LEVEL = auto()

class EtlSimulator(DagReporting):
    def __init__(self):
        super().__init__()
        self.dag_simulation: ig.Graph = None
        self.colors_status = {
            "OK": "#92FA72",
            "NOK": "#FA8072",
            "DNR": "#72DAFA"
        }
        self.vs_mapping_failed: list[MappingRef] = []
        self.vs_mapping_impact: list[MappingRef] = []

    def set_mappings_failed(self, mapping_refs: list[MappingRef]) -> None:
        """Markeert opgegeven mappings als gefaald in de ETL-DAG en registreert de impact.

        Deze functie zoekt de opgegeven mappings in de ETL-DAG, markeert ze als gefaald,
        en bepaalt welke downstream componenten hierdoor worden beïnvloed. De impact wordt opgeslagen voor rapportage en visualisatie.

        Args:
            mapping_refs (list): Een lijst van MappingRef tuples, elk representerend een gefaalde mapping.

        Returns:
            None
        """
        try:
            self.dag_simulation = self.get_dag_ETL()
            for vx in self.dag_simulation.vs.select(type_eq=VertexType.MAPPING.name):
                vx["run_status"] = "OK"
        except NoFlowError:
            logger.error("There are no mappings, so there is no ETL flow!")
            return
        for mapping_ref in mapping_refs:
            try:
                id_mapping = self.get_mapping_id(mapping_ref)
                self.vs_mapping_failed.append(self.dag_simulation.vs.select(name=id_mapping)[0])
            except ValueError:
                code_model, code_entity = mapping_ref
                logger.error(f"Can't find entity '{code_model}.{code_entity}' in ETL flow!")
                continue

    def _set_affected(self, vx_failed: ig.Vertex) -> None:
        """Bepaalt en registreert de impact van een gefaalde knoop in de ETL-DAG.

        Deze functie zoekt alle knopen die stroomafwaarts (downstream) van de gefaalde knoop liggen,
        en slaat deze samen met de gefaalde knoop op in de impactlijst voor rapportage en visualisatie.

        Args:
            dag (ig.Graph): De ETL-DAG waarin de failure is opgetreden.
            vx_failed (ig.Vertex): De gefaalde knoop in de ETL-DAG.

        Returns:
            None
        """
        ids_affected = self.dag_simulation.subcomponent(vx_failed, mode="out")
        ids_affected.remove(vx_failed.index)
        self.impact.append(
            {"failed": vx_failed["name"], "affected": self.dag_simulation.vs(ids_affected)["name"]}
        )

    def _format_failure_impact(self) -> None:
        """Update the DAG to reflect the impact of failed nodes.

        Identifies and marks nodes affected by the failures, updating their visual attributes (color, shape) in the DAG.

        Returns:
            ig.Graph: The updated DAG.
        """
        for vx in self.dag_simulation.vs.select(type_eq=VertexType.MAPPING.name):
            vx["color"] = self.colors_status[vx["run_status"]]

    def get_report_fallout(self) -> list[dict]:
        """Retrieves dictionary reporting on the affected ETL components

        Returns:
            list: Report on mappings and entities that failed or are affected by the failure
        """
        result = []
        dag = self.get_dag_ETL()
        for failure in self.impact:
            vs_affected = dag.vs.select(name_in=failure["affected"])
            mappings_data = [
                vx.attributes()
                for vx in vs_affected
                if vx["type"] == VertexType.MAPPING.name
            ]
            entities_data = [
                vx.attributes()
                for vx in vs_affected
                if vx["type"] == VertexType.ENTITY.name
            ]
            failed = [vx.attributes() for vx in dag.vs.select(name=failure["failed"])][0]
            result.append(
                {
                    "failed": failed,
                    "affected": {"mappings": mappings_data, "entities": entities_data},
                }
            )
        return result

    def plot_etl_fallout(self, file_html: str) -> None:
        """Plots the ETL fallout graph, highlighting failed nodes and their impact.

        Generates an HTML visualization of the ETL DAG, highlighting the failed entities/mappings and the downstream
        components affected by the failure.

        Args:
            file_html (str): Path to the output HTML file.

        Returns:
            None
        """
        self._format_etl_dag()
        self._format_failure_impact()
        self.plot_graph_html(dag=self.dag_simulation, file_html=file_html)
