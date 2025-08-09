import igraph as ig
from logtools import get_logger

from .dag_reporting import DagReporting, EntityRef, NoFlowError, VertexType
from .dag_builder import MappingRef

logger = get_logger(__name__)

class EtlFailure(DagReporting):
    """Beheert en rapporteert ETL-fouten en hun impact in de ETL-DAG.

    Deze klasse markeert gefaalde mappings of entiteiten, bepaalt de impact op stroomafwaartse componenten,
    en biedt methoden voor rapportage en visualisatie van de gevolgen van ETL-fouten.
    """

    def __init__(self):
        super().__init__()
        self.dag = ig.Graph()
        self.impact = []

    def set_mappings_failed(self, mapping_refs: list[MappingRef]) -> None:
        """Sets the specified mappings as failed in the ETL DAG.

        Marks the given mappings as failed and identifies all downstream components affected by these failures.
        The impact of the failures (failed mapping and affected components) is stored for reporting and visualization.

        Args:
            mapping_refs (list[MappingRef]): A list of MappingRef tuples, each representing a failed mapping.

        Returns:
            None
        """
        try:
            dag = self.get_dag_ETL()
        except NoFlowError:
            logger.error("There are no mappings, so there is no ETL flow!")
            return
        for mapping_ref in mapping_refs:
            try:
                id_mapping = self.get_mapping_id(mapping_ref)
                vx_failed = dag.vs.select(name=id_mapping)[0]
            except ValueError:
                code_model, code_entity = mapping_ref
                logger.error(f"Can't find entity '{code_model}.{code_entity}' in ETL flow!")
                continue
            self._set_affected(dag=dag, vx_failed=vx_failed)

    def set_entities_failed(self, entity_refs: list[EntityRef]) -> None:
        """Sets the specified entities as failed in the ETL DAG.

        Marks the given entities as failed and identifies all downstream components affected by these failures.
        The impact of the failures (failed entity and affected components) is stored for reporting and visualization.

        Args:
            entity_refs (list[EntityRef]): A list of EntityRef tuples, each representing a failed entity.

        Returns:
            None
        """
        try:
            dag = self.get_dag_ETL()
        except NoFlowError:
            logger.error("There are no mappings, so there is no ETL flow!")
            return
        for entity_ref in entity_refs:
            try:
                id_entity = self.get_entity_id(entity_ref)
                vx_failed = dag.vs.select(name=id_entity)[0]
            except ValueError:
                code_model, code_entity = entity_ref
                logger.error(f"Can't find entity '{code_model}.{code_entity}' in ETL flow!")
                continue
            self._set_affected(dag=dag, vx_failed=vx_failed)


    def _set_affected(self, dag: ig.Graph, vx_failed: ig.Vertex) -> None:
        """Bepaalt en registreert de impact van een gefaalde knoop in de ETL-DAG.

        Deze functie zoekt alle knopen die stroomafwaarts (downstream) van de gefaalde knoop liggen,
        en slaat deze samen met de gefaalde knoop op in de impactlijst voor rapportage en visualisatie.

        Args:
            dag (ig.Graph): De ETL-DAG waarin de failure is opgetreden.
            vx_failed (ig.Vertex): De gefaalde knoop in de ETL-DAG.

        Returns:
            None
        """
        ids_affected = dag.subcomponent(vx_failed, mode="out")
        ids_affected.remove(vx_failed.index)
        self.impact.append(
            {"failed": vx_failed["name"], "affected": dag.vs(ids_affected)["name"]}
        )

    def _format_failure_impact(self, dag: ig.Graph) -> ig.Graph:
        """Formats the ETL DAG to highlight failed nodes and their affected components.

        Updates the graph visualization by marking failed nodes and all downstream affected nodes in red.
        Failed nodes are also given a star shape for emphasis.

        Args:
            dag (ig.Graph): The ETL DAG to be formatted.

        Returns:
            ig.Graph: The formatted ETL DAG with highlighted failures and affected nodes.
        """
        for failure in self.impact:
            dag.vs.select(name=failure["failed"])["color"] = "red"
            dag.vs.select(name=failure["failed"])["shape"] = "star"
            for affected in failure["affected"]:
                dag.vs.select(name=affected)["color"] = "red"
        return dag

    def get_report_fallout(self) -> list[dict]:
        """Generates a report of the fallout from failed nodes in the ETL DAG.

        Returns a list of dictionaries, each describing a failed node and the affected mappings and entities downstream.
        This report can be used for further analysis or visualization of ETL failures.

        Returns:
            list[dict]: A list of fallout reports, each containing the failed node and its affected mappings and entities.
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
        dag = self.get_dag_ETL()
        dag = self._format_etl_dag(dag=dag)
        dag = self._format_failure_impact(dag=dag)
        self.plot_graph_html(dag=dag, file_html=file_html)
