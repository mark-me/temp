import igraph as ig

from .dag_reporting import DagReporting, NoFlowError, VertexType
from logtools import get_logger

logger = get_logger(__name__)

class EtlFailure(DagReporting):
    def __init__(self):
        super().__init__()
        self.dag = ig.Graph()
        self.impact = []

    def set_entities_failed(self, entity_refs: list) -> None:
        """Sets the specified entities as failed in the ETL DAG.

        Marks the given entities as failed and identifies all downstream components affected by these failures.
        The impact of the failures (failed entity/mapping and affected components) is stored for reporting and visualization.

        Args:
            entity_refs (list): A list of EntityRef tuples, each representing a failed entity.

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
            ids_affected = dag.subcomponent(vx_failed, mode="out")
            ids_affected.remove(vx_failed.index)
            self.impact.append(
                {"failed": vx_failed["name"], "affected": dag.vs(ids_affected)["name"]}
            )

    def _format_failure_impact(self, dag: ig.Graph) -> ig.Graph:
        """Update the DAG to reflect the impact of failed nodes.

        Identifies and marks nodes affected by the failures, updating their visual attributes (color, shape) in the DAG.

        Returns:
            ig.Graph: The updated DAG.
        """
        for failure in self.impact:
            dag.vs.select(name=failure["failed"])["color"] = "red"
            dag.vs.select(name=failure["failed"])["shape"] = "star"
            for affected in failure["affected"]:
                dag.vs.select(name=affected)["color"] = "red"
        return dag

    def get_report_fallout(self) -> list:
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
        dag = self.get_dag_ETL()
        dag = self._format_etl_dag(dag=dag)
        dag = self._format_failure_impact(dag=dag)
        self.plot_graph_html(dag=dag, file_html=file_html)
