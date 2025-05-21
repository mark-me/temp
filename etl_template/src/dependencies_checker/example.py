import json

from dependencies_checker import DagReporting, EntityRef, EtlFailure

if __name__ == "__main__":
    """Examples of the class use-cases
    """
    dir_output = "etl_templates/output/etl_report/"
    dir_RETW = "etl_templates/src/dependencies_checker/retw_examples/"
    # List of RETW files to process, order of the list items is irrelevant
    lst_files_RETW = [
        "Usecase_Aangifte_Behandeling.json",
        "Usecase_Test_BOK.json",
        "DMS_LDM_AZURE_SL.json",
    ]
    lst_files_RETW = [dir_RETW + file for file in lst_files_RETW]
    dag = DagReporting()
    dag.add_RETW_files(files_RETW=lst_files_RETW)

    """File dependencies
    * Visualizes of the total network of files, entities and mappings
    * Visualizes of a given entity's network of connected files, entities and mappings
    * Visualizes dependencies between files, based on entities they have in common
    * Lists entities which are used in mappings, but are not defined in a Power Designer document
    """
    # Visualization of the total network of files, entities and mappings
    dag.plot_graph_total(file_html=f"{dir_output}all.html")
    # Visualization of a given entity's network of connected files, entities and mappings
    entity = EntityRef("Da_Central_CL", "AggrProcedureCategory")
    dag.plot_entity_journey(
        entity=entity,
        file_html=f"{dir_output}entity_journey.html",
    )
    # Visualization of dependencies between files, based on entities they have in common
    dag.plot_file_dependencies(
        file_html=f"{dir_output}file_dependencies.html", include_entities=True
    )
    # Entities which are used in mappings, but are not defined in a Power Designer document
    lst_entities = dag.get_entities_without_definition()
    with open(f"{dir_output}entities_not_defined.jsonl", "w", encoding="utf-8") as file:
        for item in lst_entities:
            file.write(json.dumps(item) + "\n")

    """ETL Flow (DAG)
    * Determine the ordering of the mappings in an ETL flow
    * Visualizes the ETL flow for all RETW files combined
    """
    # Determine the ordering of the mappings in an ETL flow: a list of mapping dictionaries with their RunLevel and RunLevelStage
    lst_mapping_order = dag.get_mapping_order()
    with open(f"{dir_output}mapping_order.jsonl", "w", encoding="utf-8") as file:
        for item in lst_mapping_order:
            file.write(json.dumps(item) + "\n")
    # Visualization of the ETL flow for all RETW files combined
    dag.plot_etl_dag(file_html=f"{dir_output}ETL_flow.html")

    """Failure simulation
    * Sets a failed object status
    * Visualization of the total network of files, entities and mappings
    """
    lst_entities_failed = [
        EntityRef("Da_Central_CL", "AggrLastStatus"),
        EntityRef("Da_Central_BOK", "AggrLastStatus"),
    ]  # Set for other examples
    etl_simulator = EtlFailure()
    # Adding RETW files to generate complete ETL DAG
    etl_simulator.add_RETW_files(files_RETW=lst_files_RETW)
    # Set failed node
    etl_simulator.set_entities_failed(lst_entities_failed)
    # Create fallout report file
    lst_mapping_order = etl_simulator.get_report_fallout()
    with open(f"{dir_output}dag_run_fallout.json", "w", encoding="utf-8") as file:
        json.dump(lst_mapping_order, file, indent=4)
    # Create fallout visualization
    etl_simulator.plot_etl_fallout(file_html=f"{dir_output}dag_run_report.html")

