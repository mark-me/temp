import json
import hashlib
from collections import namedtuple
from datetime import datetime
from enum import Enum, auto
from pathlib import Path

import igraph as ig

from logtools import get_logger

logger = get_logger(__name__)

EntityRef = namedtuple("EntityRef", ("CodeModel", "CodeEntity"))
MappingRef = namedtuple("MappingRef", ("FileRETW", "CodeMapping"))


class VertexType(Enum):
    """Enumerates the types of vertices in the graph.

    Provides distinct identifiers for each type of node in the graph, including entities, mappings, and files.
    """

    ENTITY = auto()
    MAPPING = auto()
    FILE_RETW = auto()
    ERROR = auto()


class EdgeType(Enum):
    """Enumerates the types of edges in the graph.

    Provides distinct identifiers for each type of edge in the graph, representing relationships between files, mappings, and entities.
    """

    FILE_ENTITY = auto()
    FILE_MAPPING = auto()
    ENTITY_SOURCE = auto()
    ENTITY_TARGET = auto()


class NoFlowError(Exception):
    pass


class DagGenerator:
    """Genereert en beheert gerichte acyclische grafen (DAG's) die ETL-processen representeren.

    Deze klasse verzorgt het aanmaken, bewerken en analyseren van DAG's op basis van
    geëxtraheerde informatie uit RETW-bestanden. Ze biedt methoden om RETW-bestanden toe te voegen,
    DAG's te bouwen die het volledige ETL-proces of individuele bestanden representeren,
    de uitvoeringsvolgorde te bepalen en afhankelijkheden te identificeren.
    """

    def __init__(self):
        """Initialiseert een nieuwe instantie van de klasse DagGenerator.

        Stelt de begintoestand in door lege dictionaries aan te maken voor het opslaan van RETW-bestanden, entiteiten en mappings,
        en een lijst voor het opslaan van verbindingen (edges). Deze datastructuren worden gevuld naarmate RETW-bestanden worden toegevoegd en verwerkt.
        """
        self.files_RETW = {}
        self.entities = {}
        self.mappings = {}
        self.edges = []

    def add_RETW_files(self, files_RETW: list) -> bool:
        """Verwerk meerdere RETW-bestanden.

        Verwerkt elk RETW-bestand in de invoerlijst.

        Args:
            files_RETW (list): Lijst van RETW-bestanden met mappings.

        Retourneert:
            bool: Geeft aan of alle RETW-bestanden zijn verwerkt.
        """
        # Make sure added files are unique
        files_RETW = list(dict.fromkeys(files_RETW))

        # Process files
        for file_RETW in files_RETW:
            # Add file to parser
            if not self.add_RETW_file(file_RETW=file_RETW):
                logger.error(f"Failed to add RETW file '{file_RETW}'")
                return False
        return True

    def add_RETW_file(self, file_RETW: str) -> bool:
        """Laadt een RETW json bestand

        Args:
            file_RETW (str): RETW bestand met modellen en/of mappings

        Returns:
            bool: Geeft aan of alle RETW-bestand is verwerkt.
        """
        try:
            with open(file_RETW) as file:
                dict_RETW = json.load(file)
            logger.info(f"Added RETW file '{file_RETW}'")
        except FileNotFoundError:
            logger.error(f"Could not find file '{file_RETW}'")
            return False
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON content in file '{file_RETW}'")
            return False

        # Add file node information
        order_added = len(self.files_RETW)
        id_file = self.get_file_id(file_RETW)
        self.files_RETW |= {
            id_file: {
                "name": id_file,
                "type": VertexType.FILE_RETW.name,
                "Order": order_added,
                "FileRETW": file_RETW,
                "CreationDate": datetime.fromtimestamp(
                    Path(file_RETW).stat().st_ctime
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "ModificationDate": datetime.fromtimestamp(
                    Path(file_RETW).stat().st_mtime
                ).strftime("%Y-%m-%d %H:%M:%S"),
            }
        }

        logger.info(f"Adding entities 'created' in the RETW file '{file_RETW}'")
        self._add_model_entities(file_RETW=file_RETW, dict_RETW=dict_RETW)
        if "Mappings" in dict_RETW:
            logger.info(f"Adding mappings from the RETW file '{file_RETW}'")
            self._add_mappings(file_RETW=file_RETW, mappings=dict_RETW["Mappings"])
        else:
            logger.warning(f"No mappings from the RETW file '{file_RETW}'")
        return True

    def _stable_hash(self, key: str) -> int:
        """Genereer een stabiele hash van een string.

        Maakt een stabiele hashwaarde van een opgegeven string met behulp van MD5.
        De hash wordt omgezet naar een geheel getal voor consistent gebruik.

        Args:
            key (str): De invoerstring.

        Retourneert:
            int: De stabiele hashwaarde als geheel getal.
        """
        str_bytes = bytes(key, "UTF-8")
        hash_md5 = hashlib.md5(str_bytes)
        return int(hash_md5.hexdigest(), base=16)

    def get_file_id(self, file: str) -> int:
        """Maakt een stabiele hashwaarde voor een RETW bestand.

        Args:
            file (str): De bestandslocatie

        Returns:
            int: Maakt een stabiele hashwaarde voor een RETW bestand.
        """
        return self._stable_hash(key=file.name)

    def get_entity_id(self, entity_ref: EntityRef) -> int:
        """Genereer een stabiele hash-ID voor een entiteit.

        Maakt een stabiele hashwaarde op basis van de gecombineerde entiteitscode en modelcode.

        Args:
            entity_ref (EntityRef): Een namedtuple met de code van het model en de code van de entiteit.

        Retourneert:
            int: De stabiele hash-ID voor de entiteit.
        """
        code_model, code_entity = entity_ref
        return self._stable_hash(key=code_model + code_entity)

    def get_mapping_id(self, mapping_ref: MappingRef) -> int:
        """Genereer een stabiele hash-ID voor een mapping.

        Maakt een stabiele hashwaarde op basis van het gecombineerde RETW-bestandspad en de mappingcode.

        Args:
            mapping_ref (MappingRef): Een namedtuple met het RETW-bestandspad en de mappingcode.

        Retourneert:
            int: De stabiele hash-ID voor de mapping.
        """
        file_RETW, code_mapping = mapping_ref
        id_file = self.get_file_id(file=file_RETW)
        return self._stable_hash(key=str(id_file) + code_mapping)

    def _add_model_entities(self, file_RETW: str, dict_RETW: list) -> None:
        """Voegt model entiteiten toe aan de graaf.

        Extraheert de entiteiten uit het document model in de RETW dictionary en voegt zet als knopen aan graaf toe.
        Also adds edges between the file and its entities.

        Args:
            file_RETW (str): RETW bestandspad
            dict_RETW (list): Dictionary met RETW data.

        Returns:
            None
        """
        # Determine document model
        model = [
            model for model in dict_RETW["Models"] if model["IsDocumentModel"] is True
        ][0]
        if "Entities" not in model:
            logger.warning(f"No entities for a document model in '{file_RETW}'")
            return

        for entity in model["Entities"]:
            id_entity = self.get_entity_id(EntityRef(model["Code"], entity["Code"]))
            dict_entity = {
                id_entity: {
                    "name": id_entity,
                    "type": VertexType.ENTITY.name,
                    "Id": entity["Id"],
                    "Name": entity["Name"],
                    "Code": entity["Code"],
                    "IdModel": model["Id"],
                    "NameModel": model["Name"],
                    "CodeModel": model["Code"],
                    "CreationDate": entity["CreationDate"],
                    "Creator": entity["Creator"],
                    "ModificationDate": entity["ModificationDate"],
                    "Modifier": entity["Modifier"],
                }
            }
            self.entities.update(dict_entity)
            edge_entity_file = {
                "source": self.get_file_id(file=file_RETW),
                "target": id_entity,
                "type": EdgeType.FILE_ENTITY.name,
            }
            self.edges.append(edge_entity_file)

    def _add_mappings(self, file_RETW: str, mappings: dict) -> None:
        """Add mappings to the graph.

        Processes each mapping extracted from the RETW dictionary, adds them as nodes to the graph,
        and establishes edges between the file and its mappings, as well as between mappings and their
        source and target entities.

        Args:
            file_RETW (str): RETW file path.
            mappings (dict): Dictionary containing mapping data.

        Returns:
            None
        """
        for mapping_RETW in mappings:
            id_mapping = self.get_mapping_id(
                MappingRef(file_RETW, mapping_RETW["Code"])
            )
            mapping = {
                id_mapping: {
                    "name": id_mapping,
                    "type": VertexType.MAPPING.name,
                    "Id": mapping_RETW["Id"],
                    "Name": mapping_RETW["Name"],
                    "Code": mapping_RETW["Code"],
                    "CreationDate": mapping_RETW["CreationDate"],
                    "Creator": mapping_RETW["Creator"],
                    "ModificationDate": mapping_RETW["ModificationDate"],
                    "Modifier": mapping_RETW["Modifier"],
                }
            }
            self.mappings.update(mapping)
            edge_mapping_file = {
                "source": self.get_file_id(file=file_RETW),
                "target": id_mapping,
                "type": EdgeType.FILE_MAPPING.name,
                "CreationDate": mapping_RETW["CreationDate"],
                "Creator": mapping_RETW["Creator"],
                "ModificationDate": mapping_RETW["ModificationDate"],
                "Modifier": mapping_RETW["Modifier"],
            }
            self.edges.append(edge_mapping_file)
            self._add_mapping_sources(id_mapping=id_mapping, mapping=mapping_RETW)
            self._add_mapping_target(id_mapping=id_mapping, mapping=mapping_RETW)

    def _add_mapping_sources(self, id_mapping: int, mapping: dict) -> None:
        """Add mapping source entities to the graph.

        Iterates through the source composition of a mapping, extracts source entities,
        and adds them as nodes to the graph. Also adds edges between the mapping and its source entities.

        Args:
            id_mapping (int): Unique identifier of the mapping.
            mapping (dict): Dictionary containing mapping data.

        Returns:
            None
        """
        has_source_compositions = "SourceComposition" in mapping
        if has_source_compositions:
            has_source_compositions = len(mapping["SourceComposition"]) > 0
        if not has_source_compositions:
            logger.error(f"No source entities for mapping '{mapping['Name']}'")
            return
        for source in mapping["SourceComposition"]:
            source_entity = source["Entity"]
            if (
                "Stereotype" in source_entity
                and source_entity["Stereotype"] == "mdde_FilterBusinessRule"
            ):
                continue
            id_entity = self.get_entity_id(
                EntityRef(source_entity["CodeModel"], source_entity["Code"])
            )
            entity = {
                id_entity: {
                    "name": id_entity,
                    "type": VertexType.ENTITY.name,
                    "Id": source_entity["Id"],
                    "Name": source_entity["Name"],
                    "Code": source_entity["Code"],
                    "CodeModel": source_entity["CodeModel"],
                }
            }
            if id_entity not in self.entities:
                self.entities.update(entity)
            edge_entity_mapping = {
                "source": id_entity,
                "target": id_mapping,
                "type": EdgeType.ENTITY_SOURCE.name,
            }
            self.edges.append(edge_entity_mapping)

    def _add_mapping_target(self, id_mapping: int, mapping: dict) -> None:
        """Add mapping target entity to the graph.

        Extracts the target entity of a mapping, adds it as a node to the graph,
        and creates an edge between the mapping and its target entity.

        Args:
            id_mapping (int): Unique identifier of the mapping.
            mapping (dict): Dictionary containing mapping data.

        Returns:
            None
        """
        if "EntityTarget" not in mapping:
            logger.error(f"No target entity for mapping '{mapping['Name']}'")
            return
        target_entity = mapping["EntityTarget"]
        id_entity = self.get_entity_id(
            EntityRef(target_entity["CodeModel"], target_entity["Code"])
        )
        entity = {
            id_entity: {
                "name": id_entity,
                "type": VertexType.ENTITY.name,
                "Id": target_entity["Id"],
                "Name": target_entity["Name"],
                "Code": target_entity["Code"],
                "CodeModel": target_entity["CodeModel"],
            }
        }
        if id_entity not in self.entities:
            self.entities.update(entity)
        edge_entity_mapping = {
            "source": id_mapping,
            "target": id_entity,
            "type": EdgeType.ENTITY_TARGET.name,
        }
        self.edges.append(edge_entity_mapping)

    def get_dag_total(self) -> ig.Graph:
        """Genereert een graaf met alle mappings, entiteiten en RETW bestanden.

        Bouwt een igraph graaf met de verzamelde mappings, entiteiten en bestanden als knopen,
        en legt de verbindingen tussen de knopen aan.

        Returns:
            ig.Graph: De gegenereerde graaf.
        """
        logger.info("Building a graph for RETW files, entities and mappings")
        vertices = (
            list(self.mappings.values())
            + list(self.entities.values())
            + list(self.files_RETW.values())
        )
        edges = list(self.edges)
        graph = ig.Graph.DictList(vertices=vertices, edges=edges, directed=True)
        logger.info("Build graph total")
        return graph

    def get_dag_single_retw_file(self, file_retw: str) -> ig.Graph:
        """Genereert een subgraaf van een specifiek RETW bestand.

        Bouwt een totale graaf en haalt daar de subgraaf uit voor het specifiek RETW bestand.

        Args:
            file_retw (str): De naam van het RETW bestand.

        Returns:
            ig.Graph: De subgraaf voor de gespecificeerde RETW file.
        """

        logger.info(f"Creating a graph for the file, '{file_retw}'")
        dag = self.get_dag_total()
        vx_file = dag.vs.select(FileRETW_eq=file_retw)
        vx_file_graph = dag.subcomponent(vx_file[0], mode="out")
        vx_delete = [i for i in dag.vs.indices if i not in vx_file_graph]
        dag.delete_vertices(vx_delete)
        return dag

    def get_dag_file_dependencies(self, include_entities: bool = True) -> ig.Graph:
        """Genereert een graaf van de afhankelijkheden tussen RETW bestanden, gebaseerd op entiteit gebruik.

        Genereert een graaf die de afhankelijkheden tussen RETW bestanden expliciet maakt op gedeelde entiteiten.
        De graaf heeft bestanden als knopen en de afhankelijkheden zijn de links tussen de knopen. Optioneel kunnen de
        gedeelde entiteiten als knopen aan de diagram worden toegevoegd.

        Args:
            include_entities (bool, optioneel): Geeft aan of de entiteiten als knopen moeten worden opgenomen. Standaard is True.

        Returns:
            ig.Graph: Een graaf van de bestandsafhankelijkheden.
        """
        dag = self.get_dag_total()
        vs_files = dag.vs.select(type_eq=VertexType.FILE_RETW.name)
        dict_vertices = {}
        lst_edges = []

        for vx_file in vs_files:
            dict_vertices |= {vx_file["name"]: vx_file.attributes()}
            vs_mappings = [
                vs
                for vs in dag.vs(dag.successors(vx_file))
                if vs["type"] == VertexType.MAPPING.name
            ]

            # Mappings can have source entities that are defined in another RETW file
            for vx_mapping in vs_mappings:
                # Get source entities
                vs_first_order = dag.vs(dag.neighborhood(vx_mapping, mode="in"))
                vs_source_entities = [
                    vx for vx in vs_first_order if vx["type"] == VertexType.ENTITY.name
                ]

                # Find RETW files connected to source entities other than the file in vx_file
                for vx_source_entity in vs_source_entities:
                    vx_file_source = [
                        vx
                        for vx in dag.vs(
                            dag.neighborhood(vx_source_entity.index, mode="in")
                        )
                        if vx["type"] == VertexType.FILE_RETW.name
                        and vx["name"] != vx_file["name"]
                    ]
                    if not vx_file_source:
                        continue

                    vx_file_source = vx_file_source[0]
                    dict_vertices |= {
                        vx_file_source["name"]: vx_file_source.attributes()
                    }

                    if include_entities:
                        dict_vertices |= {
                            vx_source_entity["name"]: vx_source_entity.attributes()
                        }
                        lst_edges.extend(
                            (
                                {
                                    "source": vx_file_source["name"],
                                    "target": vx_source_entity["name"],
                                },
                                {
                                    "source": vx_source_entity["name"],
                                    "target": vx_file["name"],
                                },
                            )
                        )
                    else:
                        # Make connection between files
                        lst_edges.append(
                            {
                                "source": vx_file_source["name"],
                                "target": vx_file["name"],
                            }
                        )

        dag_dependencies = ig.Graph.DictList(
            vertices=list(dict_vertices.values()), edges=lst_edges, directed=True
        )
        return dag_dependencies

    def get_dag_entity(self, entity: EntityRef) -> ig.Graph:
        """Bouw een subgraaf voor een specifieke entiteit.

        Bouwt de volledige graaf en extraheert de subgraaf die gerelateerd is aan een specifieke entiteit,
        inclusief de inkomende en uitgaande verbindingen.

        Args:
            entity (EntityRef): Een namedtuple die een referentie naar de entiteit bevat.

        Retourneert:
            ig.Graph: De subgraaf voor de opgegeven entiteit.
        """
        dag = self.get_dag_total()
        # Extract graph for relevant entity
        id_entity = self.get_entity_id(entity)
        vx_entity = dag.vs.select(name=id_entity)[0]
        vs_entity_graph = dag.subcomponent(vx_entity, mode="in") + dag.subcomponent(
            vx_entity, mode="out"
        )
        vs_delete = [i for i in dag.vs.indices if i not in vs_entity_graph]
        dag.delete_vertices(vs_delete)
        return dag

    def _make_increasing_with_duplicates(self,lst: list) -> list:
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

    def _dag_ETL_run_order(self, dag: ig.Graph) -> ig.Graph:
        """Verrijk de ETL DAG met de volgorder waarmee de mappings uitgevoerd moeten worden.

        Args:
            dag (ig.Graph): ETL DAG met entiteiten en mappings

        Returns:
            ig.Graph: ETL DAG waar de knopen verrijkt worden met het attribuut 'run_level',
            entiteit knopen krijgen de waarde -1, omdat de executievolgorde niet van toepassing is op entiteiten.
        """
        # For each node calculate the number of mapping nodes before the current node
        lst_mapping_order = [
            sum(
                dag.vs[vs]["type"] == VertexType.MAPPING.name
                for vs in dag.subcomponent(dag.vs[i], mode="in")
            )
            - 1
            for i in range(dag.vcount())
        ]
        # Assign valid run order to mappings only
        lst_run_level = []
        lst_run_level.extend(
            run_level if role == VertexType.MAPPING.name else -1
            for run_level, role in zip(lst_mapping_order, dag.vs["type"])
        )
        lst_run_level = self._make_increasing_with_duplicates(lst=lst_run_level)
        dag.vs["run_level"] = lst_run_level
        dag = self._dag_ETL_run_level_stages(dag=dag)
        return dag

    def _dag_ETL_run_level_stages(self, dag: ig.Graph) -> ig.Graph:
        """Determine mapping stages for each run level

        Args:
            dag (ig.Graph): DAG describing the ETL

        Returns:
            ig.Graph: ETL stages for a level added in the mapping vertex attribute 'stage'
        """
        dict_level_runs = {}
        # All mapping nodes
        vs_mapping = dag.vs.select(type_eq=VertexType.MAPPING.name)

        # Determine run stages of mappings by run level
        run_levels = list({node["run_level"] for node in vs_mapping})
        for run_level in run_levels:
            # Find run_level mappings and corresponding source entities
            mapping_sources = [
                {"mapping": mapping["name"], "sources": dag.predecessors(mapping)}
                for mapping in vs_mapping.select(run_level_eq=run_level)
            ]
            # Create graph of mapping conflicts (mappings that draw on the same sources)
            graph_conflicts = self._dag_ETL_run_level_conflicts_graph(mapping_sources)
            # Determine unique sorting for conflicts
            order = graph_conflicts.vertex_coloring_greedy(method="colored_neighbors")
            # Apply them back to the DAG
            dict_level_runs |= dict(zip(graph_conflicts.vs["name"], order))
            for k, v in dict_level_runs.items():
                dag.vs.select(name=k)["run_level_stage"] = v
        return dag

    def _dag_ETL_run_level_conflicts_graph(self, mapping_sources: dict) -> ig.Graph:
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
                    qty_common = len(set(a["sources"]) & set(b["sources"]))
                    if qty_common > 0:
                        lst_edges.append(
                            {"source": a["mapping"], "target": b["mapping"]}
                        )
        graph_conflicts = ig.Graph.DictList(
            vertices=lst_vertices, edges=lst_edges, directed=False
        )
        return graph_conflicts

    def get_dag_ETL(self) -> ig.Graph:
        """Build the ETL DAG, showing the flow of data between entities and mappings.

        Constructs a directed acyclic graph (DAG) representing the ETL process,
        including mappings and entities as vertices, and their relationships as edges.
        The DAG is enriched with run order information and isolated entities are removed.

        Returns:
            ig.Graph: The ETL DAG.

        Raises:
            NoFlowError: If no mappings are found, indicating no ETL flow.
        """
        vertices = list(self.mappings.values()) + list(self.entities.values())
        edge_types = [EdgeType.ENTITY_SOURCE.name, EdgeType.ENTITY_TARGET.name]
        edges = [e for e in self.edges if e["type"] in edge_types]
        dag = ig.Graph.DictList(vertices=vertices, edges=edges, directed=True)
        dag = self._dag_ETL_run_order(dag=dag)

        # Delete entities without mappings
        vs_no_connections = []
        vs_no_connections.extend(
            vtx.index
            for vtx in dag.vs
            if dag.degree(vtx, mode="in") == 0 and dag.degree(vtx, mode="out") == 0
        )

        if vs_no_connections:
            dag.delete_vertices(vs_no_connections)
            if len(dag.vs) == 0:
                raise NoFlowError("No mappings, so no ETL flow")
        logger.info("Build graph mappings")
        return dag
