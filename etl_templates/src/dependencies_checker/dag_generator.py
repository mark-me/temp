import json
import hashlib
from collections import namedtuple
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Union
from copy import deepcopy

import igraph as ig

from logtools import get_logger

logger = get_logger(__name__)

EntityRef = namedtuple("EntityRef", ("CodeModel", "CodeEntity"))
MappingRef = namedtuple("MappingRef", ("FileRETW", "CodeMapping"))


class VertexType(Enum):
    """Enumeratie van de typen knopen in de graaf.

    Biedt unieke identificaties voor elk type knoop in de graaf, waaronder entiteiten, mappings, RETW-bestanden en fouten.
    """

    ENTITY = auto()
    MAPPING = auto()
    FILE_RETW = auto()
    ERROR = auto()


class EdgeType(Enum):
    """Enumeratie van de typen randen in de graaf.

    Biedt unieke identificaties voor elk type rand in de graaf, waaronder koppelingen tussen bestanden en entiteiten, bestanden en mappings, bronentiteiten en mappings, en mappings en doeleenheden.
    """

    FILE_ENTITY = auto()
    FILE_MAPPING = auto()
    ENTITY_SOURCE = auto()
    ENTITY_TARGET = auto()

class ErrorDagNotBuilt(Exception):
    def __init__(self):
        self.message = "DAG nog niet opgebouwd"
        super().__init__(self.message)


class NoFlowError(Exception):
    def __init__(self, *args):
        self.message = "Geen flow in de DAG"
        super().__init__(self.message)


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
        self.files_RETW: dict = {}
        self.entities: dict = {}
        self.mappings: dict = {}
        self.edges: list = []
        self.dag: ig.Graph = None

    def build_dag(self, files_RETW: Union[str, list]):
        """Genereert een graaf met alle mappings, entiteiten en RETW bestanden.

        Bouwt een igraph graaf met de verzamelde mappings, entiteiten en bestanden als knopen,
        en legt de verbindingen tussen de knopen aan.

        Args:
            files_RETW (str|list): Enkel RETW bestandspad of lijst van RETW-bestandspaden met mappings.
        """
        if type(files_RETW) is list:
            self._add_RETW_files(files_RETW=files_RETW)
        elif type(files_RETW) is str:
            self._add_RETW_file(file_RETW=files_RETW)
        else:
            raise TypeError
        logger.info("Building a graph for RETW files, entities and mappings")
        vertices = (
            list(self.mappings.values())
            + list(self.entities.values())
            + list(self.files_RETW.values())
        )
        edges = list(self.edges)
        self.dag = ig.Graph.DictList(vertices=vertices, edges=edges, directed=True)

    def _add_RETW_files(self, files_RETW: list) -> bool:
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
            if not self._add_RETW_file(file_RETW=file_RETW):
                logger.error(f"Failed to add RETW file '{file_RETW}'")
                return False
        return True

    def _add_RETW_file(self, file_RETW: str) -> bool:
        """Laadt een RETW json bestand

        Args:
            file_RETW (str): RETW bestand met modellen en/of mappings

        Returns:
            bool: Geeft aan of alle RETW-bestand is verwerkt.
        """
        try:
            with open(file_RETW) as file:
                dict_RETW = json.load(file)
            logger.info(f"RETW bestand '{file_RETW}' toegevoegd")
        except FileNotFoundError:
            logger.error(f"Kon RETW bestand '{file_RETW}' niet vinden.")
            return False
        except json.JSONDecodeError:
            logger.error(f"Invalide JSON content in het RETW bestand '{file_RETW}'")
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

        logger.info(
            f"Voegt de entiteiten die zijn 'gedefinieerd' in het RETW bestand '{file_RETW}'"
        )
        self._add_model_entities(file_RETW=file_RETW, dict_RETW=dict_RETW)
        if "Mappings" in dict_RETW:
            logger.info(f"Mappings uit het RETW bestand '{file_RETW}' toevoegen")
            self._add_mappings(file_RETW=file_RETW, mappings=dict_RETW["Mappings"])
        else:
            logger.warning(f"Geen mappings in het RETW bestand '{file_RETW}'")
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
            entity.update(
                {
                    "name": id_entity,
                    "type": VertexType.ENTITY.name,
                    "IdModel": model["Id"],
                    "NameModel": model["Name"],
                    "CodeModel": model["Code"],
                }
            )
            dict_entity = {id_entity: entity}

            self.entities.update(dict_entity)
            edge_entity_file = {
                "source": self.get_file_id(file=file_RETW),
                "target": id_entity,
                "type": EdgeType.FILE_ENTITY.name,
            }
            self.edges.append(edge_entity_file)

    def _add_mappings(self, file_RETW: str, mappings: dict) -> None:
        """Voegt mappings toe aan de graaf en koppelt deze aan het RETW-bestand.

        Itereert over de opgegeven mappings, voegt elke mapping als knoop toe aan de graaf,
        en maakt verbindingen met het bijbehorende RETW-bestand, de bronentiteiten en de doeleenheid.

        Args:
            file_RETW (str): Het pad naar het RETW-bestand.
            mappings (dict): Een lijst van mapping dictionaries uit het RETW-bestand.

        Returns:
            None
        """
        for mapping_RETW in mappings:
            id_mapping = self.get_mapping_id(
                MappingRef(file_RETW, mapping_RETW["Code"])
            )
            mapping_RETW.update(
                {
                    "name": id_mapping,
                    "type": VertexType.MAPPING.name,
                }
            )
            mapping = {id_mapping: mapping_RETW}
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
        """Voegt bronentiteiten van een mapping toe aan de graaf.

        Extraheert de bronentiteiten uit de mapping, voegt deze als knopen toe aan de graaf,
        en maakt verbindingen tussen de bronentiteiten en de mapping.

        Args:
            id_mapping (int): Unieke identifier van de mapping.
            mapping (dict): Dictionary met mappinggegevens.

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
            source_entity.update(
                {
                    "name": id_entity,
                    "type": VertexType.ENTITY.name,
                }
            )
            entity = {id_entity: source_entity}
            if id_entity not in self.entities:
                self.entities.update(entity)
            edge_entity_mapping = {
                "source": id_entity,
                "target": id_mapping,
                "type": EdgeType.ENTITY_SOURCE.name,
            }
            self.edges.append(edge_entity_mapping)

    def _add_mapping_target(self, id_mapping: int, mapping: dict) -> None:
        """Voegt de doeleenheid van een mapping toe aan de graaf.

        Extraheert de doeleenheid uit de mapping, voegt deze als knoop toe aan de graaf,
        en maakt een verbinding tussen de mapping en de doeleenheid.

        Args:
            id_mapping (int): Unieke identifier van de mapping.
            mapping (dict): Dictionary met mappinggegevens.

        Returns:
            None
        """
        if "EntityTarget" not in mapping:
            logger.error(f"No target entity for mapping '{mapping['Name']}'")
            return
        entity_target = mapping["EntityTarget"]
        id_entity = self.get_entity_id(
            EntityRef(entity_target["CodeModel"], entity_target["Code"])
        )
        entity_target.update(
            {
                "name": id_entity,
                "type": VertexType.ENTITY.name,
            }
        )
        entity = {id_entity: entity_target}
        if id_entity not in self.entities:
            self.entities.update(entity)
        edge_entity_mapping = {
            "source": id_mapping,
            "target": id_entity,
            "type": EdgeType.ENTITY_TARGET.name,
        }
        self.edges.append(edge_entity_mapping)

    def _add_dag_statistics(self):
        self._stats_mapping_run_level()

    def _stats_mapping_run_level(self):
        """Bepaalt en wijst run-levels toe aan mappings in de graaf.

        Bereken voor elke mapping het aantal voorafgaande mappings en wijs een run-level toe op basis van afhankelijkheden.
        Dit helpt bij het bepalen van de uitvoeringsvolgorde van mappings in het ETL-proces.

        Returns:
            None
        """
        # For each node calculate the number of mapping nodes before the current node
        dag_mappings = self.get_dag_mappings()
        dag_mappings.vs["qty_preceding"] = [
            len(dag_mappings.subcomponent(dag_mappings.vs[i], mode="in")) - 1
            for i in range(dag_mappings.vcount())
        ]

        dag_mappings.vs.select(qty_preceding_eq=0)["run_level"] = 0
        # Set run_levels iterating from lowest preceding number of mappings to highest
        vs_processing = [
            vx
            for vx in dag_mappings.vs.select(run_level_eq=0)
            if len(dag_mappings.neighbors(vx, mode="out")) > 0
        ]

        for vx in vs_processing:
            vs_successors = dag_mappings.neighbors(vx, mode="out")
            dag_mappings.vs[vs_successors]["run_level"] = vx["run_level"] + 1
            vs_processing.extend(dag_mappings.vs[vs_successors])

        # Adding run levels to the DAG with mappings and entities
        for vx in dag_mappings.vs:
            self.dag.vs.select(name_eq=vx["name"])["run_level"] = vx["run_level"]

    def _stats_entity_level(self):
        vs_entities = self.dag.vs.select(type_eq=VertexType.ENTITY.name)
        # TODO: Finish function

    def get_dag_total(self) -> ig.Graph:
        """Geeft de volledige gegenereerde graaf (DAG) terug.

        Retourneert de volledige igraph graaf die alle mappings, entiteiten en RETW-bestanden bevat.
        Als de graaf nog niet is opgebouwd, wordt een foutmelding opgegooid.

        Returns:
            ig.Graph: De volledige gegenereerde graaf.

        Raises:
            ErrorDagNotBuilt: Als de graaf nog niet is opgebouwd.
        """
        if not self.dag:
            raise ErrorDagNotBuilt
        return self.dag

    def get_dag_single_retw_file(self, file_retw: str) -> ig.Graph:
        """Genereert een subgraaf voor een specifiek RETW-bestand.

        Bouwt een subgraaf die alleen de knopen en verbindingen bevat die gerelateerd zijn aan het opgegeven RETW-bestand.
        Dit is handig om de afhankelijkheden en structuur van een enkel bestand te analyseren.

        Args:
            file_retw (str): Het pad naar het RETW-bestand.

        Returns:
            ig.Graph: De subgraaf voor het opgegeven RETW-bestand.

        Raises:
            ErrorDagNotBuilt: Als de graaf nog niet is opgebouwd.
        """
        logger.info(f"Creating a graph for the file, '{file_retw}'")
        if not self.dag:
            raise ErrorDagNotBuilt
        dag = self.dag.copy()
        vx_file = dag.vs.select(FileRETW_eq=file_retw)
        vx_file_graph = dag.subcomponent(vx_file[0], mode="out")
        vx_delete = [i for i in dag.vs.indices if i not in vx_file_graph]
        dag.delete_vertices(vx_delete)
        return dag

    def get_dag_file_dependencies(self, include_entities: bool = True) -> ig.Graph:
        """Genereert een afhankelijkheidsgraaf tussen RETW-bestanden.

        Bouwt een graaf die de afhankelijkheden tussen RETW-bestanden toont, optioneel met entiteiten als tussenliggende knopen.
        Dit is nuttig om te visualiseren welke bestanden afhankelijk zijn van elkaar via gedeelde entiteiten en mappings.

        Args:
            include_entities (bool, optional): Of entiteiten als tussenliggende knopen moeten worden opgenomen. Standaard True.

        Returns:
            ig.Graph: De afhankelijkheidsgraaf tussen RETW-bestanden.

        Raises:
            ErrorDagNotBuilt: Als de graaf nog niet is opgebouwd.
        """
        if not self.dag:
            raise ErrorDagNotBuilt
        dag = self.dag.copy()

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

    def get_dag_of_entity(self, entity: EntityRef) -> ig.Graph:
        """Genereert een subgraaf voor een specifieke entiteit.

        Bouwt een subgraaf die alle knopen en verbindingen bevat die direct of indirect gerelateerd zijn aan de opgegeven entiteit.
        Dit is nuttig om de afhankelijkheden en het bereik van een enkele entiteit binnen de totale graaf te analyseren.

        Args:
            entity (EntityRef): De entiteit waarvoor de subgraaf wordt gegenereerd.

        Returns:
            ig.Graph: De subgraaf voor de opgegeven entiteit.

        Raises:
            ErrorDagNotBuilt: Als de graaf nog niet is opgebouwd.
        """
        if not self.dag:
            raise ErrorDagNotBuilt

        dag = deepcopy(self.dag)

        # Extract graph for relevant entity
        id_entity = self.get_entity_id(entity)
        vx_entity = dag.vs.select(name=id_entity)[0]
        vs_entity_graph = dag.subcomponent(vx_entity, mode="in") + dag.subcomponent(
            vx_entity, mode="out"
        )
        vs_delete = [i for i in dag.vs.indices if i not in vs_entity_graph]
        dag.delete_vertices(vs_delete)
        return dag

    def get_dag_ETL(self) -> ig.Graph:
        """Genereert een graaf van alleen entiteiten en mappings voor het ETL-proces.

        Bouwt een subgraaf waarin alle RETW-bestanden zijn verwijderd, zodat alleen de entiteiten en mappings overblijven.
        Dit is nuttig om het ETL-proces te analyseren zonder de bestandsstructuur.

        Returns:
            ig.Graph: De ETL-graaf zonder RETW-bestanden.

        Raises:
            ErrorDagNotBuilt: Als de graaf nog niet is opgebouwd.
        """
        if not self.dag:
            raise ErrorDagNotBuilt
        dag = deepcopy(self.dag)
        vs_files = dag.vs.select(type_eq=VertexType.FILE_RETW.name)
        dag.delete_vertices(vs_files)
        return dag

    def get_dag_mappings(self) -> ig.Graph:
        """Genereert een graaf van alleen mappings en hun onderlinge afhankelijkheden.

        Bouwt een subgraaf waarin alleen de mappings en hun afhankelijkheden via entiteiten zijn opgenomen.
        Dit is nuttig om te analyseren hoe mappings elkaar beïnvloeden binnen het ETL-proces.

        Returns:
            ig.Graph: De graaf met mappings en hun afhankelijkheden.

        Raises:
            ErrorDagNotBuilt: Als de graaf nog niet is opgebouwd.
        """
        if not self.dag:
            raise ErrorDagNotBuilt
        dag = deepcopy(self.dag)

        dict_vertices = {}
        lst_edges = []

        vs_mappings = dag.vs.select(type_eq=VertexType.MAPPING.name)
        for vx_mapping in vs_mappings:
            dict_vertices |= {vx_mapping["name"]: vx_mapping.attributes()}
            vs_entities_source = dag.vs(dag.neighbors(vx_mapping, mode="in"))

            # Getting mappings that filled source entities
            for vx_entity_source in vs_entities_source:
                if idx_mapping_input := dag.neighbors(vx_entity_source, mode="in"):
                    vs_mapping_input = dag.vs(idx_mapping_input)[0]
                    lst_edges.append(
                        {
                            "source": vs_mapping_input["name"],
                            "target": vx_mapping["name"],
                        }
                    )
                lst_edges

            # Get mappings that depend on target entities
            vx_entity_target = dag.vs(dag.neighbors(vx_mapping, mode="out"))[0]
            vs_mappings_target = dag.vs(dag.neighbors(vx_entity_target, mode="out"))
            lst_edges.extend(
                {
                    "source": vx_mapping["name"],
                    "target": vx_mapping_target["name"],
                }
                for vx_mapping_target in vs_mappings_target
            )
        lst_vertices = list(dict_vertices.values())
        dag_mappings = ig.Graph.DictList(
            vertices=lst_vertices, edges=lst_edges, directed=True
        )
        return dag_mappings
