# Integrator

## Dag Implementatie

Dit bestand definieert de kernimplementatie voor het bouwen en beheren van een ETL (Extract, Transform, Load) Directed Acyclic Graph (DAG) binnen een data-integratiesysteem. Het breidt een basis-DAG-bouwer uit met ETL-specifieke logica, zoals het in kaart brengen van relaties, de uitvoeringsvolgorde en strategieën voor het voorkomen van deadlocks. Het bestand maakt gebruik van de `igraph`-bibliotheek voor graafbewerkingen en biedt mechanismen om mappings en entiteiten binnen de DAG te extraheren, sorteren en analyseren.

**Belangrijke componenten**
**DeadlockPrevention Enum**
Bevat een opsomming van strategieën voor het voorkomen van deadlocks (SOURCE of TARGET), die bepalen hoe de uitvoeringsvolgorde en conflicten in de DAG worden afgehandeld.

**DagImplementation Klasse**
De hoofdklasse, afgeleid van `DagBuilder`, bevat alle ETL-DAG-logica:

- **build_dag**: Breidt de basisconstructie van de DAG uit met ETL-specifieke verrijking van mappings.
- **_add_model_to_mapping**: Koppelt mapping-nodes aan hun bijbehorende modelmetadata door de DAG te doorlopen.
- **get_run_config**: Geeft een gesorteerde lijst van mapping-nodes terug, verrijkt met uitvoeringsvolgorde en modelinformatie, op basis van de gekozen deadlockpreventiestrategie.
- **_dag_ETL_run_order / _dag_run_level_stages**: Bepalen en wijzen uitvoeringsstadia toe aan mappings, zodat de juiste volgorde en parallelisatie worden gewaarborgd zonder deadlocks.
- **_dag_ETL_run_levels_conflicts_graph**: Bouwt een conflictgrafiek op om mappings te identificeren die niet parallel kunnen worden uitgevoerd vanwege gedeelde afhankelijkheden.
- **get_mappings / get_entities**: Hulpmethoden om alle mapping- of entiteit-nodes uit de DAG op te halen voor verdere verwerking of inspectie.

**Foutafhandeling**
Aangepaste uitzonderingen (`InvalidDeadlockPrevention`, `NoFlowError`) worden gebruikt om ongeldige configuraties en lege DAG's op een nette manier af te handelen.

**Logging**
Maakt gebruik van een logger om fouten en belangrijke gebeurtenissen te rapporteren, wat helpt bij debugging en monitoring.

**Rol binnen het grotere systeem**
Dit bestand is een kernonderdeel van de ETL-orkestratielaag en levert de logica om de ETL-DAG op te bouwen, te analyseren en uit te voeren. Het zorgt ervoor dat datatransformaties in de juiste volgorde worden uitgevoerd, met ondersteuning voor parallelle uitvoering en het vermijden van deadlocks. Daarmee is het een essentieel onderdeel voor betrouwbare en efficiënte data-integratieworkflows.

## Dag Builder

Dit bestand definieert de `DagBuilder`-klasse, die verantwoordelijk is voor het opbouwen en beheren van Directed Acyclic Graphs (DAG’s) die ETL-processen (Extract, Transform, Load) representeren op basis van informatie uit RETW-JSON-bestanden. De DAG’s modelleren de relaties en afhankelijkheden tussen bestanden, entiteiten en mappings, en maken analyse mogelijk van de uitvoeringsvolgorde, afhankelijkheden en subgrafen die relevant zijn voor specifieke bestanden of entiteiten.

Het bestand maakt gebruik van de `igraph`-bibliotheek voor graafbewerkingen en biedt een reeks methoden om de hoofd-DAG op te bouwen, te analyseren en daaruit subgrafen te extraheren. Ook worden ondersteunende enums, uitzonderingen en hulpfuncties gedefinieerd voor stabiele identificatie en foutafhandeling.

**Belangrijke componenten**
**Enums en Namedtuples**
- `VertexType`: Somt de knooptypes in de graaf op (ENTITY, MAPPING, FILE_RETW, ERROR).
- `EdgeType`: Somt de typen relaties op tussen knopen (bijv. tussen bestanden en entiteiten of mappings).
- `EntityRef`, `MappingRef`: Namedtuples voor het uniek identificeren van entiteiten en mappings.

**Uitzonderingen**
- `ErrorDagNotBuilt`: Wordt gegenereerd als graafbewerkingen worden uitgevoerd voordat de DAG is opgebouwd.
- `NoFlowError`: Wordt gegenereerd als er geen geldige stroom in de DAG is.

**DagBuilder Klasse**
**Doel**
Centrale klasse voor:
- Het laden van RETW-bestanden en extraheren van entiteiten en mappings.
- Het bouwen van een volledige DAG die het ETL-proces representeert.
- Het aanbieden van methoden om afhankelijkheden en uitvoeringsvolgorde te analyseren, extraheren en visualiseren.

**Belangrijkste methoden**
- `build_dag(files_RETW)`: Hoofdmethode om de DAG op te bouwen uit één of meerdere RETW-bestanden.
- `_add_RETW_file / _add_RETW_files`: Laden en parsen van RETW-bestanden, extractie van modellen, entiteiten en mappings.
- `_add_model_entities / _add_mappings`: Toevoegen van entiteiten en mappings als knopen, en creëren van randen die hun relaties weergeven.
- `_add_mapping_sources / _add_mapping_target`: Verwerken van de verbindingen tussen mappings en hun bron- of doelelementen.
- `get_file_id / get_entity_id / get_mapping_id`: Genereert stabiele, unieke ID’s voor bestanden, entiteiten en mappings met behulp van MD5-hashes.
- `_add_dag_statistics`: Annoteren van knopen met run-levels en ETL-levels om de uitvoeringsvolgorde en verwerkingsfasen aan te geven.
- `get_dag_total`: Haalt de volledige DAG op.
- `get_dag_single_retw_file`: Extraheert een subgraaf voor een specifiek RETW-bestand.
- `get_dag_file_dependencies`: Bouwt een graaf van afhankelijkheden tussen RETW-bestanden, optioneel inclusief entiteiten.
- `get_dag_of_entity`: Extraheert een subgraaf die relevant is voor een specifieke entiteit.
- `get_dag_ETL`: Levert een subgraaf met alleen entiteiten en mappings (zonder bestanden).
- `get_dag_mappings`: Bouwt een graaf van mappings en hun onderlinge afhankelijkheden.

**Patronen en ontwerpprincipes**
- **Graafconstructie**: Maakt gebruik van `igraph.DictList` om grafen op te bouwen vanuit dictionaries van knopen en randen.
- **Stabiele hashing**: Zorgt voor consistente knoopidentificatie over meerdere runs en bestanden.
- **Subgraafextractie**: Biedt meerdere methoden om gerichte deelgrafen te extraheren voor gefocuste analyse.
- **Statistiekenannotatie**: Berekent en kent run- en ETL-niveaus toe aan knopen voor planningsdoeleinden.

**Logging**
Gebruikt een logger (`logtools.get_logger`) voor informatieve meldingen en foutmeldingen gedurende het hele proces.

**Samenvatting**
Dit bestand vormt een essentieel onderdeel van de analyse en orkestratie van ETL-pijplijnen. Het maakt de opbouw, inspectie en manipulatie van DAG’s mogelijk die de gegevensstroom en afhankelijkheden van transformaties modelleren zoals beschreven in RETW-bestanden. Het abstraheert de complexiteit van graafbeheer en biedt een rijke API voor andere tools of ontwikkelaars om ETL-processen te bevragen en te visualiseren.

## DAG Rapporten

**Overzicht**
Dit bestand, `dag_reporting.py`, breidt de functionaliteit van een bestaande Directed Acyclic Graph (DAG)-implementatie voor ETL-processen (Extract, Transform, Load) uit met geavanceerde rapportage- en visualisatiemogelijkheden. Bovenop de basis-DAG-logica voegt het methoden toe voor het genereren van interactieve HTML-visualisaties van verschillende aspecten van de DAG (zoals bestandsafhankelijkheden, entiteitstrajecten en ETL-flows), met behulp van bibliotheken zoals `igraph`, `networkx` en `pyvis`. De klasse bevat ook hulpmethoden voor het formatteren, inkleuren en annoteren van knopen om de leesbaarheid en bruikbaarheid van de visualisaties te verbeteren.

Dit bestand vormt een belangrijk onderdeel van de introspectie- en documentatietooling binnen het ETL-systeem, en stelt gebruikers en ontwikkelaars in staat om datastromen, afhankelijkheden en mogelijke knelpunten in de ETL-pijplijn inzichtelijk te maken.

**Belangrijke componenten**
**Klassen en Enums**
- **ObjectPosition (Enum)**: Categoriseert knopen in de DAG als `START`, `INTERMEDIATE`, `END` of `UNDETERMINED` op basis van hun verbindingen. Wordt gebruikt voor analyse en visualisatie.
- **DagReporting (Klasse)**: De hoofdklasse, afgeleid van `DagImplementation`, biedt alle rapportage- en visualisatiefuncties. Beheert de weergave van knopen, grafiekconversie en diverse plotfuncties.

**Visualisatie- en formatteermethoden**
- **_set_visual_attributes**: Wijs vormen, kleuren en tooltips toe aan knopen op basis van hun type en kenmerken, als voorbereiding op visualisatie.
- **_set_node_tooltip**: Genereert gedetailleerde tooltips voor knopen, inclusief metadata zoals naam, code, model en wijzigingsinformatie.
- **_dag_node_hierarchy_level / _calculate_node_levels / _set_max_end_node_level**: Bepalen en toewijzen van hiërarchische niveaus aan knopen voor een overzichtelijke layout.
- **_dag_node_position_category**: Classificeert de positie van knopen (start, tussenliggend, eind) voor analyse en kleurcodering.
- **_dag_etl_coloring**: Past kleurenschema’s toe op knopen op basis van hun type en model, ter bevordering van de interpretatie.

**Grafiekconversie en output**
- **_igraph_to_networkx**: Converteert een `igraph`-graaf naar een `networkx`-DiGraph, zodat deze compatibel is met `pyvis` voor visualisatie.
- **plot_graph_html**: Genereert en slaat een interactieve HTML-visualisatie van een graaf op, met hiërarchische layout en navigatiemogelijkheden.

**Visualisatie op hoog niveau**
- **plot_graph_total**: Visualiseert de volledige DAG, inclusief alle bestanden, entiteiten en mappings.
- **plot_graph_retw_file**: Visualiseert de subgraaf die hoort bij een specifiek RETW-bestand.
- **plot_file_dependencies**: Visualiseert afhankelijkheden tussen RETW-bestanden, optioneel inclusief entiteiten.
- **plot_entity_journey**: Visualiseert alle afhankelijkheden van een specifieke entiteit, met visuele nadruk op die entiteit.
- **plot_etl_dag**: Visualiseert de ETL-flow-DAG, inclusief foutafhandeling wanneer er geen geldige flow aanwezig is.

**Analysehulpmiddelen**
- **get_entities_without_definition**: Geeft een lijst van entiteiten terug die geen definitie hebben in een van de RETW-bestanden — handig om incomplete of verweesde entiteiten op te sporen.
- **_format_etl_dag**: Regisseert de opmaak van de ETL-DAG voor visualisatie, door niveaus, hiërarchie, visuele attributen en kleurcodering toe te passen.

**Samenvatting**
Dit bestand is verantwoordelijk voor het omzetten van ruwe DAG-data in rijke, interactieve visualisaties en rapporten, waardoor ETL-processen inzichtelijker worden en eenvoudiger zijn te documenteren of debuggen. Het vormt de brug tussen de onderliggende DAG-logica en de inzichten voor eindgebruikers.