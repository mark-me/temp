# ETL Azure Synapse Pipeline

![Deployment](images/etl.png){ align=right width="90" }

Deze documentatie beschrijft de ETL pipelines die gebruik maken van de standaard laadprocedures(zie [ETL Laad Procedures](MDDE_ETL_procs.md)). Het doel van deze pipelines is door middel van de standaard laadprocedures op een gestandaardiseerde manier data uit brontabellen te laden naar doeltabellen binnen dezelfde database en schema.

### Azure Synapse pipeline : MDDE/SQLPOOL2/PL_MDDE_Orchestration_Main

![Deployment](images/pl_mdde_orchestration_main.png){ width="360" }

Dit is de hoofdpijpline die de combinatie  runlevel en runlevelstage uitleest/sorteert uit de `Config`-tabel.
Voor elke unieke runlevel/runlevelstage combinatie wordt de subpipeline `PL_MDDE_Orchestration_Loop` aangeroepen (For Each mechanisme). De runlevel,runlevelstage en Loadrunid (van de pipeline) wordt  meegegeven.
Hierbij wordt wel gewacht totdat de subpipeline  voor een runlevel/runlevelstage combinatie afgerond is alvorens de volgende combinatie opgestart wordt. 


### Azure Synapse pipeline : MDDE/SQLPOOL2/PL_MDDE_Orchestration_Loop

![Deployment](images/pl_mdde_orchestration_loop.png){ width="360" } 

De configuratie wordt gelezen uit de `Config`-tabel voor de meegegeven runlevel en runlevelstage.
Op basis van de configuratie wordt er parallel sessies opgestart om  de  hoofd storeprocedure (`sp_LoadEntityData`) aan te roepen.


