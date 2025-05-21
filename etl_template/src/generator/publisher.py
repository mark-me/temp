import xmltodict
import codecs
import yaml
import json
from pathlib import Path
import shutil

from log_config import logging

logger = logging.getLogger(__name__)


class DDLPublisher:
    """Publish SQL files in VS Studio project and add them to the SQLProject file"""

    def __init__(self, params: dict):
        self.params = params
        self.dir_repo = self.params.dir_repository
        self.file_vs_project = self.params.publisher_config.vs_project_file
        self.vs_project_file = Path(f"{self.dir_repo}/{self.file_vs_project}")

    def publish(self):
        """
        Publiceert SQL-bestanden in het Visual Studio-project en voegt ze toe aan het SQLProject-bestand.

        Deze functie verwerkt de aangemaakte DDL's, voegt nieuwe mappen, bestanden en post-deploy scripts toe aan het projectbestand en slaat het resultaat op.

        Returns:
            None: De functie voert acties uit en slaat wijzigingen op, maar retourneert geen waarde.
        """
        logger.info("--> Starting MDDE Publisher <--")
        # Opening JSON file
        created_ddls = self._read_created_ddls()
        dict_xml = self._read_project_file()
        dict_xml = self._remove_vs_version_condition(dict_xml)
        # Add new Folders:
        lst_folders = created_ddls["Folder Include"]
        dict_xml = self._add_folders(dict_xml, lst_folders)
        # Add new Files:
        lst_files = created_ddls["Build Include"]
        dict_xml = self._add_files(dict_xml, lst_files)
        # Add new Post Deploy Scripts:
        lst_post_deploy = created_ddls["None Include"]
        dict_xml = self._add_post_deploy(dict_xml, lst_post_deploy)

        self._save_vs_project_file(dict_xml)
        # Create a copy of the project file to the intermediate folder
        cp_dir = Path(f"{self.params.dir_generate}/")
        cp_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(
            self.file_vs_project,
            Path(f"{cp_dir}/{self.file_vs_project}"),
        )

    def _save_vs_project_file(self, dict_xml):
        out = xmltodict.unparse(dict_xml, pretty=True, short_empty_elements=False)
        with open(self.file_vs_project, "wb") as file:
            file.write(out.encode("utf-8"))

    def _add_post_deploy(self, dict_xml: dict, lst_post_deploy: list) -> dict:
        """
        Voegt nieuwe post-deploy scripts toe aan het Visual Studio-projectbestand als deze nog niet aanwezig zijn.

        Deze functie zorgt ervoor dat alle opgegeven post-deploy scripts worden toegevoegd aan het projectbestand, zonder duplicaten.

        Args:
            dict_xml (dict): Het geparste XML-woordenboek van het projectbestand.
            lst_post_deploy (list): Een lijst met post-deploy scriptbestanden die moeten worden toegevoegd.

        Returns:
            dict: Het aangepaste XML-woordenboek met de toegevoegde post-deploy scripts.
        """
        for ItemGroup in dict_xml["Project"]["ItemGroup"]:
            if "None" in ItemGroup and "Build" in ItemGroup:
                lst_post_deploy_existing = []
                lst_post_deploy_existing.extend(
                    include["@Include"] for include in ItemGroup["None"]
                )
                # create a list with items not already in the vs project file
                lst_addDeployScripts = set(lst_post_deploy) - set(
                    lst_post_deploy_existing
                )
                for i in lst_addDeployScripts:
                    ItemGroup["None"].append(
                        {"@Include": i, "CopyToOutputDirectory": "PreserveNewest"}
                    )
                    logger.info(
                        f"Added Post Deploy Scripts to VS SQL Project file:  {i}"
                    )
        return dict_xml

    def _add_files(self, dict_xml: dict, lst_files: list) -> dict:
        """
        Voegt nieuwe bestanden toe aan het Visual Studio-projectbestand als deze nog niet aanwezig zijn.

        Deze functie zorgt ervoor dat alle opgegeven bestanden worden toegevoegd aan het projectbestand, zonder duplicaten.

        Args:
            dict_xml (dict): Het geparste XML-woordenboek van het projectbestand.
            lst_files (list): Een lijst met bestandsnamen die moeten worden toegevoegd.

        Returns:
            dict: Het aangepaste XML-woordenboek met de toegevoegde bestanden.
        """
        for ItemGroup in dict_xml["Project"]["ItemGroup"]:
            if "Build" in ItemGroup:
                lst_files_existing = []
                lst_files_existing.extend(
                    include["@Include"] for include in ItemGroup["Build"]
                )
                # create a list with items not already in the vs project file
                lst_addFiles = set(lst_files) - set(
                    lst_files_existing
                )
                for i in lst_addFiles:
                    ItemGroup["Build"].append({"@Include": i})
                    logger.info(f"Added file to VS SQL Project file:  {i}")
        return dict_xml

    def _add_folders(self, dict_xml: dict, lst_folders: list):
        """
        Voegt nieuwe mappen toe aan het Visual Studio-projectbestand als deze nog niet aanwezig zijn.

        Deze functie zorgt ervoor dat alle opgegeven mappen worden toegevoegd aan het projectbestand, zonder duplicaten.

        Args:
            dict_xml (dict): Het geparste XML-woordenboek van het projectbestand.
            lst_folders (list): Een lijst met mapnamen die moeten worden toegevoegd.

        Returns:
            dict: Het aangepaste XML-woordenboek met de toegevoegde mappen.
        """

        """
        Onderstaande code levert volgende foute code op:

            <PropertyGroup>
                <VisualStudioVersion Condition="'$(VisualStudioVersion)' == ''">11.0</VisualStudioVersion>
                <VisualStudioVersion Condition="'$(SSDTExists)' == ''">11.0</VisualStudioVersion>
                <SSDTExists Condition="Exists('$(MSBuildExtensionsPath)\Microsoft\VisualStudio\v$(VisualStudioVersion)\SSDT\Microsoft.Data.Tools.Schema.SqlTasks.targets')">True</SSDTExists>
            </PropertyGroup>


        Maar als wij dit handmatig nu aanpassen naar het volgende werkt hij goed. Dus uitzoeken hoe wij deze volgorde kunnen aanpassen!!

            <PropertyGroup>
                <VisualStudioVersion Condition="'$(VisualStudioVersion)' == ''">11.0</VisualStudioVersion>
                <SSDTExists Condition="Exists('$(MSBuildExtensionsPath)\Microsoft\VisualStudio\v$(VisualStudioVersion)\SSDT\Microsoft.Data.Tools.Schema.SqlTasks.targets')">True</SSDTExists>
                <VisualStudioVersion Condition="'$(SSDTExists)' == ''">11.0</VisualStudioVersion>
            </PropertyGroup>

        """
        for ItemGroup in dict_xml["Project"]["ItemGroup"]:
            if "Folder" in ItemGroup:
                lst_folders_existing = []
                lst_folders_existing.extend(
                    include["@Include"] for include in ItemGroup["Folder"]
                )
                # create a list with items not already in the vs project file
                lst_addFolders = set(lst_folders) - set(lst_folders_existing)
                for i in lst_addFolders:
                    ItemGroup["Folder"].append({"@Include": i})
                    logger.info(f"Added folder to VS SQL Project file:  {i}")
        return dict_xml

    def _read_created_ddls(self):
        """
        Leest het JSON-bestand met aangemaakte DDL's en retourneert de inhoud als een dictionary.

        Deze functie opent het opgegeven JSON-bestand en laadt de gegevens voor verdere verwerking.

        Returns:
            dict: De ingelezen DDL-informatie uit het JSON-bestand.
        """
        file_created_ddls = Path(
            f"{self.params.dir_generate}/{self.params.generator_config.created_ddls_json}"
        )
        with open(file_created_ddls) as json_file:
            created_ddls = json.load(json_file)
        return created_ddls

    def _remove_vs_version_condition(self, dict_xml: dict) -> dict:
        """
        Verwijdert het element VisualStudioVersion met een conditie die 'SSDTExists' bevat uit de XML-woordenboekstructuur van het project.

        Deze functie helpt fouten te voorkomen bij het laden van het project in Visual Studio door de correcte volgorde van elementen te waarborgen.

        Args:
            dict_xml (dict): Het geparste XML-woordenboek van het projectbestand.

        Returns:
            dict: Het aangepaste XML-woordenboek waarin het opgegeven VisualStudioVersion-element is verwijderd.
        """
        for PropertyGroup in dict_xml["Project"]["PropertyGroup"]:
            if "VisualStudioVersion" in PropertyGroup:
                for VisualStudioVersion in PropertyGroup["VisualStudioVersion"]:
                    # TODO: After initial run, wrong line is removed, but than type changes to string.. Code review to check if best solution.
                    if (
                        not isinstance(VisualStudioVersion, str)
                        and "SSDTExists" in VisualStudioVersion["@Condition"]
                    ):
                        PropertyGroup["VisualStudioVersion"].remove(VisualStudioVersion)
                        break
        return dict_xml

    def _read_project_file(self):
        """
        Leest en parst het Visual Studio-projectbestand naar een dictionary.

        Deze functie laadt het projectbestand als XML en zet het om naar een Python-dictionary voor verdere verwerking.

        Returns:
            dict: De geparste XML-inhoud van het projectbestand als een dictionary.
        """
        xml = codecs.open(self.vs_project_file, "r", "utf-8-sig")
        org_xml = xml.read()
        dict_xml = xmltodict.parse(org_xml, process_namespaces=False)
        xml.close()
        return dict_xml
