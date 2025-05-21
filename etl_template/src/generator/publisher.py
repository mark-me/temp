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

    def publish(self):
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
        logger.info("--> Starting MDDE Publisher <--")
        # Opening JSON file
        file_created_ddls = Path(
            str(self.params.dir_generate)
            + "/"
            + self.params.generator_config.created_ddls_json
        )
        with open(file_created_ddls) as json_file:
            created_ddls = json.load(json_file)

        vs_project_file = Path(
            str(self.params.dir_repository)
            + "/"
            + self.params.publisher_config.vs_project_file
        )
        xml = codecs.open(vs_project_file, "r", "utf-8-sig")
        org_xml = xml.read()
        dict_xml = xmltodict.parse(org_xml, process_namespaces=False)
        xml.close()
        # Remove <VisualStudioVersion Condition="'$(SSDTExists)' == ''">11.0</VisualStudioVersion> due to error in loading project in VS
        for PropertyGroup in dict_xml["Project"]["PropertyGroup"]:
            if "VisualStudioVersion" in PropertyGroup:
                for VisualStudioVersion in PropertyGroup["VisualStudioVersion"]:
                    # TODO: After initial run, wrong line is removed, but than type changes to string.. Code review to check if best solution.
                    if not isinstance(VisualStudioVersion, str):
                        if "SSDTExists" in VisualStudioVersion["@Condition"]:
                            PropertyGroup["VisualStudioVersion"].remove(
                                VisualStudioVersion
                            )
                            break

        # Add new Folders:
        for ItemGroup in dict_xml["Project"]["ItemGroup"]:
            if "Folder" in ItemGroup:
                lst_existingFolders = []
                for include in ItemGroup["Folder"]:
                    lst_existingFolders.append(include["@Include"])
                # create a list with items not already in the vs project file
                lst_addFolders = set(created_ddls["Folder Include"]) - set(
                    lst_existingFolders
                )
                for i in lst_addFolders:
                    ItemGroup["Folder"].append({"@Include": i})
                    logger.info(f"Added folder to VS SQL Project file:  {i}")

        # Add new Files:
        for ItemGroup in dict_xml["Project"]["ItemGroup"]:
            if "Build" in ItemGroup:
                lst_existingFiles = []
                for include in ItemGroup["Build"]:
                    lst_existingFiles.append(include["@Include"])
                # create a list with items not already in the vs project file
                lst_addFiles = set(created_ddls["Build Include"]) - set(
                    lst_existingFiles
                )
                for i in lst_addFiles:
                    ItemGroup["Build"].append({"@Include": i})
                    logger.info(f"Added file to VS SQL Project file:  {i}")

        # Add new Post Deploy Scripts:
        for ItemGroup in dict_xml["Project"]["ItemGroup"]:
            if "None" in ItemGroup and "Build" in ItemGroup:
                lst_existingDeployScripts = []
                for include in ItemGroup["None"]:
                    lst_existingDeployScripts.append(include["@Include"])
                # create a list with items not already in the vs project file
                lst_addDeployScripts = set(created_ddls["None Include"]) - set(
                    lst_existingDeployScripts
                )
                for i in lst_addDeployScripts:
                    ItemGroup["None"].append(
                        {"@Include": i, "CopyToOutputDirectory": "PreserveNewest"}
                    )
                    logger.info(
                        f"Added Post Deploy Scripts to VS SQL Project file:  {i}"
                    )

        out = xmltodict.unparse(dict_xml, pretty=True, short_empty_elements=False)
        with open(vs_project_file, "wb") as file:
            file.write(out.encode("utf-8"))
        # Create a copy of the project file to the intermediate folder
        cp_dir = Path(str(self.params.dir_generate) + "/")
        cp_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(
            vs_project_file,
            Path(str(cp_dir) + "/" + self.params.publisher_config.vs_project_file),
        )


# Run Current Class
if __name__ == "__main__":
    print("Done")
