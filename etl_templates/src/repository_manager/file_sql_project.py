import shutil
from pathlib import Path

from logtools import get_logger
from lxml import etree

logger = get_logger(__name__)


class SqlProjEditor:
    """
    Biedt functionaliteit om SQL Server .sqlproj-bestanden te bewerken.

    Hiermee kunnen build-bestanden en post-deploy scripts aan een SQL projectbestand worden toegevoegd,
    verwijzingen naar ontbrekende bestanden worden verwijderd en het projectbestand worden opgeslagen.
    """

    def __init__(self, path_sqlproj: Path):
        """
        Initialiseert de SqlProjEditor met het opgegeven .sqlproj-bestand.

        Laadt het projectbestand, stelt het projectdirectory in en initialiseert de XML-parser en namespace mapping.

        Args:
            path_sqlproj (Path): Pad naar het SQL Server projectbestand (.sqlproj).
        """
        self.path_sqlproj = path_sqlproj
        self.path_project = self.path_sqlproj.parent

        parser = etree.XMLParser(remove_blank_text=True)
        self.tree = etree.parse(str(self.path_sqlproj), parser)
        self.root = self.tree.getroot()

        # Namespace mapping (MSBuild-projecten gebruiken deze namespace)
        self.nsmap = {"msbuild": "http://schemas.microsoft.com/developer/msbuild/2003"}

    def _find_or_create_itemgroup(self):
        """
        Zoekt naar een bestaande ItemGroup in het SQL projectbestand of maakt er een aan als deze niet bestaat.

        Doorzoekt het projectbestand naar een ItemGroup-element en retourneert het eerste gevonden element, of maakt een nieuwe aan indien geen aanwezig is.

        Returns:
            Element: Het gevonden of nieuw aangemaakte ItemGroup-element.
        """
        item_groups = self.root.xpath("//msbuild:ItemGroup", namespaces=self.nsmap)
        if item_groups:
            return item_groups[0]
        return etree.SubElement(self.root, "{%s}ItemGroup" % self.nsmap["msbuild"])

    def _collect_existing_includes(self) -> set:
        """
        Verzamelt alle bestaande 'Include'-paden uit het SQL projectbestand.

        Doorloopt alle elementen met een 'Include'-attribuut en voegt hun paden toe aan een set, waarbij backslashes worden vervangen door slashes.

        Returns:
            set: Een set met alle bestaande 'Include'-paden in het projectbestand.
        """
        includes = set()
        for elem in self.root.xpath("//msbuild:*[@Include]", namespaces=self.nsmap):
            path = elem.get("Include").lower()
            includes.add(path)
        return includes

    def _remove_missing_files(self) -> None:
        """
        Verwijdert verwijzingen naar niet-bestaande bestanden uit het SQL projectbestand.

        Doorloopt alle elementen met een 'Include'-attribuut en verwijdert deze uit het
        projectbestand als het gekoppelde bestand niet meer bestaat.

        Returns:
            None
        """
        qty_removed = 0
        for elem in self.root.xpath("//msbuild:*[@Include]", namespaces=self.nsmap):
            include_path = elem.get("Include")
            # Normalize path for cross-platform compatibility
            normalized_include_path = Path(include_path.replace("\\", "/"))
            full_path = self.path_project / normalized_include_path
            if not full_path.exists():
                parent = elem.getparent()
                parent.remove(elem)
                qty_removed += 1
        logger.info(
            f"{qty_removed} verwijzingen naar niet-bestaande bestanden verwijderd."
        )

    def add_new_files(self, folder: Path) -> None:
        """
        Voegt nieuwe .sql-bestanden uit een opgegeven map toe aan het SQL projectbestand.

        Doorzoekt de map recursief naar .sql-bestanden, controleert of ze al in het projectbestand staan en voegt ze toe aan de juiste sectie.

        Args:
            folder (Path): De map waarin gezocht wordt naar nieuwe .sql-bestanden.
            item_type (str): Het type item-groep waarin de bestanden moeten worden toegevoegd (bijv. 'Build' of 'None').

        Returns:
            None
        """
        folder = Path(folder)
        if not folder.is_dir():
            logger.warning(f"Map bestaat niet: {folder}")
            return

        item_group = self._get_or_create_itemgroup_for_tag("Build")
        existing = self._collect_existing_includes()
        qty_added = 0

        for file in folder.rglob("*.sql"):
            relative_path = str(file.relative_to(folder).as_posix()).replace("/", "\\")
            item_type = (
                "None" if str(file.parent).lower() == "postdeployment" else "Build"
            )
            if relative_path.lower() in existing:
                continue

            element = etree.SubElement(
                item_group, f"{{{self.nsmap['msbuild']}}}{item_type}"
            )
            logger.info(f"Toegevoegd aan SQL project file '{relative_path}'")
            element.set("Include", relative_path)

            if item_type == "None":
                subtype = etree.SubElement(
                    element, f"{{{self.nsmap['msbuild']}}}SubType"
                )
                subtype.text = "Designer"
                copy = etree.SubElement(
                    element, f"{{{self.nsmap['msbuild']}}}CopyToOutputDirectory"
                )
                copy.text = "PreserveNewest"

            qty_added += 1

        self._add_missing_folders()

        logger.info(
            f"{qty_added} nieuwe bestanden toegevoegd aan {item_type}-sectie uit {folder}"
        )

    def _add_missing_folders(self):
        """
        Voegt ontbrekende folder-referenties toe aan het SQL projectbestand.

        Bepaalt welke folders in gebruik zijn op basis van de bestaande file-includes en voegt
        ontbrekende folder-referenties toe aan het projectbestand.
        """
        folder_elements = self.root.xpath("//msbuild:Folder", namespaces=self.nsmap)
        existing_folders = {
            elem.get("Include").replace("\\", "/") for elem in folder_elements
        }

        # Verzamel folder paths uit alle bestaande Include-attributen
        folders_in_use = set()
        item_group = self._get_or_create_itemgroup_for_tag("Build")
        for elem in item_group:
            path = elem.get("Include").replace("\\", "/")
            parts = Path(path).parts
            for i in range(1, len(parts)):
                folders_in_use.add("/".join(parts[:i]))

        # Alleen nieuwe folders toevoegen
        missing_folders = sorted(folders_in_use - existing_folders)
        if not missing_folders:
            logger.info("Geen nieuwe folders om toe te voegen.")
            return

        item_group = self._get_or_create_itemgroup_for_tag("Folder")
        for folder in missing_folders:
            el = etree.SubElement(item_group, f"{{{self.nsmap['msbuild']}}}Folder")
            el.set("Include", folder)
        logger.info(
            f"Aantal Folder Include-vermeldingen toegevoegd: {len(missing_folders)}."
        )

    def _get_or_create_itemgroup_for_tag(self, tag_name: str) -> etree.Element:
        """
        Zoekt of maakt een ItemGroup voor een specifiek tagtype in het SQL projectbestand.

        Doorzoekt het projectbestand naar een ItemGroup met het opgegeven tagtype en retourneert deze,
        of maakt een nieuwe ItemGroup aan als er geen gevonden wordt.

        Args:
            tag_name (str): De naam van het tagtype waarvoor een ItemGroup gezocht of aangemaakt moet worden.

        Returns:
            Element: Het gevonden of nieuw aangemaakte ItemGroup-element.
        """
        # Zoek een bestaande ItemGroup met elementen van het juiste type
        xpath_expr = f"//msbuild:ItemGroup[msbuild:{tag_name}]"
        groups = self.root.xpath(xpath_expr, namespaces=self.nsmap)
        if groups:
            return groups[0]
        # Geen gevonden: nieuwe groep maken
        return etree.SubElement(self.root, f"{{{self.nsmap['msbuild']}}}ItemGroup")

    def save(self, backup=True) -> None:
        """
        Slaat het gewijzigde SQL projectbestand op en maakt optioneel een back-up.

        Schrijft de huidige XML-boom naar het .sqlproj-bestand. Indien backup=True, wordt eerst een back-up van het projectbestand gemaakt.

        Args:
            backup (bool, optional): Of er een back-up van het projectbestand moet worden gemaakt. Standaard True.

        Returns:
            None
        """
        if backup:
            backup_path = self.path_sqlproj.with_suffix(".sqlproj.bak")
            shutil.copy2(self.path_sqlproj, backup_path)
            logger.info(f"Backup gemaakt: {backup_path}")
        self.tree.write(
            str(self.path_sqlproj),
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True,
        )
        logger.info(f"SQL project wijzigingen opgeslagen in {self.path_sqlproj}")
