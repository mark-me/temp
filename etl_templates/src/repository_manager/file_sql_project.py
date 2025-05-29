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
    def __init__(self, path_sqlproj):
        """
        Initialiseert de SqlProjEditor met het opgegeven .sqlproj-bestand.

        Laadt het projectbestand, stelt het projectdirectory in en initialiseert de XML-parser en namespace mapping.

        Args:
            path_sqlproj (str or Path): Pad naar het SQL Server projectbestand (.sqlproj).
        """
        self.path_sqlproj = Path(path_sqlproj)
        self.project_dir = self.path_sqlproj.parent

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
            path = elem.get("Include").replace("\\", "/")
            includes.add(path)
        return includes

    def remove_missing_files(self) -> None:
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
            full_path = self.project_dir / include_path
            if not full_path.exists():
                parent = elem.getparent()
                parent.remove(elem)
                qty_removed += 1
        logger.info(f"{qty_removed} verwijzingen naar niet-bestaande bestanden verwijderd.")

    def add_new_files(self, folder: Path, item_type: str) -> None:
        """
        Voegt nieuwe .sql-bestanden uit een opgegeven map toe aan het SQL projectbestand.

        Doorzoekt de map recursief naar .sql-bestanden, controleert of ze al in het projectbestand staan en voegt ze toe aan de juiste sectie.

        Args:
            folder (Path): De map waarin gezocht wordt naar nieuwe .sql-bestanden.
            item_type (str): Het type itemgroep waarin de bestanden moeten worden toegevoegd (bijv. 'Build' of 'None').

        Returns:
            None
        """
        folder = Path(folder)
        if not folder.is_dir():
            print(f"[WARN] Map bestaat niet: {folder}")
            return

        item_group = self._find_or_create_itemgroup()
        existing = self._collect_existing_includes()
        qty_added = 0

        for file in folder.rglob("*.sql"):
            relative_path = file.relative_to(self.project_dir).as_posix()
            if relative_path in existing:
                continue

            el = etree.SubElement(item_group, f"{self.nsmap['msbuild']}{item_type}")
            el.set("Include", relative_path)

            if item_type == "None":
                subtype = etree.SubElement(el, f"{{{self.nsmap['msbuild']}}}SubType")
                subtype.text = "Designer"
                copy = etree.SubElement(
                    el, f"{{{self.nsmap['msbuild']}}}CopyToOutputDirectory"
                )
                copy.text = "PreserveNewest"

            qty_added += 1

        logger.info(
            f"{qty_added} nieuwe bestanden toegevoegd aan {item_type}-sectie uit {folder}"
        )

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
            self.path_sqlproj.replace(backup_path)
            logger.info(f"Backup gemaakt: {backup_path}")
        self.tree.write(
            str(self.path_sqlproj),
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=True,
        )
        logger.info(f"SQL project wijzigingen opgeslagen in {self.path_sqlproj}")
