import xml.etree.ElementTree as ET
from pathlib import Path

class SqlProjEditor:
    """Biedt functionaliteit om SQL Server .sqlproj-bestanden te bewerken.

    Hiermee kunnen build-bestanden en post-deploy scripts aan een SQL projectbestand worden toegevoegd en het projectbestand worden opgeslagen.
    """

    def __init__(self, path_sqlproj):
        """Initialiseert de SqlProjEditor met het opgegeven .sqlproj-bestand.

        Laadt het projectbestand en bereidt het voor op bewerkingen.

        Args:
            path_sqlproj (str): Pad naar het SQL Server projectbestand (.sqlproj).
        """
        self.path_sqlproj = Path(path_sqlproj)
        self.tree = ET.parse(self.path_sqlproj)
        self.root = self.tree.getroot()
        self.ns = {'msbuild': 'http://schemas.microsoft.com/developer/msbuild/2003'}

    def _find_or_create_itemgroup(self, condition=None) -> ET.Element:
        """Zoekt een bestaande ItemGroup of maakt er een aan indien nodig.

        Doorzoekt het projectbestand naar een ItemGroup met de opgegeven conditie of maakt een nieuwe aan als deze niet bestaat.

        Args:
            condition (str, optional): De conditie waarop gezocht wordt in ItemGroups.

        Returns:
            Element: Het gevonden of nieuw aangemaakte ItemGroup-element.
        """
        for item_group in self.root.findall('msbuild:ItemGroup', self.ns):
            if condition is None or item_group.get('Condition') == condition:
                return item_group
        return ET.SubElement(self.root, 'ItemGroup')

    def add_build_file(self, filepath: str) -> None:
        # sourcery skip: class-extract-method
        """Voegt een build-bestand toe aan het SQL projectbestand.

        Maakt een Build-element aan voor het opgegeven bestand en voegt dit toe aan het projectbestand.

        Args:
            filepath (str): Pad naar het build-bestand dat moet worden toegevoegd.

        Returns:
            None
        """
        item_group = self._find_or_create_itemgroup()
        compile_elem = ET.SubElement(item_group, 'Build')
        compile_elem.set('Include', filepath)

    def add_postdeploy_script(self, path_file_post_deploy: str) -> None:
        """Voegt een post-deploy script toe aan het SQL projectbestand.

        Maakt een None-element aan voor het opgegeven post-deploy scriptbestand en stelt de benodigde eigenschappen in.

        Args:
            path_file_post_deploy (str): Pad naar het post-deploy scriptbestand dat moet worden toegevoegd.

        Returns:
            None
        """
        item_group = self._find_or_create_itemgroup()
        elem_none = ET.SubElement(item_group, 'None')
        elem_none.set('Include', path_file_post_deploy)
        elem_dep = ET.SubElement(elem_none, 'SubType')
        elem_dep.text = 'Designer'
        elem_post = ET.SubElement(elem_none, 'CopyToOutputDirectory')
        elem_post.text = 'PreserveNewest'

    def save(self, backup=True) -> None:
        """Slaat het gewijzigde SQL projectbestand op.

        Maakt optioneel een back-up van het bestaande projectbestand voordat het nieuwe bestand wordt weggeschreven.

        Args:
            backup (bool, optional): Of er een back-up van het projectbestand moet worden gemaakt. Standaard True.

        Returns:
            None
        """
        if backup:
            self.path_sqlproj.rename(self.path_sqlproj.with_suffix('.sqlproj.bak'))
        self.tree.write(self.path_sqlproj, encoding='utf-8', xml_declaration=True)
