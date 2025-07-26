from datetime import datetime

from logtools import get_logger

logger = get_logger(__name__)


class ObjectTransformer:
    """Collectie van functions die structuren en data van Power Designer objecten kan transformeren

    Het transformeren van structuren wordt gedaan om het 'querien' van data voor de ETL en DDL te versimpelen
    """

    def __init__(self):
        self.__timestamp_fields = ["a:CreationDate", "a:ModificationDate"]

    def clean_keys(self, content: dict | list) -> dict | list:
        """Verwijdert voorlooptekens '@' en 'a:' uit alle sleutels in een dict of lijst van dicts.

        Deze functie normaliseert de sleutelnamen van Power Designer objecten zodat ze eenvoudiger te verwerken zijn in ETL-processen.
        Het resultaat is een dict of lijst van dicts met opgeschoonde sleutelnamen.

        Args:
            content (dict | list): Een dictionary of lijst van dictionaries met mogelijk geneste sleutels.

        Returns:
            dict | list: Dezelfde structuur als input, maar met opgeschoonde sleutelnamen.
        """
        lst_object = [content] if isinstance(content, dict) else content
        for i in range(len(lst_object)):
            attrs = [key for key in list(lst_object[i].keys()) if key[:1] == "@"]
            for attr in attrs:
                lst_object[i][attr[1:]] = lst_object[i].pop(attr)
            attrs = [key for key in list(lst_object[i].keys()) if key[:2] == "a:"]
            for attr in attrs:
                lst_object[i][attr[2:]] = lst_object[i].pop(attr)

        result = lst_object[0] if isinstance(content, dict) else lst_object
        return result

    def _convert_values_datetime(self, d: dict, convert_key: str) -> dict:
        """Converteert alle (geneste) dictionary records met een specifieke waarde van de naam die een Unix timestamp bevatten naar een datetime object

        Args:
            d (dict): Dictionary die de timestamp waarde bevat
            remove_key (str): De naam van de sleutels die een timestamp waarde bevatten

        Returns:
            dict: De dictionary zonder de sleutels
        """
        if isinstance(d, dict):
            logger.debug("object is dictionary; file:pd_transform_object; object:d")
            for key in list(d.keys()):
                if key == convert_key:
                    d[key] = datetime.fromtimestamp(int(d[key]))
                else:
                    self._convert_values_datetime(d[key], convert_key)
            return d
        elif isinstance(d, list):
            logger.debug("object is list; file:pd_transform_object; object:d")
            for i in range(len(d)):
                d[i] = self._convert_values_datetime(d[i], convert_key)
            return d

    def convert_timestamps(self, pd_content: dict) -> dict:
        """Converteert alle unix time integers naar een datetime object op basis van de lijst van attribuutnamen gespecificeerd in de constructor

        Args:
            pd_content (dict): Power Designer document data

        Returns:
            dict: Hetzelfde Power Designer document data, maar met geconverteerde timestamps
        """
        for field in self.__timestamp_fields:
            pd_content = self._convert_values_datetime(pd_content, field)
        return pd_content

    def extract_value_from_attribute_text(
        self, extended_attrs_text: str, preceded_by: str) -> str:
        """Extraheert de opgegeven tekst uit een tekst string. Deze tekst kan voorafgegaan worden door een specifieke tekst en wordt afgesloten door een \n of het zit aan het einde van de string

        Args:
            extended_attrs_text (str): De tekst dat de waarde bevat waarop gezocht wordt
            preceded_by (str): De tekst die de te vinden tekst voorafgaat

        Returns:
            str: De waarde die geassocieerd wordt met de voorafgaande tekst
        """
        idx_check = extended_attrs_text.find(preceded_by)
        if idx_check > 0:
            logger.info(f"'{idx_check}' values found in extended_attrs_text using: '{preceded_by}'")
            idx_start = extended_attrs_text.find(preceded_by) + len(preceded_by)
            idx_end= len(extended_attrs_text) + 1
            value = extended_attrs_text[idx_start:idx_end]
            idx_start = value.find("=") + 1
            return value[idx_start:].upper()
        logger.warning(f"no values found in extended_attrs_text using: '{preceded_by}'")
        return ""