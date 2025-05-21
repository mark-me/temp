from datetime import datetime
from typing import Union

from log_config import logging


logger = logging.getLogger(__name__)


class ObjectTransformer:
    """Collectie van functions die structuren en data van Power Designer objecten kan transformeren

    Het transformeren van structuren wordt gedaan om het 'querien' van data voor de ETL en DDL te versimpelen 
    """

    def __init__(self):
        self.__timestamp_fields = ["a:CreationDate", "a:ModificationDate"]

    def clean_keys(self, content: Union[dict, list]):
        """Hernoemt sleutels van Power Designer objecten (m.a.w. dictionaries) zodat prefixes als '@' en 'a:' worden verwijderd

        Args:
            content (Union[dict, list]): Een dict of list van dicts met Power Designer objecten

        Returns:
            _type_: List of dict met hernoemde sleutels (afhankelijk van welk type werd doorgegeven als parameter)
        """
        if isinstance(content, dict):
            logger.info("List object is actually dictionary; file:pd_transform_object; object:content")
            lst_object = [content]
        else:
            lst_object = content
        for i in range(len(lst_object)):
            attrs = [key for key in list(lst_object[i].keys()) if key[:1] == "@"]
            for attr in attrs:
                lst_object[i][attr[1:]] = lst_object[i].pop(attr)
            attrs = [key for key in list(lst_object[i].keys()) if key[:2] == "a:"]
            for attr in attrs:
                lst_object[i][attr[2:]] = lst_object[i].pop(attr)

        if isinstance(content, dict):
            result = lst_object[0]
        else:
            result = lst_object
        return result

    def __convert_values_datetime(self, d: dict, convert_key: str) -> dict:
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
                    self.__convert_values_datetime(d[key], convert_key)
            return d
        elif isinstance(d, list):
            logger.debug("object is list; file:pd_transform_object; object:d")
            for i in range(len(d)):
                d[i] = self.__convert_values_datetime(d[i], convert_key)
            return d

    def convert_timestamps(self, pd_content: dict) -> dict:
        """Converteert alle unix time integers naar een datetime object op basis van de lijst van attribuutnamen gespecificeerd in de constructor

        Args:
            pd_content (dict): Power Designer document data

        Returns:
            dict: Hetzelfde Power Designer document data, maar met geconverteerde timestamps
        """
        for field in self.__timestamp_fields:
            pd_content = self.__convert_values_datetime(pd_content, field)
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
            idx_end = extended_attrs_text.find("\n", idx_start)
            idx_end = idx_end if idx_end > -1 else len(extended_attrs_text) + 1
            value = extended_attrs_text[idx_start:idx_end]
            idx_start = value.find("=") + 1
            value = value[idx_start:].upper()
        else:
            logger.warning(f"no values found in extended_attrs_text using: '{preceded_by}'")
            value = ""
        return value