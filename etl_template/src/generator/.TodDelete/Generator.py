import datetime
import os
import sys
import json
from pathlib import Path

import xmltodict
from jinja2 import Environment, FileSystemLoader

print(os.getcwd())
if __name__ == "__main__":
    sys.path.append(os.getcwd())
from etl_templates.src.log_config.logging_config import logging

logger = logging.getLogger(__name__)

"""Reading the XML Power Designer ldm file into a dictionary

        Args:
            file_input (str): The path to a JSON file

        Returns:
            dict: The JSON (Power Designer model data) converted to a dictionary
        """


class Generator:
    def __init__(self, file_input: str, folder_output: str):
        self.file_input = file_input
        self.folder_output = folder_output
        # Extracting data from the file
        self.content = self.__read_file_model(file_input=file_input)
        self.__create_source_view()
        self.__create_filter_functions()
        self.__get_templates()
        self.__generate_ddl()
        self.__write_ddl()
        
        print("")
    
    # TODO: Deze ombouwen naar JINJA template use.
    def __create_filter_functions(self):
        for filter in self.dict_model['Transformations']['Filters']:
            sqlstatement = ""
            sqlstatement = sqlstatement +  "CREATE FUNCTION [" + filter['CodeModel'] + "].[" +  filter['Code'].lower() + "] () RETURNS nvarchar(50)" + " AS " + "\n"
            sqlstatement = sqlstatement +  "BEGIN" + "\n"
            for attribute in filter['Attributes']: 
                sqlstatement = sqlstatement + "\t" + "DECLARE @" + attribute['Name']
                if 'DataType' in attribute:
                                if 'Length' in attribute:
                                    length = attribute['Length']
                                else: 
                                    length = ''
                                if 'Precision' in attribute:
                                    precision = attribute['Precision']
                                else: 
                                    precision = ''
                                datatypesql = self.__dataTypeToMsSql(attribute['DataType'],length, precision)
                elif 'DataType' not in attribute:
                    datatypesql = 'nvarchar(50)'
                    logger.error(f"For filter {filter['Code'].lower()} in model {filter['CodeModel']} not Datatype is given.")
                sqlstatement = sqlstatement + " " + datatypesql + ";" + "\n"
                sqlstatement = sqlstatement + "\t" + "SET " + filter['SqlExpression'] + "\n"
                sqlstatement = sqlstatement + "\t" + "RETURN @" + attribute['Name'] + ";" + "\n"
                sqlstatement = sqlstatement + "END"
            dir_output = self.folder_output + filter['CodeModel'] + '/' + 'functions' + '/' 
            directory = Path(dir_output)
            # Make directory if not exist.
            directory.mkdir(parents=True, exist_ok=True)
            file_output = dir_output + filter['Code'].lower() + '.sql'
            with open(file_output, mode="w", encoding="utf-8") as file_ddl:
                    file_ddl.write(sqlstatement)
            logger.info(f"Written Filter DDL {file_output}")
            
    def __create_source_view(self):
        print("")
        for mapping in self.dict_model['Transformations']['Mappings']:
            if 1==1: #mapping["Name"] == 'DTO':
                sqlstatement = ""
                #sqlstatement = "CREATE VIEW " + sqlstatement + mapping['MappingName'] + " AS " + "\n"
                sqlstatement = "CREATE VIEW " + mapping['EntityTarget']['CodeModel'] + ".vw_src_" +  mapping['EntityTarget']['Name'] + "_" + mapping['Code'] + " AS " + "\n"
                sqlstatement = sqlstatement + "SELECT " + "\n"
                #sqlstatement = self.__createKeyColumn(mapping, sqlstatement)
                sqlstatement = self.__createBkColumn(mapping, sqlstatement)
                sqlstatement = sqlstatement + "\t" + ",a.* " + "\n" + "FROM ( "+ "\n" + "SELECT " + "\n"
                sqlstatement = self.__createSelectAttributes(mapping, sqlstatement)
                sqlstatement = self.__createHashForAttributes(mapping, sqlstatement)                              
                sqlstatement = self.__createSysAttributes(sqlstatement)
                sqlstatement = self.__createSourceObjects(mapping, sqlstatement) 
                sqlstatement = self.__createFilters(mapping, sqlstatement)
                sqlstatement = sqlstatement + "\t" + ") as a"
            print(sqlstatement)
            dir_output = self.folder_output + mapping['EntityTarget']['CodeModel'] + '/' + 'views' + '/' 
            directory = Path(dir_output)
            # Make directory if not exist.
            directory.mkdir(parents=True, exist_ok=True)
            file_output = dir_output + 'vw_src_' + mapping['EntityTarget']['Name'] + "_" + mapping['Code'] + '.sql'
            with open(file_output, mode="w", encoding="utf-8") as file_ddl:
                    file_ddl.write(sqlstatement)
            logger.info(f"Written Table DDL {file_output}")
        print("")

    def __createKeyColumn(self, mapping, sqlstatement):
        for Identifiers  in mapping['EntityTarget']['Identifiers']:
            if Identifiers['IsPrimary'] == True:
                sqlstatement = sqlstatement + "\t" + " " + mapping['EntityTarget']['Name'] + "Key" + " = " + "HASHBYTES('SHA2_512',"
                sqlstatement = sqlstatement + "TRIM(" + "CONCAT("
                for index, Attribute in enumerate(Identifiers['Attributes']):
                    if index > 0:
                        sqlstatement = sqlstatement + ", '_', "
                    sqlstatement = sqlstatement + Attribute['Name'] + ")"
                sqlstatement = sqlstatement + ')'
                sqlstatement = sqlstatement + ')'+ "\n" + ","
        return sqlstatement
    
    def __createBkColumn(self, mapping, sqlstatement):
        for Identifiers  in mapping['EntityTarget']['Identifiers']:
            if Identifiers['IsPrimary'] == True:
                sqlstatement = sqlstatement + "\t"  + mapping['EntityTarget']['Name'] + "BK" + " = " 
                sqlstatement = sqlstatement + "TRIM(" + "CONCAT('', "
                for index, Attribute in enumerate(Identifiers['Attributes']):
                    if index > 0:
                        sqlstatement = sqlstatement + ", '_', "
                    sqlstatement = sqlstatement + Attribute['Name'] + ")"
                sqlstatement = sqlstatement + ')'+ "\n"
        return sqlstatement

    def __createSelectAttributes(self, mapping, sqlstatement):
        for mappingattribute in mapping['AttributeMapping']:
            sqlstatement = sqlstatement + "\t"
            if mappingattribute['Order'] > 0:
               sqlstatement = sqlstatement + "," 
            sqlstatement = sqlstatement + mappingattribute['AttributeTarget']['Name'] + " = "
            if 'Expression' in mappingattribute:
                sqlstatement = sqlstatement + mappingattribute['Expression'] + "\n"
            else: 
                aliasnaam = ""
                if 'EntityAliasName' in mappingattribute['AttributesSource']:
                    aliasnaam = mappingattribute['AttributesSource']['EntityAliasName']
                if 'EntityAlias' in mappingattribute['AttributesSource']:
                    sqlstatement = sqlstatement + f" /*{aliasnaam}*/ " + mappingattribute['AttributesSource']['EntityAlias'] + "."
                    sqlstatement = sqlstatement + mappingattribute['AttributesSource']['Name']  + "\n"
        return sqlstatement

    def __createFilters(self, mapping, sqlstatement):
        sqlstatement = sqlstatement + "WHERE 1=1"
        for filters in mapping['SourceObjects']:
            if filters['JoinType'] == 'APPLY':
                if 'JoinConditions' in filters:
                    for filter in filters['JoinConditions']:
                        sqlstatement = sqlstatement + " AND "  + "\n" 
                        sqlstatement = sqlstatement + filters['Entity']['CodeModel'] + "." + filters['Code'].lower() + "()" + " = " 
                        sqlstatement = sqlstatement + filter['JoinConditionComponents']['AttributeParent']['EntityAlias'] + "." + filter['JoinConditionComponents']['AttributeParent']['Code']
        return sqlstatement

    def __createSourceObjects(self, mapping, sqlstatement):
        for SourceObject in mapping['SourceObjects']:
            if SourceObject['JoinType'] != 'APPLY':
                sqlstatement = sqlstatement + SourceObject['JoinType'] + " "
                JoinAliasName = SourceObject['JoinAliasName']
                JoinAlias = SourceObject['JoinAlias']
                sqlstatement = sqlstatement + SourceObject['Entity']['CodeModel'] + "." + SourceObject['Entity']['Code']  + " AS " + JoinAlias  + f" /*{JoinAliasName}*/ " 
                if SourceObject['Order'] > 0:
                    sqlstatement = sqlstatement + " ON " 
                if 'JoinConditions' in SourceObject:
                    for JoinCondition in SourceObject['JoinConditions']:
                        if JoinCondition['Order'] > 0:
                            sqlstatement = sqlstatement + " AND " 
                        sqlstatement = sqlstatement + "\n" + "\t" + JoinAlias  + "." + JoinCondition['JoinConditionComponents']['AttributeChild']['Code'] + " " + JoinCondition['Operator'] + " "
                        ParentLiteral = str(JoinCondition['ParentLiteral']) + ""
                        if ParentLiteral == "":
                            sqlstatement = sqlstatement + JoinCondition['JoinConditionComponents']['AttributeParent']['EntityAlias'] + "." + JoinCondition['JoinConditionComponents']['AttributeParent']['Code']
                        elif not ParentLiteral == "":
                            sqlstatement = sqlstatement + ParentLiteral
                sqlstatement = sqlstatement + "\n"
        return sqlstatement

    def __createSysAttributes(self, sqlstatement):
        sqlstatement = sqlstatement + "\t" + ",[X_LoadDateTime] = GETDATE() " + "\n"
        sqlstatement = sqlstatement + "\t" + ",[X_StartDate] = CAST(GETDATE() AS DATE) " + "\n"
        sqlstatement = sqlstatement + "\t" + ",[X_EndDate]   = '2099-12-31' " + "\n"
        sqlstatement = sqlstatement + "\t" + ",[X_IsCurrent]   = 1 " + "\n"
        return sqlstatement

    def __createHashForAttributes(self, mapping, sqlstatement):
        sqlstatement = sqlstatement + "\t" + ",[X_HashKey] = HASHBYTES('SHA2_512', CONCAT (  " + "\n" 
        for mappingattribute in mapping['AttributeMapping']:
            sqlstatement = sqlstatement + "\t"
            if mappingattribute['Order'] > 0:
               sqlstatement = sqlstatement + "," 
            if 'Expression' in mappingattribute:
                sqlstatement = sqlstatement + "\t" + mappingattribute['Expression'] + "\n"
            else: 
                if 'EntityAlias' in mappingattribute['AttributesSource']:
                    sqlstatement = sqlstatement + "\t" + "ISNULL("
                    sqlstatement = sqlstatement + mappingattribute['AttributesSource']['EntityAlias'] + "."
                    sqlstatement = sqlstatement + mappingattribute['AttributesSource']['Code']  
                    sqlstatement = sqlstatement + ", '')"+ "\n"
        sqlstatement = sqlstatement +"\t" +"\t" + "))       "  + "\n"
        return sqlstatement
        #with open(f"./output/debug/Mapping_{name}.json", mode="w") as f:
        #        json.dump(mapping,f,indent=3, default=self.__serialize_datetime)
    
    
    def __read_file_model(self, file_input: str) -> dict:
        """Reading the XML Power Designer ldm file into a dictionary

        Args:
            file_input (str): The path to a JSON file

        Returns:
            dict: The JSON (Power Designer model data) converted to a dictionary
        """
        # Function not yet used, but candidate for reading XML file
        with open(file_input) as json_file:
            self.dict_model = json.load(json_file)
        return self.dict_model
    
    def __get_templates(self):
        """
        Creates the DDL's

        Args:
         type_template (str): The type of templates your want to use to implement your models
            dict_object (dect): The object that describes the object for the template
        """
        # Loading templates
        dest_type = "dedicated-pool"
        dir_template = "./etl_templates/src/generator/templates/" + dest_type + "/"
        environment = Environment(
            loader=FileSystemLoader(dir_template), trim_blocks=True, lstrip_blocks=True
        )
        self.dict_templates = {
            "schema": environment.get_template("create_schema.sql"),
            "Tables": environment.get_template("create_table.sql"),
            "Entities": environment.get_template("create_table.sql"),
            "Views": environment.get_template("create_view.sql"),
            "Procedures": environment.get_template("create_procedure.sql"),
        }
        return self.dict_templates
    
    def __generate_ddl(self):
        print("")
        self.lst_ddls = []
        lst_objects = []
        self.lst_cleanobjects = []
        print("")
        self.dict_templates.items()
        print("")
        for type_object, template in self.dict_templates.items():
            print("")
            for model in self.dict_model["Models"]:
                # Rename Entities to Tables to get the right template
                if "Entities" in model:
                    model["Tables"] = model["Entities"]
                    del model["Entities"]
                    for table in model['Tables']:
                        # Rename Attributes to Columns and delete Attributes from table
                        table["Columns"] = table['Attributes']
                        del table['Attributes']
                        # Rename Number to Rowcount and delete Number from table
                        if 'Number' in table:
                            table["Rowcount"] = table['Number'] 
                            del table['Number']
                        else: 
                            table["Rowcount"] = '0'
                            logger.warning(f"Table: {table['Name']} from model: {model['Name']} does not have a Rowcount(number), DISTRIBUTION will be set to ROUND_ROBIN.")
                        for column in table['Columns']:
                            if 'DataType' in column:
                                if 'Length' in column:
                                    length = column['Length']
                                else: 
                                    length = ''
                                if 'Precision' in column:
                                    precision = column['Precision']
                                else: 
                                    precision = ''
                                column['DataType'] = self.__dataTypeToMsSql(column['DataType'],length, precision)
                                print('')
                            else:
                                logger.error(f"Column: {column['Name']} in table: {table['Name']} from model: {model['Name']} does not have a datatype.")
                lst_objects = []
                self.lst_cleanobjects = []
                if model["IsDocumentModel"]: 
                    if type_object in model:
                        lst_objects = model[type_object]
                        
                        for i, object in enumerate(lst_objects):
                            if 'Stereotype' not in object:
                                self.lst_cleanobjects.append(lst_objects[i])
                        for i, object in enumerate(self.lst_cleanobjects):
                            object["Schema"] = model["Code"]
                            self.lst_cleanobjects[i] = object
                    else:
                        logger.warning(f"Object for '{type_object}' does not exist in the model.")
            self.lst_ddls.append(
                {"type": type_object, "template": template, "objects": self.lst_cleanobjects})
        print("")
        return self.lst_ddls
    
    def __dataTypeToMsSql(self, datatype, length, precision ):
        self.length = ''
        self.precision = ''
        if length != '':
            self.length = '(' + length
        if precision != '':
            self.precision = ', ' + precision + ')'
        elif length != '':
            self.precision = ')'
        else:
            self.precision = ''
        # get datatype without Length and Precision
        if datatype[:1] == 'N':
            self.datatype = 'N'
        elif datatype[:2] == 'DC':
            self.datatype = 'DC'
        elif datatype[:1] == 'F':
            self.datatype = 'F'
        elif datatype[:2] == 'MN':
            self.datatype = 'MN'
        elif datatype[:2] == 'NO':
            self.datatype = 'NO'
        elif datatype[:1] == 'A':
            self.datatype = 'A'
        elif datatype[:2] == 'VA':
            self.datatype = 'VA'
        elif datatype[:4] == 'VMBT':
            self.datatype = 'VMBT'
        elif datatype[:2] == 'LA':
            self.datatype = 'LA'
        elif datatype[:3] == 'LVA':
            self.datatype = 'LVA'
        elif datatype[:3] == 'TXT':
            self.datatype = 'TXT'
        elif datatype[:3] == 'MBT':
            self.datatype = 'MBT'
        elif datatype[:3] == 'BIN':
            self.datatype = 'BIN'
        elif datatype[:4] == 'VBIN':
            self.datatype = 'VBIN'
        elif datatype[:4] == 'LBIN':
            self.datatype = 'LBIN'
        elif datatype[:3] == 'BMP':
            self.datatype = 'BMP'
        elif datatype[:3] == 'PIC':
            self.datatype = 'PIC'
        elif datatype[:3] == 'OLE':
            self.datatype = 'OLE'
        else:
            self.datatype = datatype
        translations = {
             'I' : 'int'   # Integer
            ,'SI' : 'smallint'  # Short Integer
            ,'LI' : 'bigint'  # Long Integer
            ,'BT' : 'tinyint'  # Byte: Old Byte type range is from 0-255 so fits in a tinyint
            ,'N' : 'numeric'   # Number
            ,'DC' : 'decimal'  # Decimal
            ,'F' : 'float'   # Float
            ,'SF' : 'float'  # Short Float
            ,'LF' : 'float'  # Long Float
            ,'MN' : 'money'  # Money
            ,'NO' : 'int'  # Serial: The SERIAL data type is not known in MSSQL, but stores a sequential integer, of the INT data type, that is automatically assigned by the database server when a new row is inserted.
            ,'BL' : 'bit'  # Boolean
            ,'A' : 'nchar'   # Characters
            ,'VA' : 'nvarchar'  # Variable Characters 
            ,'LA' : 'nvarchar'  # Long Characters 
            ,'LVA' : 'nvarchar' # Long Variable Characters 
            ,'TXT' : 'ntext' # Text
            ,'MBT' : 'nchar' # MultiByte: A multibyte character is not known in MSSQL, but is a character composed of sequences of one or more bytes. Each byte sequence represents a single character in the extended character set.
            ,'VMBT' : 'nvarchar'# Variable MultiByte: A multibyte character is not known in MSSQL, but is a character composed of sequences of one or more bytes. Each byte sequence represents a single character in the extended character set.
            ,'D' : 'date'   # Date
            ,'T' : 'time'   # Time
            ,'DT' : 'datetime2'  # DateTime
            ,'TS' :'timestamp'   # TimeStamp: datatype is also known as "rowversion", and the original name is really unfortunate. Because it is monotonically increasing, some people also use it to find the most recently changed rows in a table, but this is not what it is designed for, and it does not work well in high-concurrency environment
            ,'BIN' : 'binary' # Binary
            ,'VBIN' : 'varbinary'   # Variable Binary
            ,'LBIN' : 'varbinary'   # Long Binary
            ,'BMP' : 'nvarchar(500)' # Bitmap: Not used, but gets a default datatype in this conversion,
            ,'PIC' : 'nvarchar(500)' # Image: Not used, but gets a default datatype in this conversion,
            ,'OLE' : 'nvarchar(500)' # OLE: Not used, but gets a default datatype in this conversion,
            ,'P' : 'nvarchar(500)'   # Point: Not used, but gets a default datatype in this conversion,
            ,'G' : 'geometry 1'   # Geometry
            }
        datatypeSql = translations[self.datatype] + self.length  + self.precision
        return datatypeSql
        
    def __write_ddl(self):
         #self.lst_ddls[0]['type']
        for ddls in self.lst_ddls:
            print(ddls['type'])
            for object in ddls['objects']:
                dir_output = self.folder_output + object['Schema'] + "/" + ddls['type'].lower() + "/"
                # Fill Path with the destination directory. Path is used for file system operations
                directory = Path(dir_output)
                # Make directory if not exist.
                directory.mkdir(parents=True, exist_ok=True)
                content = self.dict_templates[ddls['type']].render(item=object)
                file_output = dir_output + object['Name'] + ".sql"
                with open(file_output, mode="w", encoding="utf-8") as file_ddl:
                    file_ddl.write(content)
                logger.info(f"Written Table DDL {file_output}")
                print("")

# Run Current Class
if __name__ == "__main__":
    #file_input = "./etl_templates/output/WerkBestand.json"  
    file_input = "./etl_templates/output/Example_CL_LDM.json"
    #file_input = "./etl_templates/output/LDM Country.json"
    folder_vsProject= "./etl_templates/output/ddl/"
    p = Path(file_input).resolve()
    print(f"FilePatch: {p}")
    Generator(file_input=file_input, folder_output=folder_vsProject)
    print("Done")
