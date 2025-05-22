    def __add_object_to_ddl(self, code_model: str, type_objects: str, file_output: str):
        """
        Voegt een object toe aan de lijst van aangemaakte DDL's voor het model en het type object.

        Deze methode houdt bij welke folders en bestanden zijn aangemaakt, zodat deze later kunnen worden toegevoegd aan het VS Project.

        Args:
            code_model (str): De code van het model.
            type_objects (str): Het type object, bijvoorbeeld 'Tables' of 'Views'.
            file_output (str): De bestandsnaam van het gegenereerde DDL-bestand.
        """
        folder_model = code_model
        if folder_model not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(folder_model)
        folder_tables = f"{code_model}\\{type_objects}"
        if folder_tables not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(folder_tables)
        table_file = f"{folder_tables}\\{file_output}"
        if table_file not in self.created_ddls["Build Include"]:
            self.created_ddls["Build Include"].append(table_file)



    def __add_post_deploy_to_ddl(self, file_output, file_output_master):
        """
        Voegt post-deploy scripts toe aan de lijst van aangemaakte DDL's voor het post-deploy proces.

        Deze methode houdt bij welke folders en bestanden voor post-deploy zijn aangemaakt,
        zodat deze later kunnen worden toegevoegd aan het VS Project.

        Args:
            file_output (str): De bestandsnaam van het post-deploy script.
            file_output_master (str): De bestandsnaam van het master post-deploy script.
        """
        if self.schema_post_deploy not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(self.schema_post_deploy)
        folder_model = f"{self.schema_post_deploy}\\PostDeployment"
        if folder_model not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(folder_model)
        file_codelist = f"{folder_model}\\{file_output}"
        if file_codelist not in self.created_ddls["None Include"]:
            self.created_ddls["None Include"].append(file_codelist)
        file_master = f"PostDeployment\\{file_output_master}"
        if file_master not in self.created_ddls["None Include"]:
            self.created_ddls["None Include"].append(file_master)

