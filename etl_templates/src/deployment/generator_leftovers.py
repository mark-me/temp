
    def __copy_file(file_source: str, file_destination: str):
        dest_folder = Path(file_destination).parent
        dest_folder.mkdir(parents=True, exist_ok=True)
        shutil.copy2(
            Path(file_source),
            Path(file_destination),
        )

    def __copy_mdde_scripts(self):
        """
        Kopieer de MDDE scripts naar een Visual Studio Project repository folder
        """
        logger.info("Start copy of MDDE scripts to vs Project repo folder.")
        # dir_root = f"{self.params.dir_repository}\\"
        dir_output = "CentralLayer/DA_MDDE"
        dir_scripts_mdde = self.params.generator_config.dir_scripts_mdde
        for platform in [d for d in Path(dir_scripts_mdde).iterdir() if d.is_dir()]:
            logger.info(f"Found platform folder: {dir_scripts_mdde}/{platform.name}.")
            for schema in [d for d in platform.iterdir() if d.is_dir()]:
                logger.info(
                    f"Found schema folder: {dir_scripts_mdde}/{platform.name}/{schema.name}."
                )
                for object_type in [d for d in schema.iterdir() if d.is_dir()]:
                    logger.info(
                        f"Found object type folder: {dir_scripts_mdde}\\{platform.name}\\{schema.name}\\{object_type.name}."
                    )
                    for file in [f for f in object_type.iterdir() if f.is_file()]:
                        # Add used folders to dict_created_ddls to be later used to add to the VS Project file
                        self.__add_object_to_ddl(
                            code_model=schema.name,
                            type_objects=object_type,
                            file_output=file.name,
                        )
                        dir_output_type = f"{dir_output}/{object_type.name}/"
                        Path(os.path.join(self.dir_root, dir_output_type.name)).mkdir(
                            parents=True, exist_ok=True
                        )
                        dest = Path(
                            os.path.join(self.dir_root, dir_output_type, file.name)
                        )
                        logger.info(f"Copy {file} to: {dest.resolve()}")
                        dest.write_text(file.read_text())
                        # Create a copy of the new file to the intermediate folder
                        cp_folder = Path(
                            f"{self.params.dir_generate}/{dir_output_type}/"
                        )
                        cp_folder.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(
                            Path(dest),
                            Path(f"{cp_folder}/{file.name}"),
                        )



    def new_method(self, model_code, file_output, file_output_master):
        if model_code not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(model_code)
        folder_include = f"{model_code}\\PostDeployment"
        if folder_include not in self.created_ddls["Folder Include"]:
            self.created_ddls["Folder Include"].append(folder_include)
        file_include = f"{folder_include}\\{file_output}"
        if file_include not in self.created_ddls["None Include"]:
            self.created_ddls["None Include"].append(file_include)
        file_master_include = f"{folder_include}\\{file_output_master}"
        if file_master_include not in self.created_ddls["None Include"]:
            self.created_ddls["None Include"].append(file_master_include)