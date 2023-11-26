import pandas as pd
import os


DB_PATH = os.path.join(PROJECT_FOLDER, "ppi.sqlite")

class Database:
    def __init__(self):
        HOME = os.path.expanduser("~")
        PROJECT_FOLDER = os.path.join(HOME, ".ppi")
        self.path = None
        self.interaction = None
        self.protein_df = None

    def set_path_to_data_file(self,path):
        if os.path.isfile(path):
            self.path = path
            return os.path.isfile(path)

    def read_data(self):
        self.df = pd.read_csv(self.path,sep = "\t")
        return self.df
    
    def get_proteins(self):
        df_a = df[["a_uniprot_id","a_name","a_taxid"]]
        df_a = df_a.drop_duplicates(ignore_index=True)
        df_a = df_a.rename(columns = {"a_taxid":"taxid","a_uniprot_id":"uniprot_id","a_name":"name"})
        
        df_b = df[["b_uniprot_id","b_name","b_taxid"]]
        df_b = df_b.drop_duplicates(ignore_index=True)
        df_b = df_b.rename(columns = {"b_taxid":"taxid","b_uniprot_id":"uniprot_id","b_name":"name"})

        self.protein_df = pd.concat([df_a,df_b])
        self.protein_df = self.protein_df.sort_values(by = "name")
        self.protein_df = self.protein_df.drop_duplicates(ignore_index=True)
        self.protein_df = self.protein_df.reset_index(drop=True)
        self.protein_df["id"] = self.protein_df.index + 1
        self.protein_df = self.protein_df.rename({"uniprot_id":"accession"})
        self.protein_df = self.protein_df[["id","accession","name","taxid"]]
        return self.protein_df
    
    def get_interactions(self):
        df2 = self.df.drop(["a_name","a_taxid","b_name","b_taxid"], axis = 1)
        id_merge = self.protein_df.drop(["name","taxid"], axis = 1)
        self.interaction = df2.merge(id_merge,left_on = "a_uniprot_id", right_on = "uniprot_id").drop(["a_uniprot_id"],axis = 1)
        self.interaction = self.interaction.rename(columns={"id":"protein_a_id"}).drop(["uniprot_id"], axis = 1)
        self.interaction = self.interaction.merge(id_merge,left_on = "b_uniprot_id", right_on = "uniprot_id").drop(["b_uniprot_id"],axis = 1)
        self.interaction = self.interaction.rename(columns={"id":"protein_b_id"}).drop(["uniprot_id"], axis = 1)
        return self.interaction
    
    def get_table_names(self):
        tables = []
        if self.protein_df:
            tables.append("protein")
        if self.interaction:
            tables.append("interaction")

    def get_columns(self,table = "protein" or "interaction"):
        if table == "protein":
            return self.protein_df.columns
        elif table == "interaction":
            return self.interaction.columns
    


