import pandas as pd
import os
from sqlalchemy import create_engine

class Database:
    """Provides tools for working with protein interaction data and store the information in sql database"""
    def __init__(self):
        HOME = os.path.expanduser("~")
        PROJECT_FOLDER = os.path.join(HOME, ".ppi")
        try:
            os.mkdir(PROJECT_FOLDER)
        except FileExistsError:
            pass
        DB_PATH = os.path.join(PROJECT_FOLDER, "ppi.sqlite")
        self.conn = create_engine(f"sqlite:///{DB_PATH}")
        self.path = None

    def set_path_to_data_file(self,path):
        """Set path of database for later processing

        Args:
            path (str): Path to the .tsv file

        Raises:
            FileNotFoundError: error when the path does not exist

        Returns:
            str: set path to class instance to read data file
        """
        if os.path.isfile(path):
            self.path = path
            return self.path
        else:
            raise FileNotFoundError

    def read_data(self):
        """Generate Pandas DataFrame from .tsv file

        Returns:
            DataFrame: DataFrame from the .tsv file
        """
        self.df = pd.read_csv(self.path,sep = "\t")
        return self.df
    
    def get_proteins(self):
        """Generate protein DataFrame to read protein names,accession and taxid

        Returns:
            DataFrame: Protein DataFrame
        """
        self.read_data()
        df_a = self.df[["a_uniprot_id","a_name","a_taxid"]]
        df_a = df_a.drop_duplicates(ignore_index=True)
        df_a = df_a.rename(columns = {"a_taxid":"taxid","a_uniprot_id":"uniprot_id","a_name":"name"})
        
        df_b = self.df[["b_uniprot_id","b_name","b_taxid"]]
        df_b = df_b.drop_duplicates(ignore_index=True)
        df_b = df_b.rename(columns = {"b_taxid":"taxid","b_uniprot_id":"uniprot_id","b_name":"name"})

        self.protein_df = pd.concat([df_a,df_b])
        self.protein_df = self.protein_df.sort_values(by = "name")
        self.protein_df = self.protein_df.drop_duplicates(ignore_index=True)
        self.protein_df = self.protein_df.reset_index(drop=True)
        self.protein_df.index += 1
        self.protein_df.index.name = "id"
        self.protein_df = self.protein_df.rename(columns = {"uniprot_id":"accession"})
        return self.protein_df
    
    def get_interactions(self):
        """Generate interaction DataFrame to read interaction information between proteins

        Returns:
            DataFrame: Interaction DataFrame with the interaction information between proteins
        """
        self.get_proteins()
        df2 = self.df.drop(["a_name","a_taxid","b_name","b_taxid"], axis = 1)
        self.protein_df["id"] = self.protein_df.index.to_list()
        id_merge = self.protein_df.drop(["name","taxid"], axis = 1)
        self.interaction = df2.merge(id_merge,left_on = "a_uniprot_id", right_on = "accession").drop(["a_uniprot_id"],axis = 1)
        self.interaction = self.interaction.rename(columns={"id":"protein_a_id"}).drop(["accession"], axis = 1)
        self.interaction = self.interaction.merge(id_merge,left_on = "b_uniprot_id", right_on = "accession").drop(["b_uniprot_id"],axis = 1)
        self.interaction = self.interaction.rename(columns={"id":"protein_b_id"}).drop(["accession"], axis = 1)
        self.protein_df = self.protein_df.drop(["id"],axis = 1)
        self.interaction = self.interaction.sort_values(by = "confidence_value", ignore_index = True)
        self.interaction.index += 1
        self.interaction.index.name = "id"
        return self.interaction
    
    def import_data(self):
        """Generate SQL database for protein and interaction tables in a defined DB PATH
        """
        self.get_proteins()
        self.get_interactions()
        self.protein_df.to_sql("protein", self.conn, if_exists="replace")
        self.interaction.to_sql("interaction", self.conn,if_exists = "replace")
    

    def get_table_names(self):# -> list[Any]:
        """Get generated table names

        Returns:
            list: list of generated table names
        """
        tables = []
        try:
            self.protein_df
            tables.append("protein")
        except:
            pass
        try:
            self.protein_df
            tables.append("interaction")
        except:
            pass
        return tables

    def get_columns(self,table):
        """Get column names from sql tabels

        Args:
            table (str): name of table

        Returns:
            list: list of column names
        """
        table_sql = pd.read_sql_table(table,self.conn)
        return table_sql.columns.tolist()

        



