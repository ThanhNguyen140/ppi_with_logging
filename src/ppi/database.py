import pandas as pd
import os
from sqlalchemy import create_engine
import networkx as nw
import logging

class Database:
    """This class works with export data to generate database and filter database"""
    def __init__(self):
        HOME = os.path.expanduser("~")
        PROJECT_FOLDER = os.path.join(HOME, ".ppi")
        try:
            os.mkdir(PROJECT_FOLDER)
        except FileExistsError:
            pass
        DB_PATH = os.path.join(PROJECT_FOLDER, "ppi.sqlite")
        logging.info(f"Creating engine at {DB_PATH}")
        self.engine = create_engine(f"sqlite:///{DB_PATH}")
        self.path = None
        self._has_data = False

    def set_path_to_data_file(self,path:str):
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
            logging.info(f"Path to file: {path}")
            return True
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
        self.protein_df = self.protein_df.rename(columns = {"uniprot_id":"accession"})
        self.protein_df = self.protein_df.sort_values(by = "accession", ignore_index = True)
        self.protein_df.index += 1
        self.protein_df.index.name = "id"
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
        """Generate SQL database for protein and interaction tables in a defined DB PATH"""
        self.get_proteins()
        logging.info("Creating protein table")
        self.get_interactions()
        logging.info("Creating interaction table")
        self.protein_df.to_sql("protein", self.engine, if_exists="replace")
        self.interaction.to_sql("interaction", self.engine,if_exists = "replace")


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

    def get_columns(self,table:str):
        """Get column names from sql tabels

        Args:
            table (str): name of table

        Returns:
            list: list of column names
        """
        table_sql = pd.read_sql_table(table,self.engine)
        return table_sql.columns.tolist()

    def get_where(self,pmid=False,detection_method=False,interaction_type=False,confidence_value_gte=False,disallow_self_interaction=False):
        """Generate where clause for SQL query

        Args:
            pmid (bool, optional): Filtering pmid values. Defaults to False.
            detection_method (bool, optional): Filtering detection_method. Defaults to False.
            interaction_type (bool, optional): Filtering interaction_type. Defaults to False.
            confidence_value_gte (bool, optional): Filtering confidence_value. Defaults to False.
            disallow_self_interaction (bool, optional): Filtering protein_a_id != protein_b_id. Defaults to False.

        Returns:
            str: where clause for SQL query
        """
        where = ""
        if pmid:
            where += f" WHERE pmid = '{pmid}'"
        if detection_method:
            if len(where) == 0:
                where += f" WHERE detection_method = '{detection_method}'"
            else: 
                where += f" AND detection_method = '{detection_method}'"
        if interaction_type:
            if len(where) == 0:
                where += f" WHERE interaction_type = '{interaction_type}'"
            else: 
                where += f" AND interaction_type = '{interaction_type}'"
        if confidence_value_gte:
            if len(where) == 0:
                where += f" WHERE confidence_value >= {confidence_value_gte}"
            else: 
                where += f" AND confidence_value >= {confidence_value_gte}"
        if disallow_self_interaction:
            if len(where) == 0:
                where += " WHERE protein_a_id != protein_b_id"
            else: 
                where += " AND protein_a_id != protein_b_id"
        return where
    
    def get_detection_method_statistics(self):
        """Get detection_method statistics

        Returns:
            DataFrame: A DataFrame of detection_method and the statistics
        """
        column = self.interaction.detection_method
        ind = list(column.unique())
        number = [column[column == x].count() for x in ind]
        df = pd.DataFrame(data = number, index = ind, columns = ["number"])
        df.index.name = "detection_method"
        return df

    def get_pmid_statistics(self):
        """Get pmid statistics

        Returns:
            DataFrame: A DataFrame of pmid and the statistics
        """
        column = self.interaction.pmid
        ind = list(column.unique())
        number = [column[column == x].count() for x in ind]
        df = pd.DataFrame(data = number, index = ind, columns = ["number"])
        df.index.name = "pmid"
        return df
    
    def get_interaction_type_statistics(self):
        """Get interaction_type statistics

        Returns:
            DataFrame: A DataFrame of interaction_type and the statistics
        """
        column = self.interaction.interaction_type
        ind = list(column.unique())
        number = [column[column == x].count() for x in ind]
        df = pd.DataFrame(data = number, index = ind, columns = ["number"])
        df.index.name = "interaction_type"
        return df

    def get_confidence_value_statistics(self):
        """Get confidence_value statistics

        Returns:
            DataFrame: A DataFrame of confidence_value and the statistics
        """
        column = self.interaction.confidence_value
        ind = list(column.unique())
        number = [column[column == x].count() for x in ind]
        df = pd.DataFrame(data = number, index = ind, columns = ["number"])
        df.index.name = "confidence_value"
        return df
        
    def get_graph(self,pmid=False,detection_method=False,interaction_type=False,confidence_value_gte=False,disallow_self_interaction=False):
        """Generate graphs of protein interactions

        Args:
            pmid (bool, optional): Filtering of pmid value. Defaults to False.
            detection_method (bool, optional): Filtering of detection_method. Defaults to False.
            interaction_type (bool, optional): Filtering of interaction_type. Defaults to False.
            confidence_value_gte (bool, optional): Filtering of confidence_value. Defaults to False.
            disallow_self_interaction (bool, optional): Filtering of protein_a_id != protein_b_id. Defaults to False.

        Returns:
            _type_: _description_
        """
        where = self.get_where(pmid,detection_method,interaction_type,confidence_value_gte,disallow_self_interaction)
        query = f"Select * from interaction {where}"
        df = pd.read_sql(query,self.engine)
        protein = pd.read_sql_table("protein",self.engine)
        graph = nw.MultiGraph()
        nodes = list(set(df.protein_a_id.to_list()).union(set(df.protein_b_id.to_list())))
        nodes_info = [{"accession":protein.accession[i-1],\
                       "name":protein.name[i-1],\
                        "taxid":protein.taxid[i-1]} for i in nodes]
        edges = [(df["protein_a_id"][x],df["protein_b_id"][x],{"id": df.id[x],\
                "confidence_value": df.confidence_value[x],\
                "pmid": df.pmid[x],\
                "interaction_type": df.interaction_type[x],\
                "detection_method": df.detection_method[x]}) for x in range(len(df.protein_a_id))]
        graph.add_nodes_from([(nodes[x],nodes_info[x]) for x in range(len(nodes))])
        graph.add_edges_from(edges)
        return graph

    def drop_database(self) -> None:
        """Delete the database directory
        """
        HOME = os.path.expanduser("~")
        PROJECT_FOLDER = os.path.join(HOME, ".ppi")
        DB_PATH = os.path.join(PROJECT_FOLDER, "ppi.sqlite")
        os.remove(DB_PATH)
    
    @property
    def exists(self) -> bool:
        """Check if the database exists

        Returns:
            bool: True if database exists. Else, return False
        """
        try:
            pd.read_sql("protein",self.engine)
            pd.read_sql("interaction",self.engine)
            return True
        except:
            return False
        
    @property
    def has_data(self) -> bool:
        """Check if interaction table has data

        Returns:
            bool: Returns True if interaction table has data. Otherwise, return False
        """
        if len(self.interaction) > 0:
            self._has_data = True
        else:
            self._has_data = False
        return self._has_data

    def count_nodes(self,graph:nw.MultiGraph) -> int:
        """Get number of nodes from graph

        Args:
            graph (nw.MultiGraph): Graph built by networkx.MultiGraph

        Returns:
            int: Number of nodes in graph
        """
        logging.info(f"Creating graph with {len(graph.nodes)} nodes")
        return len(graph.nodes)
    
logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename="basic.log",
        )