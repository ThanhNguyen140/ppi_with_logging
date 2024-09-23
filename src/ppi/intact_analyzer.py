import networkx as nx
import matplotlib.pyplot as plt
import logging

class IntActAnalyzer:
    """Work with network graph for database"""
    def __init__(self, graph: nx.MultiGraph):
        """Initiate network graph

        Args:
            graph (nx.MultiGraph): input as graph of interaction
        """
        self.graph: nx.MultiGraph = graph

    def draw_graph(self, edge_label:str="id", node_label:str ="id", figsize: tuple =(10, 5)):
        """Shows the graph.

        Arguments `edge_label` and `node_label` allows to change the labels in the graph.

        node_label: Any of the keys in node data
        edge_label: Any of the keys in edge data

        Args:
            edge_label (str, optional): Label to be shown on edges. Defaults to "id".
            node_label (str, optional): Label to be shown on nodes. Defaults to "id".
            figsize (tuple, optional): Size of the graph. Defaults to (10, 5).
        """
        plt.figure(figsize=figsize)
        if node_label == "id":
            node_labels = {x: x for x in self.graph.nodes}
        else:
            node_labels = {x: self.graph.nodes[x][node_label] for x in self.graph.nodes}
        edge_labels = {}
        for edge in self.graph.edges:
            eid = str(self.graph.edges[edge][edge_label])
            if edge[:2] not in edge_labels:
                edge_labels[edge[:2]] = eid
            else:
                edge_labels[edge[:2]] += f",{eid}"
        pos = nx.spring_layout(self.graph)  # Layout for the graph
        nx.draw_networkx_nodes(self.graph, pos)
        nx.draw_networkx_edges(self.graph, pos)
        nx.draw_networkx_labels(self.graph, pos, labels=node_labels)
        nx.draw_networkx_edge_labels(self.graph, pos, edge_labels=edge_labels)

        plt.show()

    def get_neighbors_name(self,name:str)-> list:
        """Get neighbors' name of a node

        Args:
            name (str): Name of protein in the node

        Returns:
            list: list of neighbor names
        """
        nodes = dict(self.graph.nodes(data=True))
        for node,data in nodes.items():
            if data["name"] == name:
                neighbors = [n for n in self.graph.neighbors(node)]
                name_neighbors = [self.graph.nodes[n]["name"] for n in neighbors]
        return name_neighbors
    
    def get_protein_with_highest_bc(self):
        """Check protein with highest betweenness centrality

        Returns:
            dict : Dictionary of the protein information with the highest betweenness centrality
        """
        logging.info("Getting protein with highest betweenness centrality")
        dct = nx.betweenness_centrality(self.graph)
        node = sorted(dct.items(), key = lambda x: x[1])[-1]
        data = self.graph.nodes[node[0]]
        data["node_id"] = node[0]
        data["bc_value"] = node[1]
        return data

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename="basic.log",
        filemode="w")