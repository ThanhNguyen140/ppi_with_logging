import click
from ppi.database import Database
from ppi.intact_analyzer import IntActAnalyzer

@click.group()
def main():
    pass

@main.command()
@click.option(
    "-p",
    "--path",
    help="Path to the data.tsv",
)
def bcentrality(path:str):
    """Export information of node that has the highest betweenness centrality

    Args:
        path (str): Path to data.tsv
    """
    db = Database()
    db.set_path_to_data_file(path)
    db.read_data()
    db.import_data()
    g = db.get_graph()
    iaa = IntActAnalyzer(g)
    bc = iaa.get_protein_with_highest_bc()
    for key,value in bc.items():
        click.echo(f"{key} : {value}")

@main.command()
@click.option(
    "-p",
    "--path",
    help="Path to the data.tsv",
)   
def number_of_nodes(path:str):
    """Export information of node that has the highest betweenness centrality

    Args:
        path (str): Path to data.tsv
    """
    db = Database()
    db.set_path_to_data_file(path)
    db.read_data()
    db.import_data()
    g = db.get_graph()
    num = db.count_nodes(g)
    click.echo(f"Number of nodes:{num}")
