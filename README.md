# Homework week 8
# **ppi**
## **Author:** Phuong Thanh Nguyen

Table of content
- [Homework week 8](#homework-week-8)
- [**ppi**](#ppi)
  - [**Author:** Phuong Thanh Nguyen](#author-phuong-thanh-nguyen)
    - [1. Summary](#1-summary)
    - [2. Objective](#2-objective)
    - [3. Materials and methods](#3-materials-and-methods)

### 1. Summary
- This project aims at building package to work with protein interaction database and plotting graphs to perform interactions. 
- Logging and click are also used in the exercise.

### 2. Objective
The package provides tools to read .tsv file and produce protein as well as interaction table in SQL database. Then a graph can be plotted based on interactions between protein databases. The module also allows filtering for certain interaction information. 

### 3. Materials and methods
 1. src: contains [database.py](./src/ppi/database.py) file and [intact_analyzer.py](./src/ppi/intact_analyzer.py).[cli.py](./src/ppi/cli.py) file allows click function for the easy use of the package.
 2. tests: contains [test_database.py](./tests/test_database.py) file and [test_inact_analyzer.py](./tests/test_inact_analyzer.py)
 3. notebook: 
1. [test_ppi.ipynb](./notebooks/test_ppi.ipynb) file to demonstrate the execution of the ppi package on [test_ppi.tsv](./tests/data/test_ppi.tsv)
2. [ppi.ipynb](./notebooks/ppi.ipynb) file to demonstrate the execution of the ppi package on [ppi.zip](./data/ppi.zip)
  
