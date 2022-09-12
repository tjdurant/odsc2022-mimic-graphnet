# Repository Overview

The code in this repository can be used to generate synthetic data or use appropriately formatted data from MIMIC III to populate a graph network in Neo4J and train/evaluate a random forest model for length-of-stay (LOS) prediction. Additional background information can be found in the slides from the Open Data Science Conference (ODSC) Europe 2022 presentation "Healthcare Predictive Modeling with Graph Networks". The slides are in the 'docs' directory and code is in the 'src' directory.

# Running the Code

To generate, load, and evaluate data, first create a Python 3.9 virtual enivornment and install the needed libraries from requirements.txt. An accessible Neo4J database is a requirement. Execute the scripts from the parent directory to generate data and run the driver application.

# Neo4J Database

To spin up a Neo4J database, the following command can be run. This command is based on the getting started tutorial found [here](https://neo4j.com/developer/docker-run-neo4j/#:~:text=Let%20us%20go%20ahead%20and%20create%20our%20Neo4j%20container%20by%20running%20the%20command%20below.%20An%20explanation%20of%20each%20option%20is%20in%20the%20following%20paragraphs.) Port 7474 can be used to access the Neo4J browser where you can login with the username:`neo4j` and password:`test` supplied below.

```
docker run \ 
    --name testneo4j \
    -p7474:7474 -p7687:7687 \
    -d \
    -v <path_to_data>:/data \
    -v <some_path>/neo4j/logs:/logs \
    -v <some_path>/neo4j/import:/var/lib/neo4j/import \
    -v <some_path>/neo4j/plugins:/plugins \
    --env NEO4J_AUTH=neo4j/test \
    neo4j:4.4.10
```

## Generate data
```
python src/generate_data.py
```

## Load data into Neo4J, train and evaluate models
```
python src/driver.py
```

The script will prompt for the CSV input file (from the generated output or equivalently formatted data), Neo4J connection parameters, and what step of the workflow is being run: load data, compute properties, train GraphSAGE, or train/evaluate a random forest based on either tabular or embedded data. Additional details can be found in the code comments.
