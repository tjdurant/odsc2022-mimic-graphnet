####################################################
# Function to train GraphSAGE using GDS library
####################################################

def train(gds, database, lr=0.1, seed=42):
    # Remove existing embeddings if they exist
    print("Removing exisitng embeddings and model")
    gds.run_cypher("MATCH (a) REMOVE a.embedding")

    # Now check if the projection exists, if it does
    # delete it
    exists = bool(gds.graph.exists(f"mimic-{database}")["exists"])
    if exists:
        G = gds.graph.get(f"mimic-{database}")
        gds.graph.drop(G)

    try: # And the same for prior GraphSAGE models
        model = gds.model.get(f"mimicModel-{database}")
        if model.exists():
            model.drop()
    except Exception as ex:
        pass

    # Recreate the projection and include computed properties
    print("Creating projected graph")

    # Not the most elegant approach, but easy try/catch to handle either the
    # graph with ICD9 hierarchy or the one without (no "has_parent_dx" relationship)
    G = None
    try:
        G, _ = gds.graph.project(
            f"mimic-{database}",
            #['Visit', 'Sex', 'Race', 'Diagnosis', 'CareSite', 'Age'],
            ['Visit', 'Diagnosis', 'CareSite'],
            #['age_at_visit', 'has_medical_hx', 'has_parent_dx', 'of_sex', 'visit_race', 'visit_site'],
            ['has_medical_hx', 'has_parent_dx', 'visit_site'],
            #nodeProperties=["degree", "age_encoded", "race_encoded", "sex_encoded"]
            nodeProperties=["degree"]
        )
        print("Training with dx relationships...")
    except Exception as ex:
        G, _ = gds.graph.project(
            f"mimic-{database}",
            #['Visit', 'Sex', 'Race', 'Diagnosis', 'CareSite', 'Age'],
            ['Visit', 'Diagnosis', 'CareSite'],
            #['age_at_visit', 'has_medical_hx', 'of_sex', 'visit_race', 'visit_site'],
            ['has_medical_hx', 'visit_site'],
            #nodeProperties=["degree", "age_encoded", "race_encoded", "sex_encoded"]
            nodeProperties=["degree"]
        )
        print("Training with NO dx relationships...")

    # Train GraphSAGE using GDS. Many other parameters that can be
    # tuned in GraphSAGE - using defaults except for learning rate and
    # number of epochs. Use the computed degree property from 
    # add_graph_properties.py as the feature property.
    print("Training GraphSAGE")
    model, _ = gds.beta.graphSage.train(
        G,
        modelName = f"mimicModel-{database}",
        learningRate = lr,
        epochs = 200,
        searchDepth = 40,
        randomSeed=seed,
        #featureProperties = ["degree", "age_encoded", "race_encoded", "sex_encoded"]
        featureProperties = ["degree"]
    )

    # Print metrics and write individual node embeddings back to Neo4J for later use
    print(model.metrics())

    gds.beta.graphSage.write(
        G,
        writeProperty='embedding',
        modelName=f"mimicModel-{database}"
    )

    return model.metrics()