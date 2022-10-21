####################################################
# Load formatted data from CSV into Neo4J using OGM
# (neomodel). Individiuals models defined in:
# models.simple_graph
####################################################

import json
import os

from models.simple_graph import *
from tqdm import tqdm
from neomodel import db
from sklearn.preprocessing import OneHotEncoder
import numpy as np


class SimpleGraph:
    def run_import(self, csv_path, import_dx_rel = False):

        first_column_of_lab_data = 9 # beginning of labs column

        try:
            if import_dx_rel:
                if not os.path.exists("resources/icd9_categories.json"):
                    # https://raw.githubusercontent.com/sirrice/icd9/master/codes_pretty_printed.json
                    # and save as resources/icd9_categories.json
                    print("Please download ICD9 diagnosis and group list per code comments")
                    exit()
                    
                with open('resources/icd9_categories.json') as icd_file:
                    data = json.load(icd_file)
                    for record_group in tqdm(data, total=len(data), desc="Loading dx data..."):
                        last = None
                        for record in record_group:
                            if record['code'] is None or len(record['code']) < 1:
                                continue

                            cur_icd = record['code'].replace('.', '')
                            cur_dx = Diagnosis.get_or_create(
                                {
                                    "icd": cur_icd
                                }
                            )

                            if last is not None:
                                cur_dx[0].parent_dx.connect(last[0])
                            last = cur_dx

            # Count number of lines for use in TQDM
            num_lines = 0
            with open(csv_path) as mimic_data:
                for line in mimic_data:
                    num_lines += 1

            diagnosis_counts = {}
            top_x_diagnoses = []
            # Reopen file for processing
            with open(csv_path) as mimic_data:
                header = []

                i = 0
                for line in tqdm(mimic_data, total=num_lines, desc="Loading data..."):
                    i += 1
                    # Strip newlines, remote quotes from strings, and split CSV
                    entry = line.strip().replace('"', "").split(",")
                    
                    # Get field names from header
                    if i == 1:
                        header = entry

                        
                        lab_node_dict = {}
                        li = first_column_of_lab_data # set lab column index
                        # create dict of lab ITEMIDs and lab names with column index as key
                        for h in header[li:]:
                            lab_node_dict[li] = h
                            li+=1

                        continue # Since header, now move on to read/process data
                    
                    # TODO: add back connection to diagnosis codes 
                    # TODO: may also have to link dx codes back to visit codes 
                    diagnoses = entry[7].split(";")
                    for diagnosis in diagnoses:
                        if diagnosis not in diagnosis_counts:
                            diagnosis_counts[diagnosis] = 0
                        diagnosis_counts[diagnosis] += 1

            dict(sorted(diagnosis_counts.items(), key=lambda item: item[1]), reversed=True)
            i = 0
            for k,v in diagnosis_counts.items():
                i += 1
                if i > 101:
                    break
                top_x_diagnoses.append(k)

            # Reopen file for processing
            with open(csv_path) as mimic_data:
                header = []

                i = 0
                for line in tqdm(mimic_data, total=num_lines, desc="Loading data..."):
                    
                    tmp_lab_dict = {}

                    i += 1
                    # Strip newlines, remote quotes from strings, and split CSV
                    entry = line.strip().replace('"', "").split(",")
                    
                    # Get field names from header
                    if i == 1:
                        header = entry
                        continue # Since header, now move on to read/process data
                    
                    # When a data row, get variables and add to Neo4J via neomodel
                    visit_id = entry[0]
                    subject_id = entry[1]
                    race = entry[2]
                    sex = entry[3]
                    age = float(entry[4])
                    care_site = entry[5]
                    diagnoses = entry[8].split(";")

                    age_category = None
                    if age < 35:
                        age_category = "18-34"
                    elif age < 51:
                        age_category = "35-50"
                    elif age < 66:
                        age_category = "51-65"
                    elif age < 81:
                        age_category = "56-80"
                    else:
                        age_category = ">=81"

                    care_site_node = CareSite.get_or_create(
                        {
                            "site_id": str(care_site).lower()
                        }
                    )

                    visit_node = Visit.create_or_update(
                        {
                            "visit_id": str(visit_id).lower(),
                            "age": age_category,
                            "race": race.lower(),
                            "sex": sex.lower()
                        }
                    )
                    
                    # Connect each of the data elements to the visit node
                    visit_node[0].care_site.connect(care_site_node[0])

                    # Add diagnosis nodes and connect to the visit
                    for diagnosis in diagnoses:
                        icd9 = diagnosis.replace(".", "")
                        #if diagnosis_counts[icd9] >= 50:
                        cur_dx = Diagnosis.get_or_create(
                                    {
                                        "icd": icd9
                                    }
                                )
                        visit_node[0].dx.connect(cur_dx[0])

                    
                    li = first_column_of_lab_data
                    # ! PLEASE NOTE: Laboratory values must be final left join when prepared in prepare_mimic_data.py for this block to work
                    # iterate through entry columns that hold laboratory values
                    for e in entry[li:]:
                        
                        # If there is no lab_mode entry, skip.
                        if e == '':
                            continue

                        # Convert lab_mode to int
                        tmp_lab_mode = int(float(e))

                        # append Within Normal Limits (wnl) or Abnormal (abn)
                        if tmp_lab_mode == 0:
                            tmp_lab_dict[lab_node_dict[li]] = lab_node_dict[li] + "_wnl"
                        if tmp_lab_mode  == 1:
                            tmp_lab_dict[lab_node_dict[li]] = lab_node_dict[li] + "_abn"
                    
                        # create lab node
                        cur_lx = Lab.get_or_create(
                            {
                                "lab_id": str(tmp_lab_dict[lab_node_dict[li]]).lower(),
                                "lab_name": tmp_lab_dict[lab_node_dict[li]].split("_")[1],
                                "lab_mode": tmp_lab_mode
                            }
                        )

                        # connect visit node to lab node
                        visit_node[0].lx.connect(cur_lx[0])

                        li+=1
                    

            
            sex_qry = "MATCH (n:Visit) RETURN COLLECT(DISTINCT n.sex) as propertyList"
            results, meta = db.cypher_query(sex_qry)
            values = results[0][0]

            sex_enc = OneHotEncoder(handle_unknown='ignore')
            sex_enc.fit(np.array(values).reshape(-1,1))

            age_qry = "MATCH (n:Visit) RETURN COLLECT(DISTINCT n.age) as propertyList"
            results, meta = db.cypher_query(age_qry)
            values = results[0][0]

            age_enc = OneHotEncoder(handle_unknown='ignore')
            age_enc.fit(np.array(values).reshape(-1,1))

            race_qry = "MATCH (n:Visit) RETURN COLLECT(DISTINCT n.race) as propertyList"
            results, meta = db.cypher_query(race_qry)
            values = results[0][0]

            race_enc = OneHotEncoder(handle_unknown='ignore')
            race_enc.fit(np.array(values).reshape(-1,1))

            visits = Visit.nodes.filter()
            for visit in visits:
                visit.sex_encoded = sex_enc.transform([[visit.sex]]).toarray()[0].tolist()
                visit.age_encoded = age_enc.transform([[visit.age]]).toarray()[0].tolist()
                visit.race_encoded = race_enc.transform([[visit.race]]).toarray()[0].tolist()
                visit.save()

            diagnosis_nodes = Diagnosis.nodes.filter()
            for diagnosis_node in diagnosis_nodes:
                diagnosis_node.sex_encoded = sex_enc.transform([["none"]]).toarray()[0].tolist()
                diagnosis_node.age_encoded = age_enc.transform([["none"]]).toarray()[0].tolist()
                diagnosis_node.race_encoded = race_enc.transform([["none"]]).toarray()[0].tolist()
                diagnosis_node.save()

            care_sites = CareSite.nodes.filter()
            for care_site in care_sites:
                care_site.sex_encoded = sex_enc.transform([["none"]]).toarray()[0].tolist()
                care_site.age_encoded = age_enc.transform([["none"]]).toarray()[0].tolist()
                care_site.race_encoded = race_enc.transform([["none"]]).toarray()[0].tolist()
                care_site.save()

        except Exception as ex:
            print("Here")
            print(ex)
