####################################################
# neomodel models to reprsent the LOS data in Neo4J
####################################################

from multiprocessing.dummy import Array
from neomodel import (ArrayProperty, RelationshipFrom, RelationshipTo,
                      StringProperty, IntegerProperty, StructuredNode)


class Visit(StructuredNode):
    visit_id = StringProperty(unique_index=True)
    embedding = ArrayProperty()

    sex = StringProperty()
    sex_encoded = ArrayProperty()
    race = StringProperty()
    race_encoded = ArrayProperty()
    age = StringProperty()
    age_encoded = ArrayProperty()
    
    care_site = RelationshipTo("CareSite", "visit_site")
    dx = RelationshipTo("Diagnosis", "has_medical_hx")


class Diagnosis(StructuredNode):
    icd = StringProperty(unique_index=True)
    embedding = ArrayProperty()

    sex_encoded = ArrayProperty()
    race_encoded = ArrayProperty()
    age_encoded = ArrayProperty()

    child_dx = RelationshipFrom("Diagnosis", "has_parent_dx")
    parent_dx = RelationshipTo("Diagnosis", "has_parent_dx")
    visits = RelationshipFrom("Visit", "has_medical_hx")


# class Age(StructuredNode):
#     label = StringProperty(unique_index=True)
#     embedding = ArrayProperty()

#     visits = RelationshipFrom("Visit", "age_at_visit")


# class Race(StructuredNode):
#     label = StringProperty(unique_index=True)
#     embedding = ArrayProperty()

#     visits = RelationshipFrom("Visit", "visit_race")


# class Sex(StructuredNode):
#     label = StringProperty(unique_index=True)
#     embedding = ArrayProperty()

#     visits = RelationshipFrom("Visit", "of_sex")


class CareSite(StructuredNode):
    site_id = StringProperty(unique_index=True)
    embedding = ArrayProperty()

    sex_encoded = ArrayProperty()
    race_encoded = ArrayProperty()
    age_encoded = ArrayProperty()

    visits = RelationshipFrom("Visit", "visit_site")
