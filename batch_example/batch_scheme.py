from py2neo.ogm import GraphObject, Property, RelatedFrom, RelatedTo


class Disease(GraphObject):
    __primarykey__ = "name"

    name = Property()
    description = Property()
    prevent = Property()
    cause = Property()
    susceptible = Property()
    cure_duration = Property()
    method = Property()
    cause_prob = Property()
    cured_prob = Property()

    recommend_food = RelatedTo("Food", "RECOMMEND_EAT")
    donot_eat_food = RelatedTo("Food", "DONOT_EAT")
    do_eat_food = RelatedTo("Food", "DO_EAT")
    recommend_drugs = RelatedTo("Drug", "RECOMMEND_DRUG")
    common_drugs = RelatedTo("Drug", "COMMON_DRUG")
    examines = RelatedTo("Examination", "NEED_EXAMINE")
    department = RelatedTo("Department", "BELONGS_TO")
    acompanies = RelatedTo("Disease", "ACOMPANY_WITH")
    symptoms = RelatedTo("Symptom", "HAS_SYMPTOM")


class Department(GraphObject):
    __primarykey__ = "name"

    name = Property()

    category = RelatedTo("Department", "BELONGS_TO")


class Producer(GraphObject):
    __primarykey__ = "name"

    name = Property()

    drugs = RelatedFrom("Drug", "PRODUCED_BY")


class Drug(GraphObject):
    __primarykey__ = "name"

    name = Property()


class Symptom(GraphObject):
    __primarykey__ = "name"

    name = Property()


class Examination(GraphObject):
    __primarykey__ = "name"

    name = Property()


class Food(GraphObject):
    __primarykey__ = "name"

    name = Property()
