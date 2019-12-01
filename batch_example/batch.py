from dataclasses import dataclass
from pnlp import piop

from py2neo.database import Graph
from batch_scheme import (Disease, Department, Producer,
                          Symptom, Examination, Food, Drug)


graph = Graph(scheme="bolt", host="localhost", port=7687,
              user="neo4j", password="password")


def create_node(item: dict):
    """
    One item of the given data to a node.
    """
    dise = Disease()
    dise.name = item.get("name", "")
    dise.description = item.get("desc", "")
    dise.prevent = item.get("prevent", "")
    dise.cause = item.get("cause", "")
    dise.susceptible = item.get("easy_get", "")
    dise.cause_prob = item.get("get_prob", "")
    dise.cured_prob = item.get("ured_prob", "")
    dise.method = item.get("cure_way", "")
    dise.cure_duration = item.get("cure_lasttime", "")
    return dise


@dataclass
class RelationUpdate:
    """
    One item of the given data to a series of relationships.
    """
    item: dict

    def __post_init__(self):
        self.dise = Disease.match(graph, self.item["name"]).first()

    def update_symptom(self):
        for name in self.item.get("symptom", []):
            symptom = Symptom()
            symptom.name = name
            self.dise.symptoms.update(symptom, {"name": "症状"})

    def update_acompany(self):
        for name in self.item.get("acompany", []):
            acompany_dise = Disease.match(graph, name).first()
            if acompany_dise:
                acompany = acompany_dise
            else:
                acompany = Disease()
                acompany.name = name
            self.dise.acompanies.update(acompany, {"name": "并发症"})

    def update_department(self):
        departs = self.item.get("cure_department", [])
        if len(departs) == 1:
            depart = Department()
            depart.name = departs[0]
            self.dise.department.update(depart, {"name": "所属科室"})
        elif len(departs) == 2:
            depart = Department()
            cate = Department()
            cate.name, depart.name = departs
            depart.category.update(cate, {"name": "所属类别"})
            self.dise.department.update(depart, {"name": "所属科室"})

    def update_examine(self):
        for name in self.item.get("check", []):
            examine = Examination()
            examine.name = name
            self.dise.examines.update(examine, {"name": "需要检查"})

    def update_common_drug(self):
        for name in self.item.get("common_drug", []):
            drug = Drug()
            drug.name = name
            self.dise.common_drugs.update(drug, {"name": "常用药"})

    def update_recommend_drug(self):
        for name in self.item.get("recommand_drug", []):
            drug = Drug()
            drug.name = name
            self.dise.recommend_drugs.update(drug, {"name": "推荐药"})

    def update_do_eat_food(self):
        for name in self.item.get("do_eat", []):
            food = Food()
            food.name = name
            self.dise.do_eat_food.update(food, {"name": "宜吃"})

    def update_donot_eat_food(self):
        for name in self.item.get("not_eat", []):
            food = Food()
            food.name = name
            self.dise.donot_eat_food.update(food, {"name": "忌吃"})

    def update_recommend_food(self):
        for name in self.item.get("recommand_eat", []):
            food = Food()
            food.name = name
            self.dise.recommend_food.update(food, {"name": "推荐吃"})

    def update_drug_producer(self):
        for cont in self.item.get("drug_detail", []):
            producer = Producer()
            drug = Drug()
            producer.name = cont.split('(')[0]
            drug.name = cont.split('(')[-1].replace(')', '')
            producer.drugs.update(drug, {"name": "生产厂商"})
            graph.push(producer)

    def update_all(self):
        self.update_acompany()
        self.update_symptom()
        self.update_department()
        self.update_examine()
        self.update_common_drug()
        self.update_recommend_drug()
        self.update_do_eat_food()
        self.update_donot_eat_food()
        self.update_recommend_food()
        self.update_drug_producer()
        graph.push(self.dise)


def batch_process(batch_file: str):
    data = piop.read_json(batch_file)
    for item in data:
        node = create_node(item)
        graph.push(node)
    for item in data:
        rlt = RelationUpdate(item)
        rlt.update_all()


if __name__ == '__main__':
    # data.json: 44111 nodes, 290998 relationships
    batch_process("small.json")
