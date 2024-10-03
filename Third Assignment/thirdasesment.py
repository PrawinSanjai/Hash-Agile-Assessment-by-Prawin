from elasticsearch import Elasticsearch
import pandas as pd

es = Elasticsearch(
    cloud_id="4d7bcea6a7d948dcb62138cba31985ab:dXMtY2Nsb3VkLmVzLmlvJDE0MDMxMjQ5N2U5MzQ5NjU5OWMxNmM0N2UxMTA4MWU4JGQyYjVlOWM0MTY5NjRlZGJhMTE3YzI1MWEwNTE4NGI4",
    api_key="U3JnSVVKSUJldjNVNkFaUk5BY0WjlTd2U4UlM1Wi1zVnp2dw=="
)


def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

def indexData(p_collection_name, p_exclude_column, data):
    data = data.where(pd.notnull(data), None)
    
    for _, row in data.iterrows():
        employee_doc = {key: row[key] for key in row.keys() if key != p_exclude_column}
        try:
            res = es.index(index=p_collection_name, body=employee_doc)
            print(f"Indexed: {res['result']}")
        except Exception as e:
            print(f"Failed to index document: {e}")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {p_column_name: p_column_value}
        }
    }
    res = es.search(index=p_collection_name, body=query)
    print(f"Found {res['hits']['total']['value']} records:")
    for hit in res['hits']['hits']:
        print(hit['_source'])

def getEmpCount(p_collection_name):
    res = es.count(index=p_collection_name)
    print(f"Total employees in {p_collection_name}: {res['count']}")

def delEmpById(p_collection_name, p_employee_id):
    query = {
        "query": {
            "match": {"Employee ID": p_employee_id}
        }
    }
    res = es.delete_by_query(index=p_collection_name, body=query)
    print(f"Deleted {res['deleted']} employee(s) with ID {p_employee_id}")

def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    res = es.search(index=p_collection_name, body=query)
    print(f"Department counts in {p_collection_name}:")
    for bucket in res['aggregations']['department_count']['buckets']:
        print(f"{bucket['key']}: {bucket['doc_count']} employees")

#Var v_nameCollection
v_nameCollection = 'hash_prawin'  

#Var v_phoneCollection
v_phoneCollection = 'hash_2350'   

#create_namecollection
createCollection(v_nameCollection)

#create_phoneCollection
createCollection(v_phoneCollection)

csv_file_path = "D:\Prawin\Programming\employee.csv"  
df = pd.read_csv(csv_file_path)  

#Get employee count
getEmpCount(v_nameCollection)

#IndexData Name
indexData(v_nameCollection, 'Department', df)

#IndexData Phone
indexData(v_phoneCollection, 'Gender', df)

#Delete employee by ID
delEmpById(v_nameCollection, 'E02003')

#Get employee count
getEmpCount(v_nameCollection)

#IT Department Name
searchByColumn(v_nameCollection, 'Department', 'IT')

#Gender Male
searchByColumn(v_nameCollection, 'Gender', 'Male')

#IT Department Phone
searchByColumn(v_phoneCollection, 'Department', 'IT')

#name DepFacet
getDepFacet(v_nameCollection)

#phone DepFacet
getDepFacet(v_phoneCollection)

