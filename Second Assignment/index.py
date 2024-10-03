import pandas as pd
from elasticsearch import Elasticsearch

es = Elasticsearch(
    cloud_id="4d7bcea6a7d948dcb6uZ2NwLmNsb3VkLmVzLmlvJDE0MDMxMjQ5N2U5MzQ5NjU5OWMxNmM0N2UxMTA4MWU4JGQyYjVlOWM0MTY5NjRlZGJhMTE3YzI1MWEwNTE4NGI4",
    api_key="U3JnSVVKSUJldjNd0WjlTd2U4UlM1Wi1zVnp2dw=="
)


csv_file_path = "D:\Prawin\Programming\employee.csv" 

df = pd.read_csv(csv_file_path, encoding='latin1')

index_name = "employee_data"

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)
    print(f"Index '{index_name}' created successfully.")
else:
    print(f"Index '{index_name}' already exists.")

for _, row in df.iterrows():
    employee_doc = {
        "Employee_ID": row["Employee ID"],
        "Full_Name": row["Full Name"],
        "Job_Title": row["Job Title"],
        "Department": row["Department"],
        "Business_Unit": row["Business Unit"],
        "Gender": row["Gender"],
        "Ethnicity": row["Ethnicity"],
        "Age": row["Age"],
        "Hire_Date": row["Hire Date"],
        "Annual_Salary": row["Annual Salary"]
    }

    res = es.index(index=index_name, body=employee_doc)
    print(f"Indexed employee ID {employee_doc['Employee_ID']}: {res['result']}")

print("Data indexing completed.")
