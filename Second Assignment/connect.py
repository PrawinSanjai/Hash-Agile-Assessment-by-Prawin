from elasticsearch import Elasticsearch


es = Elasticsearch(
    cloud_id="4d7bcea6a7d948dcb62138cba31985ab:dXMtY2VudHJhbDEuZ2NxMjQ5N2U5MzQ5NjU5OWMxNmM0N2UxMTA4MWU4JGQyYjVlOWM0MTY5NjRlZGJhMTE3YzI1MWEwNTE4NGI4",
    api_key="U3JnSVVKSUJldjNVNkFaUk5BYW86UlM1Wi1zVnp2dw=="
)


if es.ping():
    print("Connected to Elasticsearch Cloud")
else:
    print("Connection failed")
