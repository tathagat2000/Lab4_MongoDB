from pymongo import MongoClient
from pprint import pprint
client=MongoClient("mongodb+srv://201701053:201701053@nosql-qrc3n.mongodb.net/201701053?retryWrites=true&w=majority")
db=client["201701053"]

agr = [
       {"$lookup" : {
            "from" : "accounts",
            "localField" : "accounts",
            "foreignField" : "account_id",
            "as" : "output"
        }},
       {"$unwind":"$output"},
        {"$match":{"output.products":"Commodity"}},
        {"$group":{"_id": "$username","avgAmount":  {"$avg": "$output.limit"}}}
       ]

val = db.customers.aggregate(agr)

for v in val:
    pprint(v)
