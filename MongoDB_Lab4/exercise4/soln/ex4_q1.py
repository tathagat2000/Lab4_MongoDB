from pymongo import MongoClient
from pprint import pprint
client=MongoClient("mongodb+srv://201701053:201701053@nosql-qrc3n.mongodb.net/201701053?retryWrites=true&w=majority")
db=client["201701053"]


t = list(db.accounts.find(
            {"products" : {"$in" : ["InvestmentStock"]}}
        ))
db.xyz.drop()
db.xyz.insert_many(t)

#print(db.collection_names())
agr = [#{"$unwind" : {"path" : "$accounts"}},
       {"$lookup" : {
            "from" : "xyz",
            "localField" : "accounts",
            "foreignField" : "account_id",
            "as" : "output"
        }},
        {"$project" : {"accounts" : 1, "username" : 1, "name" : 1, "email" : 1,"_id" : 0}}
       ]

val = list(db.customers.aggregate(agr))
db.xyz.drop()
for v in val:
    pprint(v)
