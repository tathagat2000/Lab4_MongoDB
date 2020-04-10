from pprint import pprint
#use pprint instead of print to clearly print output documents
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure,OperationFailure
#client=MongoClient("mongodb+srv://201701053:201701053@nosql-qrc3n.mongodb.net/201701053?retryWrites=true&w=majority")
#db=client["201701053"]
try:
    client.admin.command('ismaster')

except ConnectionFailure:
    print('Server not available')

except OperationFailure:
    print('wrong credentials')

else:
    print('connected to database')
    casa=db.Sales.aggregate([{ "$group": { "_id": "$storeLocation", "total": { "$sum":1 } } }])


finally:
	client.close()

for docs in casa:
    print(docs)
