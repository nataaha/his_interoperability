# -*- coding: utf-8 -*-
# stdlib
import json,bunch
import bson as bs
# Zato
from zato.server.service import Service

# For type hints
if 0:
    from pymongo import MongoClient
    from pymongo.collection import Collection

    Collection = Collection
    MongoClient = MongoClient

class ManageDataStore(Service):
    class SimpleIO:
        input_optional = ('measure','q')
        input_required = ('app','store')
        output_optional = ('storage')
    def handle_GET(self):
        # default store, app
        store = 'data'
        app = 'app'
        query={}

        # Get a handle to a MongoDB client object
        conn = self.out.mongodb.get('ALKU MongoDB').conn.client # type: MongoClient
        if self.request.input.app is not None:
            app = self.request.input.app
            if self.request.input.store is not None:
                store = self.request.input.store

        # Select a database and collection
        db = conn[app]
        collection = db[store] # type: Collection
        insertResult = collection.insert_one({
            'location': []
        })
        # Read a document from the database
        if self.request.input.q is not None:
            if self.is_valid_json(self.request.input.q):
                query = self.request.input.q
        result = self.findData(collection=collection,query=query)

        # Print out the document read in
        self.response.payload = bs.json_util.dumps({store:result})
    def findData(self,collection=None,query=None):
        if query is not None:
            res= collection.find(filter=json.loads(query))
        else:
            res= collection.find()
        return res

    def is_valid_json(self,myjson):
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            return False
        return True



