import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


# Returns all documents from collection capped by batch size
def get_all_documents(collection_name):
    return db.collection(collection_name).get()


# Returns specific documents
def get_specific_documents(collection_name, filters=[]):
    docs = db.collection(collection_name)
    for filter in filters:
        docs = docs.where(str(filter.name), "==", str(filter.value))
    return docs.get()


# Batch delete documents in a collection
def batch_delete(docs):
    batch = db.batch()
    for doc in docs:
        batch.delete(doc.reference)
    batch.commit()


# Batch write data in a collection
def batch_write(data, collection_name):
    batch = db.batch()
    for document_data in data:
        batch.set(db.collection(collection_name).document(), document_data)
    batch.commit()
    return


cred = credentials.Certificate(PATH_TO_FIRESTORE_CREDENTIALS)
app = firebase_admin.initialize_app(cred)
db = firestore.client()
