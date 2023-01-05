from firestore import batch_delete, batch_write, get_all_documents
import logging


# Firestore limits batch transactions to 500. Hence, if the number of operations are
# greater than 500, multiple batches are created.
def write_user_data(data, collection_name):
    docs = get_all_documents(collection_name)

    if len(data) <= 500 and len(docs) <= 500:
        try:
            batch_delete(docs)
            logging.info("Documents deleted!")
        except:
            logging.error("Documents could not be deleted.")
        else:
            try:
                batch_write(data, collection_name)
                print("Documents inserted!")
            except:
                logging.error("Documents could not be inserted.")

    else:
        total_number_of_data_batches = len(
            data) / 500 if (len(data) % 500 == 0) else len(data) // 500 + 1
        logging.info("Total number of batches of data:",
                     total_number_of_data_batches)

        total_number_of_doc_batches = len(
            docs) / 500 if (len(docs) % 500 == 0) else len(docs) // 500 + 1
        logging.info("Total number of batches of documents:",
                     total_number_of_doc_batches)

        for batch_count in range(total_number_of_doc_batches):
            for i in range(0, batch_count, 500):
                try:
                    batch_delete(data[i:i+500])
                except:
                    logging.error(
                        "Documents could not be deleted for batch", batch_count)
                else:
                    print("Deletion finished batch:", batch_count)

        for batch_count in range(total_number_of_data_batches):
            for i in range(0, batch_count, 500):
                try:
                    batch_write(data[i:i+500], collection_name)
                except:
                    logging.error(
                        "Documents could not be inserted for batch", batch_count)
                else:
                    print("Insertion finished batch:", batch_count)
