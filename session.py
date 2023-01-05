from firestore import get_specific_documents, batch_delete_sessions, batch_write
import logging


# Firestore limits batch transactions to 500. Hence, if the number of operations are
# greater than 500, multiple batches are created.
def write_session_data(data, collection_name, group_name, filters):
    docs = get_specific_documents(collection_name, filters)

    if len(data) <= 500 and len(docs) <= 500:
        try:
            batch_delete_sessions(collection_name, filters)
            logging.info("Sessions deleted!")
        except:
            logging.error("Sessions could not be deleted.")
        else:
            try:
                batch_write(data, collection_name)
                print("Sessions inserted!")
            except:
                logging.error("Sessions could not be inserted.")

    else:
        total_number_of_batches = len(
            data) / 500 if (len(data) % 500 == 0) else len(data) // 500 + 1
        print("Total number of batches:", total_number_of_batches)

        for batch_count in range(total_number_of_batches):
            print("Deletion started batch:", batch_count)
            for i in range(0, batch_count, 500):
                try:
                    batch_delete_sessions(
                        data[i:i+500], collection_name, group_name)
                except:
                    logging.error(
                        "Sessions could not be deleted for batch", batch_count)
                else:
                    print("Deletion finished batch:", batch_count)

        for batch_count in range(total_number_of_batches):
            print("Insertion started batch:", batch_count)
            for i in range(0, batch_count, 500):
                try:
                    batch_write(data[i:i+500], collection_name)
                except:
                    logging.error(
                        "Sessions could not be inserted for batch", batch_count)
                else:
                    print("Insertion finished batch:", batch_count)
