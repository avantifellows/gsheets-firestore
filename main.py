from gsheets import get_google_sheet, gsheet2df, build_data_object
from user import write_user_data
from session import write_session_data
from mapping import mapping
import logging


# Main lambda function
def lambda_handler(event, context):
    if "group_name" in event and mapping[event["group_name"]]:
        SPREADSHEET_ID = mapping[event["group_name"]]["spreadsheet_id"]
        RANGE_NAME = mapping[event["group_name"]]["range_name"]
        COLLECTION_NAME = mapping[event["group_name"]]["collection_name"]

        logging.info("Processing sheet...")

        gsheet = get_google_sheet(SPREADSHEET_ID, RANGE_NAME)
        df = gsheet2df(gsheet)
        logging.info("Sheet size=", df.shape)

        data = build_data_object(df)
        if event["type"] == "user":
            write_user_data(data, COLLECTION_NAME)
        elif event["type"] == "session":
            write_session_data(
                data, "Sessions", COLLECTION_NAME, event["filters"])
        else:
            logging.error("Event type is not valid.")
    else:
        logging.error("Not enough information in request!")
