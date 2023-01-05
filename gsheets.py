from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import pandas as pd
import logging

STORAGE = file.Storage(PATH_TO_TOKEN_FILE)
SCOPE = 'https://www.googleapis.com/auth/spreadsheets.readonly'


# Authentication token created from client credentials (only for the first time)
def get_credentials():
    flow = client.flow_from_clientsecrets(
        PATH_TO_CLIENT_SECRET_FILE, scope=SCOPE)
    http = Http()
    credentials = tools.run_flow(flow, STORAGE, http=http)
    return credentials


# Get credentials from file
def get_credentials():
    credentials = STORAGE.get()
    return credentials


# Returns the google sheet
def get_google_sheet(spreadsheet_id, range_name):
    creds = get_credentials()
    service = build('sheets', 'v4', http=creds.authorize(Http()))
    gsheet = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    return gsheet


# Transforms google sheet data into a dataframe
def gsheet2df(gsheet):
    header = gsheet.get('values', [])[0]
    values = gsheet.get('values', [])[1:]
    if not values:
        logging.info("No data found.")
    else:
        all_data = []
        for col_id, col_name in enumerate(header):
            column_data = []
            for row in values:
                column_data.append(row[col_id])
            ds = pd.Series(data=column_data, name=col_name)
            all_data.append(ds)
        df = pd.concat(all_data, axis=1)
        return df


# Converts data frame into a dictionary
def build_data_object(df):
    return df.to_dict('records')
