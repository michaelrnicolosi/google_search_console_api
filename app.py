from flask import Flask
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
import json
import pandas as pd
from google.cloud import bigquery


app = Flask(__name__)

PROPERTIES = ["sc-domain:domain1.com", "sc-domain:domain2.com"]
BQ_DATASET_NAME = '<DATASET-NAME>'
BQ_TABLE_NAME = '<TABLE-NAME>'
SERVICE_ACCOUNT_FILE = '<SERVICE-ACCOUNT-FILE>'

SCOPES = ['https://www.googleapis.com/auth/webmasters']
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build(
    'searchconsole',
    'v1',
    credentials=credentials
)


@app.route("/")
def search_console():

    request = {
    'startDate': '2022-08-21',
    'endDate': '2022-08-21',
    'dimensions': ['query', 'date'],
        'dimensionFilterGroups': [{
        'filters': [{
            'dimension': 'query',
            'operator': 'INCLUDING_REGEX',
            'expression': 'some query'
        }]
    }],
    'rowLimit': 25000,
    'startRow': 0
    }

    site_url = 'sc-domain:domain1'
    response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()

    if len(response) > 1:

        x = response['rows']

        df = pd.DataFrame.from_dict(x)
        df[['query', 'date']] = pd.DataFrame(df['keys'].values.tolist(), index= df.index)
        result = df.drop(['keys'],axis=1)
        result['website'] = site_url
    
        client = bigquery.Client.from_service_account_json(DEST_SERVICE)
        dataset_id = BQ_DATASET_NAME
        table_name = BQ_TABLE_NAME
        job_config = bigquery.LoadJobConfig()
        table_ref = client.dataset(dataset_id).table(table_name)
        job_config.write_disposition = 'WRITE_APPEND'

        load_job = client.load_table_from_dataframe(result, table_ref, job_config=job_config)
        load_job.result()


    return "Hello {}!".format(name)


if __name__ == "__main__":
    app.run(debug=True)
