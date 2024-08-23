from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime

client = bigquery.Client()
with open('tabel.txt', 'r') as file:
    tabel_list = [line.strip() for line in file]
    
storage_client = storage.Client()
bucket_name = 'pelerninja'
bucket = storage_client.bucket(bucket_name)

query_template = """
TEMPEL QUERY ANDA DI SINI
"""

for tabel in tabel_list:
    query = query_template.format(tabel=tabel)
    job_config = bigquery.QueryJobConfig()
    query_job = client.query(query, job_config=job_config)


    query_job.result()


    current_time = datetime.now().strftime("%Y.%m.%d.%H.%M")
    file_name = f"{tabel}_{current_time}.csv"
    destination_uri = f"gs://{bucket_name}/weybek/{file_name}"

    extract_job_config = bigquery.job.ExtractJobConfig()
    extract_job_config.destination_format = bigquery.DestinationFormat.CSV
    extract_job_config.field_delimiter = ","
    extract_job_config.print_header = False

    extract_job = client.extract_table(
        query_job.destination, destination_uri, job_config=extract_job_config
    )

    extract_job.result()

    print(f"Query for table {tabel} completed and results saved to {destination_uri}")
