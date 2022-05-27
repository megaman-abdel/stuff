import uuid

from math import ceil

from google.cloud import bigquery as bq
from google.cloud import storage as gcs

class BigQueryWrapper:
    def __init__(self, project, client=None):
        client = client or bq.Client(project=project)
    def query(self, query_str):
        query_job = self.client.query(query_str)
        return query_job.result()

class GCSWrapper:
    def __init__(self, project, client=None):
        client = client or gcs.Client(project=project)
    
    def delete_blobs(self, blobs):
        for blob in blobs:
            blob.delete()
        return
    
    def get_blobs_list(self, bucket_name, suffix, prefix="", project=None):
        client = gcs.Client(project=project)
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        res = []

        for blob in blobs:
            if blob.name.endswith(suffix):
                res.append(blob)
        return
    
    def compose_shards(self, source_bucket, suffix, prefix="", project=None):
        bucket = self.client.bucket(source_bucket)
        blobs_to_compose = self.get_blobs_list(source_bucket, suffix=suffix, prefix=prefix, project=project)

        curr_total_blobs = len(blobs_to_compose)

        while curr_total_blobs > 1:
            for i in range(ceil(curr_total_blobs/32)):
                start = i*32
                end = min((i+1)*32, curr_total_blobs)
                bucket.blob(prefix+"/"+str(uuid.uuid4())+suffix).compose(blobs_to_compose[start:end])
                self.delete_blobs(blobs_to_compose[start:end])
            blobs_to_compose = self.get_blobs_list(source_bucket, suffix, prefix=prefix, project=project)
            curr_total_blobs = len(blobs_to_compose)
        return
