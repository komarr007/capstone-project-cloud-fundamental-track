from fastapi import FastAPI
from azure.storage.blob import BlobServiceClient, __version__
from azure.core.exceptions import ResourceNotFoundError
import json

app = FastAPI()

@app.get("/metadata")
def get_meta(container_name):
    try:
            connect_str = 'BlobEndpoint=https://clinicdumpmemory.blob.core.windows.net/;QueueEndpoint=https://clinicdumpmemory.queue.core.windows.net/;FileEndpoint=https://clinicdumpmemory.file.core.windows.net/;TableEndpoint=https://clinicdumpmemory.table.core.windows.net/;SharedAccessSignature=sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2021-12-11T12:28:40Z&st=2021-11-14T04:28:40Z&spr=https&sig=aewhtO2kZiyAsNe7YH0AzhTk73K4FKmNH1v0iOxMEvY%3D'
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            container_client = blob_service_client.get_container_client(container_name)
            blobTampungan = []
            metaTampungan = []
            tampunganjadi = []
            for blob in container_client.list_blobs():
                blobTampungan.append(blob.name)
            for i in blobTampungan:
                blob_client = blob_service_client.get_blob_client(blob=i, container=container_name)
                properties = blob_client.get_blob_properties().metadata
                metaTampungan.append(properties)
            for _ in metaTampungan:
                formatmeta = {"blob_name": [b for b in _.values()][0],"owner":[z for z in _.keys()][0]}
                tampunganjadi.append(formatmeta)
            json_formatBLob = {"meta": tampunganjadi}
            return json_formatBlob

    except ResourceNotFoundError:
        return("Container not found.")

@app.get('/')
def root():
    return {"type": "/metadata for metadata data"}
