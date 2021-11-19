from azure.storage.blob import BlobServiceClient, __version__
from azure.core.exceptions import ResourceNotFoundError
import argparse
import os
import json

parser = argparse.ArgumentParser()
parser.add_argument('--action', type=str ,help='action you wanted to do', required=False)
parser.add_argument('--name', type=str ,help='your name', required=False)
parser.add_argument('--file', help='file source you want to upload', required=False)
parser.add_argument('--container', type=str ,help='container name', required=False)
args = parser.parse_args()

class Container(object):
    connect_str = 'BlobEndpoint=https://clinicdumpmemory.blob.core.windows.net/;QueueEndpoint=https://clinicdumpmemory.queue.core.windows.net/;FileEndpoint=https://clinicdumpmemory.file.core.windows.net/;TableEndpoint=https://clinicdumpmemory.table.core.windows.net/;SharedAccessSignature=sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2021-12-11T12:28:40Z&st=2021-11-14T04:28:40Z&spr=https&sig=aewhtO2kZiyAsNe7YH0AzhTk73K4FKmNH1v0iOxMEvY%3D'
    container_name = args.container
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    def list_containers(self):
        try:
            blobTampungan = []
            metaTampungan = []
            for blob in self.container_client.list_blobs():
                blobTampungan.append(blob.name)
            for i in blobTampungan:
                blob_client = self.blob_service_client.get_blob_client(blob=i, container=self.container_name)
                properties = blob_client.get_blob_properties().metadata
                metaTampungan.append(properties)
            json_formatBLob = {"blob": metaTampungan}
            print(json_formatBLob)

        except ResourceNotFoundError:
            print("Container not found.")
    
    def block_blob(self):
        blob_client = self.blob_service_client.get_blob_client(blob=args.file,container=self.container_name)
        try:
            file = args.file
            if '/' in args.file:
                head, tail = os.path.split(args.file)

                blob_client = self.container_client.get_blob_client(blob=tail)

                with open(file, "rb") as data:
                    blob_client.upload_blob(data, blob_type="BlockBlob")
                
                metadata = {args.name : tail}
                blob_client.set_blob_metadata(metadata=metadata)
            else:
                blob_client = self.container_client.get_blob_client(blob=args.file)

                with open(file, "rb") as data:
                    blob_client.upload_blob(data, blob_type="BlockBlob")

                metadata = {args.name : args.file}
                blob_client.set_blob_metadata(metadata=metadata)
        
        except Exception as ex:
            print('Exception:')
            print(ex)


obj = Container()

if args.action == 'list':
    obj.list_containers()
elif args.action == 'upload':
    obj.block_blob()
