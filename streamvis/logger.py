import numpy as np
from google.cloud import storage
import random
import time
import signal
from . import util
from . import data_pb2 as pb

class DataLogger:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, scope):
        self.configured_plots = set()
        self.scope = scope
        self.metadata = {}
        self.seqnum = {}
        random.seed(time.time())

    def init_gcs(self, bucket_name, blob_name, buffer_items=100):
        """
        Initialize logger to log data to the given GCS blob
        buffer_items:  number of data items to buffer
        bucket_name:  GCS bucket name for writing all data
        blob_name:  GCS blob name for writing all data
        """
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        self.blob = bucket.blob(blob_name)
        if not self.blob.exists():
            self.blob.upload_from_string('')
        suffix = hex(random.randint(0, 0x1000000))[2:]
        self.tmp_blob = bucket.blob(blob_name + '-' + suffix) 
        self.buffer_items = buffer_items
        self.points = pb.Points()

    def _append_gcs(self, content):
        """
        Appends content to the GCS blob
        content: protobuf message 
        """
        if self.tmp_blob.exists():
            self.tmp_blob.delete()
        self.tmp_blob.upload_from_string(content)
        self.blob.compose([self.blob, self.tmp_blob])

    def _write_message(self, message):
        """
        do a buffered write of the message.
        If message is pb.Point, it is buffered in self.points
        If it is pb.MetaData, it is written immediately
        """
        if isinstance(message, pb.MetaData):
            packed = util.pack_message(message)
            self._append_gcs(packed)
        elif isinstance(message, pb.Point):
            self.points.p.append(message)
            if len(self.points.p) >= self.buffer_items:
                packed = util.pack_message(self.points)
                self._append_gcs(packed)
                self.points = pb.Points()
        else:
            raise RuntimeError(
                    'DataLogger::write accepts only MetaData or Point.  Received '
                    f'{type(message)}')

    def write(self, meta_name, **values):
        """
        Writes new data, possibly creating a new 
        """
        if meta_name not in self.metadata:
            meta = pb.MetaData()
            meta.id = random.randint(0, 2**32)
            meta.scope = self.scope
            meta.name = meta_name
            for name, val in values.items():
                if not isinstance(val, (int, float)):
                    raise RuntimeError(
                        'DataLogger::write: `values` contains a non-{int,float} '
                        f'value: {values}')
                datum = meta.data.add()
                datum.name = name
                datum.is_integer = isinstance(val, int)

            self.metadata[meta_name] = meta
            self.seqnum[meta_name] = 0
            self._write_message(meta)

        meta = self.metadata[meta_name]
        item = pb.Point()
        item.meta_id = meta.id
        item.seqnum = self.seqnum[meta_name]
        self.seqnum[meta_name] += 1
        for datum in meta.data:
            if datum.name not in values:
                raise RuntimeError(
                    f'DataLogger::write: `values` does not contain datum {datum.name} '
                    f'previously appearing in the metadata field.  '
                    f'Expected data are {meta.data})') 
            if datum.is_integer:
                item.idata.append(values[datum.name])
            else:
                item.fdata.append(values[datum.name])

        if len(values) != len(meta.data):
            raise RuntimeError(
                f'DataLogger::write: `values` contained extra entries. '
                f'values: {values}\nvs.'
                f'meta.data: {meta.data}')

        self._write_message(item)


    def shutdown(self):
        """
        Call shutdown in a SIGINT or SIGTERM signal handler in your main application
        for a clean exit 
        """
        # Any GCS resources need flushing or cleanup?
        pass

