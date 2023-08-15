import numpy as np
from google.cloud import storage
import random
import time
import signal
from . import util

class DataLogger:
    """
    Create one instance of this in the producer script, to send data to
    a bokeh server.
    """
    def __init__(self, scope):
        self.configured_plots = set()
        self.scope = scope
        self.point_groups = {}
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
        self.message_buf = []
        self.buffer_max_size = buffer_items

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
        buffered write of the message
        """
        if len(self.message_buf) == self.buffer_max_size:
            packed = util.pack_messages(self.message_buf)
            self._append_gcs(packed)
            self.message_buf.clear()
        self.message_buf.append(messages)

    def write(self, group_name, **values):
        """
        Writes new data, possibly creating a new MetaData item
        """
        if not all(isinstance(val, (int, float)) for val in values.values()):
            raise RuntimeError(
                'DataLogger::write: `values` contains a non-{int,float} value: {values}')

        if group_name not in self.point_groups:
            field_types = { f: isinstance(val, int) for f, val in values.items() }
            point_group = util.make_point_group(self.scope, group_name, **field_types)
            self.point_groups[group_name] = point_group
            self.seqnum[group_name] = 0
            self._write_message(point_group)
        point_group = self.point_groups[group_name]

        point = util.make_point(point_group, self.seqnum[group_name], **values)
        self.seqnum[group_name] += 1
        self._write_message(point)

    def shutdown(self):
        """
        Call shutdown in a SIGINT or SIGTERM signal handler in your main application
        for a clean exit 
        """
        # Any GCS resources need flushing or cleanup?
        pass

