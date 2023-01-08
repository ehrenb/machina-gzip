"""The gzip command in Linux can only be used to compress a single file.
In order to compress a folder, tar + gzip (which is basically tar -z) is used​."""
import base64
import gzip
import json

from machina.core.worker import Worker

class Gz(Worker):
    types = ['gzip']
    next_queues = ['Identifier']

    def __init__(self, *args, **kwargs):
        super(Gz, self).__init__(*args, **kwargs)

    def callback(self, data, properties):
        # self.logger.info(data)
        data = json.loads(data)

        # resolve path
        target = self.get_binary_path(data['ts'], data['hashes']['md5'])
        self.logger.info(f"resolved path: {target}")

        with gzip.open(target, 'rb') as f:
            data_encoded = base64.b64encode(f.read()).decode()
            body = {
                "data": data_encoded,
                "origin": {
                    "ts": data['ts'],
                    "md5": data['hashes']['md5'],
                    "uid": data['uid'],
                    "type": data['type']
                    }
                }
        self.publish_next(json.dumps(body))
