from __future__ import absolute_import

import requests
import time
from structlog import get_logger

from ..intervals import Interval, IntervalSet
from ..node import BranchNode, LeafNode

logger = get_logger()


class RemoteFinder(object):
    __fetch_multi__ = 'remote-graphite-api'

    def __init__(self, config):
        self.remotes = config['remote']['remotes']

    def find_nodes(self, query):
        logger.debug("find_nodes", finder="remote", start=query.startTime,
                     end=query.endTime, pattern=query.pattern)

        for remote_host in self.remotes:
            try:
                response = requests.get("%s/%s%s" % (remote_host, "metrics/find?format=completer&query=", query.pattern))
                if response.status_code == 200:
                    paths = response.json()['metrics']
                    for path in paths:
                        if not path['is_leaf']:
                            yield BranchNode(path['path'][:-1])
                        else:
                            reader = RemoteReader(remote_host, path['path'], query.startTime, query.endTime)
                            yield RemoteLeafNode(path['path'], reader)
            except:
                continue

    def fetch_multi(self, nodes, start_time, end_time):
        logger.debug("fetch_multi", reader="remote", start=start_time, end=end_time)

        request_groups = {}

        for node in nodes:
            if node.reader.remote_uri in request_groups:
                request_groups[node.reader.remote_uri].append(node)
            else:
                request_groups[node.reader.remote_uri] = [node]

        time_info = None
        series = {}
        for remote_uri, host_nodes in request_groups.iteritems():
            reader = host_nodes[0].reader
            paths = [node.path for node in host_nodes]

            response = requests.post("%s/%s" % (remote_uri, "render"), data={'target': paths, 'format': 'raw', 'from': start_time, 'until': end_time})

            if response.status_code == 200:
                if response.text != '':
                    lines = response.text.splitlines()
                    for line in lines:
                        key_string, time_info, data_points = reader.parse_raw_line(line.rstrip())
                        series[key_string] = data_points

        if not series:
            return self.default_multi_data(start_time, end_time, 60, [node.path for node in nodes])
        else:
            return time_info, series

    def default_multi_data(self, start_time, end_time, step, paths):
        time_info = (start_time, end_time, step)
        return time_info, dict((key, [None for v in range(start_time, end_time, step)]) for key in paths)



class RemoteReader(object):
    __slots__ = ('remote_uri', 'metric_name', 'interval_start', 'interval_end')

    def __init__(self, remote_uri, metric_name, interval_start, interval_end):
        self.remote_uri = remote_uri
        self.metric_name = metric_name
        self.interval_start = interval_start if interval_start is not None else 0
        self.interval_end = interval_end if interval_end is not None else time.time()

    def get_intervals(self):
        logger.debug("get_intervals", reader="remote", remote_uri=self.remote_uri,
                     metric_path=self.metric_name)
        return IntervalSet([Interval(self.interval_start, self.interval_end)])

    def fetch(self, start_time, end_time):
        logger.debug("fetch", reader="remote", remote_uri=self.remote_uri,
                     metric_path=self.metric_name,
                     start=start_time, end=end_time)
        response = requests.post("%s/%s" % (self.remote_uri, "render"), data={'target': self.metric_name, 'format': 'raw', 'from': start_time, 'until': end_time})

        if response.status_code == 200:
            if response.text == '':
                return self.default_data(start_time, end_time, 60)
            else:
                key_string, time_info, data_points = self.parse_raw_line(response.text.rstrip())
                return time_info, data_points
        else:
            return self.default_data(start_time, end_time, 60)

    def default_data(self, start_time, end_time, step):
        time_info = (start_time, end_time, step)
        return time_info, [None for v in range(start_time, end_time, step)]

    def parse_raw_line(self, response_text):
        meta_data, datapoints_string = response_text.split("|")
        key_string, start_string, end_string, step_string = meta_data.split(",")
        step = int(step_string)
        start = int(start_string)
        end = int(end_string)
        datapoints = [float(point) if point != "None" else None for point in datapoints_string.split(",")]
        if len(datapoints) == 0:
            return self.default_data(start, end, step)
        else:
            time_info = (start, end, step)
            return key_string, time_info, datapoints

class RemoteLeafNode(LeafNode):
    __fetch_multi__ = 'remote-graphite-api'
