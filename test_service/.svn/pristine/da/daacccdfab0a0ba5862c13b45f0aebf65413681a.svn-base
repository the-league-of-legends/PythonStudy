__author__ = 'zouhl'
from yt_base import YTKafkaConsumer


def _valid_data(msg, ter_code):
    if not ter_code:
        return msg.decode()

    if isinstance(ter_code, str):
        if ter_code.encode() in msg:
            return msg
        else:
            return None

    if isinstance(ter_code, list):
        for each in ter_code:
            if each.encode() in msg:
                return msg
    return None


class KafkaOutput(YTKafkaConsumer):
    def __init__(self, server, topic, group_id=None, filter_info=None):
        if group_id is None:
            group_id = 'automation_1'
        super().__init__(server, topic, group_id)
        self.filter_info = filter_info

    def get_data(self):
        for msg in self.consume_data():
            if self.filter_info:
                msg_filtered = _valid_data(msg, self.filter_info)
            else:
                msg_filtered = msg

            if msg_filtered:
                yield msg_filtered

