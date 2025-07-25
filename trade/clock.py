from datetime import datetime
from abc import ABC, abstractmethod

class Clock(ABC): # 接口
    @abstractmethod
    def set_time(self, time: str):
        raise NotImplementedError("set_time is not implemented")
    @abstractmethod
    def set_ts(self, ts: int):
        raise NotImplementedError("set_ts is not implemented")
    @abstractmethod
    def get_time(self):
        raise NotImplementedError("get_time is not implemented")
    @abstractmethod
    def get_ts(self):
        raise NotImplementedError("get_ts is not implemented")

class RClock(Clock):
    def __init__(self):
        pass

    def set_time(self, time: str):
        raise NotImplementedError("set_time is not implemented")

    def set_ts(self, ts: int):
        raise NotImplementedError("set_ts is not implemented")

    def get_time(self):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_ts(self):
        return int(datetime.now().timestamp())
    
class VClock(Clock):
    def __init__(self, time: str):
        self._time = time

    def set_time(self, time: str):
        self._time = time

    def set_ts(self, ts: int):
        self._time = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    def get_time(self):
        return self._time

    def get_ts(self):
        return int(datetime.strptime(self._time, '%Y-%m-%d %H:%M:%S').timestamp())