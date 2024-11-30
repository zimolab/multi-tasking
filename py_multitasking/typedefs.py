import enum
import multiprocessing as mp
import queue
import threading
from typing import Union, Any, Tuple

Queue = Union[queue.Queue, mp.Queue]
Event = Union[threading.Event, mp.Event]
Lock = Union[threading.Lock, mp.Lock]


# (result, is_empty)
QueueGetResult = Tuple[Any, bool]


class Scope(enum.Enum):
    Global = 0
    Session = 1
    PerTask = 2
    Null = 3
