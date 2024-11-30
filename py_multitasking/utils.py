import queue
from typing import Optional, Any, Sequence

from .typedefs import Queue, QueueGetResult, Lock


def queue_get(
    q: Queue,
    block: bool = False,
    timeout: Optional[float] = None,
    task_done: bool = False,
) -> QueueGetResult:
    try:
        r = q.get(timeout=timeout, block=block)
        if task_done:
            q.task_done()
    except queue.Empty:
        return None, True
    else:
        return r, False


def _queue_get_until_empty(
    q: Queue,
    block: bool = False,
    timeout: Optional[float] = None,
    task_done: bool = False,
) -> Sequence[Any]:
    result = []
    while True:
        r, is_empty = queue_get(q, block=block, timeout=timeout, task_done=task_done)
        if is_empty:
            break
        result.append(r)
    return result


def queue_get_until_empty(
    q: Queue,
    block: bool = False,
    timeout: Optional[float] = None,
    task_done: bool = False,
    lock: Optional[Lock] = None,
) -> Sequence[Any]:
    if lock is not None:
        with lock:
            return _queue_get_until_empty(q, block, timeout, task_done)
    else:
        return _queue_get_until_empty(q, block, timeout, task_done)


def queue_put(
    q: Queue,
    item: Any,
    block: bool = False,
    timeout: Optional[float] = None,
    join: bool = False,
) -> bool:
    try:
        q.put(item, timeout=timeout, block=block)
        if join:
            q.join()
        return True
    except queue.Full:
        return False
