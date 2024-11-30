from typing import Optional, Any, Sequence, Tuple

from .exceptions import NoSuchObjectError
from .typedefs import Queue, Event, Lock, QueueGetResult
from .utils import queue_get, queue_put, queue_get_until_empty


class ContextBase(object):
    def __init__(
        self,
        input_queue: Optional[Queue],
        output_queue: Optional[Queue],
        cancel_event: Optional[Event],
    ):
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._cancel_event = cancel_event

    def has_input_queue(self) -> bool:
        return self._input_queue is not None

    def has_output_queue(self) -> bool:
        return self._output_queue is not None

    def has_cancel_event(self) -> bool:
        return self._cancel_event is not None

    def is_cancel_event_set(self) -> bool:
        if not self.has_cancel_event():
            raise NoSuchObjectError("no cancel event")
        return self._cancel_event.is_set()


class TaskContext(ContextBase):
    def __init__(
        self,
        input_queue: Optional[Queue],
        output_queue: Optional[Queue],
        cancel_event: Optional[Event],
    ):
        super().__init__(input_queue, output_queue, cancel_event)

    def read_input(
        self,
        block: bool = False,
        timeout: Optional[float] = None,
        task_done: bool = False,
    ) -> QueueGetResult:
        if not self.has_input_queue():
            raise NoSuchObjectError("no input queue")

        return queue_get(
            self._input_queue, block=block, timeout=timeout, task_done=task_done
        )

    def read_input_until_empty(
        self,
        block: bool = False,
        timeout: Optional[float] = None,
        task_done: bool = False,
    ) -> Sequence[Any]:
        if not self.has_input_queue():
            raise NoSuchObjectError("no input queue")
        return queue_get_until_empty(
            self._input_queue,
            block=block,
            timeout=timeout,
            task_done=task_done,
        )

    def write_output(
        self,
        item: Any,
        block: bool = False,
        timeout: Optional[float] = None,
        join: bool = False,
    ) -> bool:
        if not self.has_output_queue():
            raise NoSuchObjectError("no output queue")
        return queue_put(
            self._output_queue, item, block=block, timeout=timeout, join=join
        )


class MainContext(ContextBase):
    def __init__(
        self,
        input_queue: Queue,
        output_queue: Queue,
        cancel_event: Event,
        *,
        output_queue_lock: Optional[Lock] = None,
    ):
        super().__init__(input_queue, output_queue, cancel_event)
        self._output_queue_lock = output_queue_lock

    def set_cancel_event(self):
        if not self.has_cancel_event():
            raise NoSuchObjectError("no cancel event")
        self._cancel_event.set()

    def clear_cancel_event(self):
        if not self.has_cancel_event():
            raise NoSuchObjectError("no cancel event")
        self._cancel_event.clear()

    def write_input(
        self,
        item: Any,
        block: bool = False,
        timeout: Optional[float] = None,
        join: bool = False,
    ) -> bool:
        if not self.has_input_queue():
            raise NoSuchObjectError("no input queue")
        return queue_put(
            self._input_queue, item, block=block, timeout=timeout, join=join
        )

    def read_output(
        self,
        block: bool = False,
        timeout: Optional[float] = None,
        task_done: bool = False,
    ) -> QueueGetResult:
        if not self.has_output_queue():
            raise NoSuchObjectError("no output queue")
        return queue_get(
            self._output_queue, block=block, timeout=timeout, task_done=task_done
        )

    def read_output_until_empty(
        self,
        block: bool = False,
        timeout: Optional[float] = None,
        task_done: bool = False,
    ) -> Sequence[Any]:
        if not self.has_output_queue():
            raise NoSuchObjectError("no output queue")
        return queue_get_until_empty(
            self._output_queue,
            block=block,
            timeout=timeout,
            task_done=task_done,
            lock=self._output_queue_lock,
        )


class CombinedContext(MainContext, TaskContext):
    def __init__(
        self,
        input_queue: Queue,
        output_queue: Queue,
        cancel_event: Event,
        *,
        output_queue_lock: Optional[Lock] = None,
        input_queue_lock: Optional[Lock] = None,
    ):
        super().__init__(input_queue, output_queue, cancel_event)
        self._input_queue_lock = input_queue_lock
        self._output_queue_lock = output_queue_lock

    def read_input(
        self,
        block: bool = False,
        timeout: Optional[float] = None,
        task_done: bool = False,
    ) -> QueueGetResult:
        if not self.has_input_queue():
            raise NoSuchObjectError("no input queue")
        return queue_get(
            self._input_queue,
            block=block,
            timeout=timeout,
            task_done=task_done,
        )

    def read_input_until_empty(
        self,
        block: bool = False,
        timeout: Optional[float] = None,
        task_done: bool = False,
    ) -> Sequence[Any]:
        if not self.has_input_queue():
            raise NoSuchObjectError("no input queue")
        return queue_get_until_empty(
            self._input_queue,
            block=block,
            timeout=timeout,
            task_done=task_done,
            lock=self._input_queue_lock,
        )


def create_context(
    input_queue: Optional[Queue],
    output_queue: Optional[Queue],
    cancel_event: Optional[Event],
    *,
    output_queue_lock: Optional[Lock] = None,
) -> Tuple[TaskContext, MainContext]:
    task_context = TaskContext(input_queue, output_queue, cancel_event)
    main_context = MainContext(
        input_queue, output_queue, cancel_event, output_queue_lock=output_queue_lock
    )
    return task_context, main_context
