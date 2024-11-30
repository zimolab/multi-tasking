import sys
from concurrent.futures import Future, CancelledError
from typing import Callable, Any, Optional, Union

from .context import TaskContext, MainContext
from .exceptions import NotSubmittedError, AlreadySubmittedError

# The signature of the task function should be:
# (context: TaskContext, *args, **kwargs) -> Any
RegularTaskFunc = Callable[..., Any]
if sys.version_info >= (3, 9):
    TaskFuncTypes = Union[RegularTaskFunc, Callable[[TaskContext, ...], Future]]
else:
    TaskFuncTypes = RegularTaskFunc


class TaskResult(object):
    def __init__(self, value: Any, exception: Optional[Exception]):
        self._value = value
        self._exception = exception

    @property
    def value(self) -> Any:
        return self._value

    @property
    def exception(self) -> Optional[Exception]:
        return self._exception

    @property
    def timeout(self) -> bool:
        return isinstance(self._exception, TimeoutError)

    @property
    def cancelled(self) -> bool:
        return isinstance(self._exception, CancelledError)

    @property
    def successful(self) -> bool:
        return self._exception is None

    @classmethod
    def from_future(
        cls, future: Future, timeout: Optional[float] = None
    ) -> "TaskResult":
        exception = None
        result = None
        try:
            result = future.result(timeout=timeout)
        except Exception as e:
            exception = e
        return cls(result, exception)


class Task(object):

    def __init__(self, name: str):
        self._name = name

        self._future: Optional[Future] = None
        self._main_context: Optional[MainContext] = None
        self._before_submit_called: bool = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def future(self) -> Optional[Future]:
        return self._future

    @property
    def submitted(self) -> bool:
        return self._future is not None

    @property
    def done(self) -> bool:
        self._ensure_submitted()
        return self._future.done()

    @property
    def cancelled(self) -> bool:
        self._ensure_submitted()
        return self._future.cancelled()

    @property
    def running(self) -> bool:
        self._ensure_submitted()
        return self._future.running()

    @property
    def context(self) -> MainContext:
        self._ensure_submitted()
        return self._main_context

    def cancel(self, with_cancel_event_set: bool = True) -> bool:
        self._ensure_submitted()
        if self.context.has_cancel_event() and with_cancel_event_set:
            self.context.set_cancel_event()
        return self._future.cancel()

    def result(self, timeout: Optional[float] = None) -> TaskResult:
        self._ensure_submitted()
        return TaskResult.from_future(self._future, timeout=timeout)

    def result_or_none(self) -> Optional[TaskResult]:
        self._ensure_submitted()
        if self._future.done():
            return TaskResult.from_future(self._future)
        else:
            return None

    def write_input(
        self,
        msg: Any,
        block: bool = False,
        timeout: Optional[float] = None,
        join: bool = False,
    ):
        self._ensure_submitted()
        return self.context.write_input(msg, block=block, timeout=timeout, join=join)

    def read_output(
        self,
        block: bool = False,
        timeout: Optional[float] = None,
        task_done: bool = False,
    ):
        self._ensure_submitted()
        return self.context.read_output(
            block=block, timeout=timeout, task_done=task_done
        )

    def read_output_until_empty(
        self,
        block: bool = False,
        timeout: Optional[float] = None,
        task_done: bool = False,
    ):
        self._ensure_submitted()
        return self.context.read_output_until_empty(
            block=block, timeout=timeout, task_done=task_done
        )

    def _before_submit(self, ctx: MainContext):
        self._ensure_not_submitted()
        if self._before_submit_called:
            raise RuntimeError("before_submit() has already been called.")
        self._main_context = ctx
        self._before_submit_called = True

    def _after_submit(self, future: Future):
        self._ensure_not_submitted()
        if not self._before_submit_called:
            raise RuntimeError("before_submit() has not been called.")
        self._future = future

    def _ensure_submitted(self):
        if not self.submitted:
            raise NotSubmittedError("not submitted yet.")

    def _ensure_not_submitted(self):
        if self.submitted:
            raise AlreadySubmittedError("already submitted.")
