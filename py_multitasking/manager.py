import inspect
from abc import abstractmethod
from concurrent import futures
from dataclasses import dataclass
from typing import List, Optional, Tuple, Any, Dict, Union

from .context import CombinedContext, create_context, TaskContext
from .exceptions import NotAvailableError, AlreadyDestroyedError, NoSuchObjectError
from .task import Task, TaskResult, TaskFuncTypes
from .typedefs import Queue, Event, Lock, Scope

ALL_COMPLETED = futures.ALL_COMPLETED
FIRST_COMPLETED = futures.FIRST_COMPLETED
FIRST_EXCEPTION = futures.FIRST_EXCEPTION

TASK_CONTEXT_PARMA_NAMES = ("ctx", "context", "task_context", "task_ctx")


@dataclass
class Scopes(object):
    input_queue: Scope = Scope.Null
    output_queue: Scope = Scope.Null
    cancel_event: Scope = Scope.Null
    output_queue_lock: Scope = Scope.Null

    @classmethod
    def Session(cls) -> "Scopes":
        return Scopes(
            input_queue=Scope.Session,
            output_queue=Scope.Session,
            cancel_event=Scope.Session,
            output_queue_lock=Scope.Session,
        )

    @classmethod
    def PerTask(cls) -> "Scopes":
        return Scopes(
            input_queue=Scope.PerTask,
            output_queue=Scope.PerTask,
            cancel_event=Scope.PerTask,
            output_queue_lock=Scope.PerTask,
        )

    @classmethod
    def Global(cls) -> "Scopes":
        return Scopes(
            input_queue=Scope.Global,
            output_queue=Scope.Global,
            cancel_event=Scope.Global,
            output_queue_lock=Scope.Global,
        )

    @classmethod
    def Null(cls) -> "Scopes":
        return Scopes(
            input_queue=Scope.Null,
            output_queue=Scope.Null,
            cancel_event=Scope.Null,
            output_queue_lock=Scope.Null,
        )


class TaskSession(object):
    def __init__(
        self,
        manager: "TaskManagerBase",
        session_input_queue: bool = True,
        session_output_queue: bool = True,
        session_cancel_event: bool = True,
        session_input_queue_lock: bool = True,
        session_output_queue_lock: bool = True,
        input_queue_size: Optional[int] = None,
        output_queue_size: Optional[int] = None,
    ):
        self._manager: "TaskManagerBase" = manager

        self._tasks: List[Task] = []

        self._in_queue_size = input_queue_size
        self._out_queue_size = output_queue_size

        self._in_queue: Optional[Queue] = None
        if session_input_queue:
            self._in_queue = self._manager.create_queue(input_queue_size)

        self._out_queue: Optional[Queue] = None
        if session_output_queue:
            self._out_queue = self._manager.create_queue(output_queue_size)

        self._cancel_event: Optional[Event] = None
        if session_cancel_event:
            self._cancel_event = self._manager.create_event()

        self._in_queue_lock: Optional[Lock] = None
        if session_input_queue_lock:
            self._in_queue_lock = self._manager.create_lock()

        self._out_queue_lock: Optional[Lock] = None
        if session_output_queue_lock:
            self._out_queue_lock = self._manager.create_lock()

        self._context = CombinedContext(
            input_queue=self._in_queue,
            output_queue=self._out_queue,
            cancel_event=self._cancel_event,
            input_queue_lock=self._in_queue_lock,
            output_queue_lock=self._out_queue_lock,
        )

        self._destroyed = False

    def is_name_available(self, name: str) -> bool:
        self._ensure_not_destroyed()
        return not any(t.name == name for t in self._tasks)

    def add_task(self, task: Task) -> None:
        self._ensure_not_destroyed()
        if task in self._tasks:
            return
        if not self.is_name_available(task.name):
            raise NotAvailableError(f"task name is not available: {task.name}")
        self._tasks.append(task)

    def get_task(self, name: str) -> Optional[Task]:
        self._ensure_not_destroyed()
        return next((t for t in self._tasks if t.name == name), None)

    def get_tasks(self) -> List[Task]:
        self._ensure_not_destroyed()
        return [t for t in self._tasks]

    def remove_task(self, task: Union[str, Task]) -> Optional[Task]:
        self._ensure_not_destroyed()
        if isinstance(task, Task):
            if task not in self._tasks:
                return None
            self._tasks.remove(task)
            return task
        elif isinstance(task, str):
            task_obj = self.get_task(task)
            if task_obj is None:
                return None
            self._tasks.remove(task_obj)
            return task_obj
        else:
            raise TypeError(f"invalid type: {type(task)}")

    def clear_tasks(self):
        self._ensure_not_destroyed()
        self._tasks.clear()

    @property
    def all_done(self) -> bool:
        self._ensure_not_destroyed()
        return all(task.done for task in self._tasks)

    @property
    def any_done(self) -> bool:
        self._ensure_not_destroyed()
        return any(task.done for task in self._tasks)

    @property
    def done_tasks(self) -> List[Task]:
        self._ensure_not_destroyed()
        return [task for task in self._tasks if task.done]

    @property
    def session_input_queue(self) -> Optional[Queue]:
        self._ensure_not_destroyed()
        return self._in_queue

    @property
    def session_output_queue(self) -> Optional[Queue]:
        self._ensure_not_destroyed()
        return self._out_queue

    @property
    def session_cancel_event(self) -> Optional[Event]:
        self._ensure_not_destroyed()
        return self._cancel_event

    @property
    def session_input_queue_lock(self) -> Optional[Lock]:
        self._ensure_not_destroyed()
        return self._in_queue_lock

    @property
    def session_output_queue_lock(self) -> Optional[Lock]:
        self._ensure_not_destroyed()
        return self._out_queue_lock

    @property
    def context(self) -> CombinedContext:
        self._ensure_not_destroyed()
        return self._context

    def wait_for_all(
        self, timeout: Optional[float] = None, return_when: str = ALL_COMPLETED
    ) -> Tuple[List[Task], List[Task]]:
        self._ensure_not_destroyed()
        future_for_tasks = {task.future: task for task in self._tasks}
        future_list = list(future_for_tasks.keys())
        done, not_done = futures.wait(
            future_list, timeout=timeout, return_when=return_when
        )
        done_tasks = [
            future_for_tasks[future] for future in done if future in future_for_tasks
        ]
        not_done_tasks = [
            future_for_tasks[future]
            for future in not_done
            if future in future_for_tasks
        ]
        return done_tasks, not_done_tasks

    def results(self, timeout: Optional[float] = None) -> Dict[str, TaskResult]:
        self._ensure_not_destroyed()
        ret = {}
        for task in self._tasks:
            result = task.result(timeout=timeout)
            ret[task.name] = result
        return ret

    def done_results(self) -> Dict[str, Any]:
        self._ensure_not_destroyed()
        return {task.name: task.result() for task in self._tasks if task.done}

    def cancel_all(self, with_cancel_event_set=True) -> None:
        self._ensure_not_destroyed()
        for task in self._tasks:
            task.cancel(with_cancel_event_set=with_cancel_event_set)

    def submit(
        self,
        name: str,
        func: TaskFuncTypes,
        scopes: Optional[Scopes] = None,
        *args,
        **kwargs,
    ) -> "TaskSession":
        self._ensure_not_destroyed()
        scopes = scopes or Scopes()
        if not self.is_name_available(name):
            raise NotAvailableError(f"task name is not available: {name}")
        task = Task(name=name)
        input_queue = self._create_input_queue(scopes.input_queue)
        output_queue = self._create_output_queue(scopes.output_queue)
        cancel_event = self._create_cancel_event(scopes.cancel_event)
        output_queue_lock = self._create_output_queue_lock(scopes.output_queue_lock)
        task_context, main_context = create_context(
            input_queue=input_queue,
            output_queue=output_queue,
            cancel_event=cancel_event,
            output_queue_lock=output_queue_lock,
        )
        # noinspection PyProtectedMember
        task._before_submit(main_context)
        task_future = self._manager.submit(func, task_context, *args, **kwargs)
        # noinspection PyProtectedMember
        task._after_submit(task_future)
        self._tasks.append(task)
        return self

    def destroy(self) -> None:
        self._ensure_not_destroyed()
        self._destroyed = True
        del self._tasks
        del self._in_queue
        del self._out_queue
        del self._cancel_event
        del self._in_queue_lock
        del self._out_queue_lock

    def _ensure_not_destroyed(self) -> None:
        if self._destroyed:
            raise AlreadyDestroyedError("already been destroyed")

    def _create_input_queue(self, scope: Scope) -> Optional[Queue]:
        if scope == Scope.Null:
            return None

        if scope == Scope.Global:
            global_in_queue = self._manager.global_input_queue
            if global_in_queue is None:
                raise NoSuchObjectError("no global input queue")
            return global_in_queue

        if scope == Scope.Session:
            session_in_queue = self._in_queue
            if session_in_queue is None:
                raise NoSuchObjectError("no session input queue")
            return session_in_queue

        if scope == Scope.PerTask:
            return self._manager.create_queue(self._in_queue_size)

        raise ValueError(f"invalid scope: {scope}")

    def _create_output_queue(self, scope: Scope) -> Optional[Queue]:
        if scope == Scope.Null:
            return None

        if scope == Scope.Global:
            global_out_queue = self._manager.global_output_queue
            if global_out_queue is None:
                raise NoSuchObjectError("no global output queue")
            return global_out_queue

        if scope == Scope.Session:
            session_out_queue = self._out_queue
            if session_out_queue is None:
                raise NoSuchObjectError("no session output queue")
            return session_out_queue

        if scope == Scope.PerTask:
            return self._manager.create_queue(self._out_queue_size)

        raise ValueError(f"invalid scope: {scope}")

    def _create_cancel_event(self, scope: Scope) -> Optional[Event]:
        if scope == Scope.Null:
            return None

        if scope == Scope.Global:
            global_cancel_event = self._manager.global_cancel_event
            if global_cancel_event is None:
                raise NoSuchObjectError("no global cancel event")
            return global_cancel_event

        if scope == Scope.Session:
            session_cancel_event = self._cancel_event
            if session_cancel_event is None:
                raise NoSuchObjectError("no session cancel event")
            return session_cancel_event

        if scope == Scope.PerTask:
            return self._manager.create_event()

        raise ValueError(f"invalid scope: {scope}")

    def _create_input_queue_lock(self, scope: Scope) -> Optional[Lock]:
        if scope == Scope.Null:
            return None

        if scope == Scope.Global:
            global_in_queue_lock = self._manager.global_input_queue_lock
            if global_in_queue_lock is None:
                raise NoSuchObjectError("no global input queue lock")
            return global_in_queue_lock

        if scope == Scope.Session:
            session_in_queue_lock = self._in_queue_lock
            if session_in_queue_lock is None:
                raise NoSuchObjectError("no session input queue lock")
            return session_in_queue_lock

        if scope == Scope.PerTask:
            return self._manager.create_lock()

        raise ValueError(f"invalid scope: {scope}")

    def _create_output_queue_lock(self, scope: Scope) -> Optional[Lock]:
        if scope == Scope.Null:
            return None

        if scope == Scope.Global:
            global_out_queue_lock = self._manager.global_output_queue_lock
            if global_out_queue_lock is None:
                raise NoSuchObjectError("no global output queue lock")
            return global_out_queue_lock

        if scope == Scope.Session:
            session_out_queue_lock = self._out_queue_lock
            if session_out_queue_lock is None:
                raise NoSuchObjectError("no session output queue lock")
            return session_out_queue_lock

        if scope == Scope.PerTask:
            return self._manager.create_lock()

        raise ValueError(f"invalid scope: {scope}")


class TaskManagerBase(object):
    def __init__(
        self,
        executor: futures.Executor,
        global_input_queue: bool = False,
        global_output_queue: bool = False,
        global_cancel_event: bool = False,
        global_input_queue_lock: bool = False,
        global_output_queue_lock: bool = False,
        global_input_queue_size: Optional[int] = None,
        global_output_queue_size: Optional[int] = None,
        task_context_param_names: Tuple[str, ...] = TASK_CONTEXT_PARMA_NAMES,
    ):
        self._executor: futures.Executor = executor

        self._task_context_param_names = task_context_param_names or ()

        self._global_input_queue_size = global_input_queue_size
        self._global_output_queue_size = global_output_queue_size

        self._in_queue: Optional[Queue] = None
        if global_input_queue:
            self._in_queue = self.create_queue(self._global_input_queue_size)

        self._out_queue: Optional[Queue] = None
        if global_output_queue:
            self._out_queue = self.create_queue(self._global_output_queue_size)

        self._cancel_event: Optional[Event] = None
        if global_cancel_event:
            self._cancel_event = self.create_event()

        self._in_queue_lock: Optional[Lock] = None
        if global_input_queue_lock:
            self._in_queue_lock = self.create_lock()

        self._out_queue_lock: Optional[Lock] = None
        if global_output_queue_lock:
            self._out_queue_lock = self.create_lock()

        self._context: CombinedContext = CombinedContext(
            input_queue=self._in_queue,
            output_queue=self._out_queue,
            cancel_event=self._cancel_event,
            input_queue_lock=self._in_queue_lock,
            output_queue_lock=self._out_queue_lock,
        )

        self._sessions: List[TaskSession] = []

    @property
    def global_context(self) -> CombinedContext:
        return self._context

    @property
    def global_input_queue(self) -> Optional[Queue]:
        return self._in_queue

    @property
    def global_output_queue(self) -> Optional[Queue]:
        return self._out_queue

    @property
    def global_cancel_event(self) -> Optional[Event]:
        return self._cancel_event

    @property
    def global_input_queue_lock(self) -> Optional[Lock]:
        return self._in_queue_lock

    @property
    def global_output_queue_lock(self) -> Optional[Lock]:
        return self._out_queue_lock

    @abstractmethod
    def create_queue(self, size: Optional[int]) -> Queue:
        pass

    @abstractmethod
    def create_event(self) -> Event:
        pass

    @abstractmethod
    def create_lock(self) -> Lock:
        pass

    def submit(
        self,
        func: TaskFuncTypes,
        task_context: Optional[TaskContext],
        *args,
        **kwargs,
    ) -> futures.Future:
        # print(self._is_context_func_type(func))
        if self.is_context_func_type(func):
            # for function with task_context parameter, which signature is like:
            # def foo(task_context: TaskContext, *args, **kwargs):
            #     pass
            return self._executor.submit(func, task_context, *args, **kwargs)
        else:
            # for regular function without task_context parameter, which signature is like:
            # def foo(*args, **kwargs):
            #     pass
            return self._executor.submit(func, *args, **kwargs)

    def session(
        self,
        session_input_queue: bool = True,
        session_output_queue: bool = True,
        session_cancel_event: bool = True,
        session_input_queue_lock: bool = True,
        session_output_queue_lock: bool = True,
        input_queue_size: Optional[int] = None,
        output_queue_size: Optional[int] = None,
    ) -> TaskSession:
        return TaskSession(
            self,
            session_input_queue=session_input_queue,
            session_output_queue=session_output_queue,
            session_cancel_event=session_cancel_event,
            session_input_queue_lock=session_input_queue_lock,
            session_output_queue_lock=session_output_queue_lock,
            input_queue_size=input_queue_size,
            output_queue_size=output_queue_size,
        )

    def map(
        self,
        task_name_prefix: str,
        func: TaskFuncTypes,
        scopes: Optional[Scopes] = None,
        *iterables,
    ) -> TaskSession:
        session = self.session()
        for i, args in enumerate(zip(*iterables)):
            name = f"{task_name_prefix}{i}"
            session.submit(name, func, scopes, *args)
        return session

    def shutdown(self):
        pass

    def is_context_func_type(self, func: TaskFuncTypes) -> bool:
        if not callable(func):
            return False
        signature = inspect.signature(func)
        if len(signature.parameters) < 1:
            return False
        first_param = next(iter(signature.parameters.values()))
        param_type = first_param.annotation
        if param_type is TaskContext or param_type == TaskContext.__class__.__name__:
            return True
        param_name = first_param.name
        return param_name in self._task_context_param_names

    def __enter__(self):
        self._executor.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._executor.__exit__(exc_type, exc_val, exc_tb)
