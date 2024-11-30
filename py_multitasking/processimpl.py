from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from multiprocessing.context import BaseContext
from multiprocessing.managers import SyncManager
from typing import Optional, Tuple

from .manager import TaskManagerBase
from .typedefs import Lock, Event, Queue


class TaskManagerWithProcessPoolExecutor(TaskManagerBase):
    def __init__(
        self,
        max_workers: Optional[int] = None,
        mp_context: Optional[BaseContext] = None,
        initializer: Optional[callable] = None,
        initargs: Tuple = (),
        *,
        manager: Optional[SyncManager] = None,
        global_input_queue: bool = False,
        global_output_queue: bool = False,
        global_cancel_event: bool = False,
        global_input_queue_lock: bool = False,
        global_output_queue_lock: bool = False,
        global_input_queue_size: Optional[int] = None,
        global_output_queue_size: Optional[int] = None,
    ):
        self._executor: ProcessPoolExecutor = ProcessPoolExecutor(
            max_workers, mp_context, initializer, initargs
        )
        self._manager = manager or Manager()
        self._shutdown_manager = manager is None

        super().__init__(
            self._executor,
            global_input_queue=global_input_queue,
            global_output_queue=global_output_queue,
            global_cancel_event=global_cancel_event,
            global_input_queue_lock=global_input_queue_lock,
            global_output_queue_lock=global_output_queue_lock,
            global_input_queue_size=global_input_queue_size,
            global_output_queue_size=global_output_queue_size,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        if self._shutdown_manager:
            self._manager.shutdown()

    def shutdown(self):
        self._executor.shutdown()
        if self._shutdown_manager:
            self._manager.shutdown()

    def create_queue(self, size: Optional[int] = None) -> Queue:
        if not size:
            return self._manager.Queue()
        return self._manager.Queue(size)

    def create_event(self) -> Event:
        return self._manager.Event()

    def create_lock(self) -> Lock:
        return self._manager.Lock()
