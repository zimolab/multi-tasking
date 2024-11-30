from multiprocessing.context import BaseContext
from multiprocessing.managers import SyncManager
from typing import Optional, Tuple, Callable

from .manager import TaskManagerBase
from .processimpl import TaskManagerWithProcessPoolExecutor
from .threadimpl import TaskManagerWithThreadPoolExecutor


def with_process_pool_executor(
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
) -> TaskManagerBase:
    return TaskManagerWithProcessPoolExecutor(
        max_workers=max_workers,
        mp_context=mp_context,
        initializer=initializer,
        initargs=initargs,
        manager=manager,
        global_input_queue=global_input_queue,
        global_output_queue=global_output_queue,
        global_cancel_event=global_cancel_event,
        global_input_queue_lock=global_input_queue_lock,
        global_output_queue_lock=global_output_queue_lock,
        global_input_queue_size=global_input_queue_size,
        global_output_queue_size=global_output_queue_size,
    )


def with_thread_pool_executor(
    max_workers: Optional[int] = None,
    thread_name_prefix: str = "",
    initializer: Optional[Callable] = None,
    initargs: Tuple = (),
    *,
    global_input_queue: bool = False,
    global_output_queue: bool = False,
    global_cancel_event: bool = False,
    global_input_queue_lock: bool = False,
    global_output_queue_lock: bool = False,
    global_input_queue_size: Optional[int] = None,
    global_output_queue_size: Optional[int] = None,
) -> TaskManagerBase:
    return TaskManagerWithThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix=thread_name_prefix,
        initializer=initializer,
        initargs=initargs,
        global_input_queue=global_input_queue,
        global_output_queue=global_output_queue,
        global_cancel_event=global_cancel_event,
        global_input_queue_lock=global_input_queue_lock,
        global_output_queue_lock=global_output_queue_lock,
        global_input_queue_size=global_input_queue_size,
        global_output_queue_size=global_output_queue_size,
    )
