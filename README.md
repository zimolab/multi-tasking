# Multitasking Library for Python

## Introduction

A simple python library to simplify multitasking programming with ProcessPoolExecutor or ThreadPoolExecutor. 

## Installation

```shell
pip install multi-tasking
```


## Usage

### Basic Usage

Let's use the `with_pool_executor()` to run a function in parallel.

```python
import math
import time

from py_multitasking import with_process_pool_executor, Scopes


def is_prime(n: int) -> bool:
    if n <= 1:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        if n % i == 0:
            return False
    return True


def find_primes(num_range: range):
    primes = []
    for num in num_range:
        if is_prime(num):
            primes.append(num)
    return primes


def find_primes_0():
    start_time = time.time_ns()
    primes = []
    for num in range(1, 5000000):
        if is_prime(num):
            primes.append(num)
    print(f"Found: {len(primes)} primes in total")
    print(f"Execution time: {(time.time_ns() - start_time) / 1e9} seconds")


def find_primes_1():
    start_time = time.time_ns()
    with with_process_pool_executor(max_workers=5) as manager:
        scopes = Scopes()
        session = (
            manager.session()
            .submit("task1", find_primes, scopes, range(1, 1000000))
            .submit("task2", find_primes, scopes, range(1000000, 2000000))
            .submit("task3", find_primes, scopes, range(2000000, 3000000))
            .submit("task4", find_primes, scopes, range(3000000, 4000000))
            .submit("task5", find_primes, scopes, range(4000000, 5000000))
        )
        found = 0
        session.wait_for_all()
        for task_name, result in session.results().items():
            if not result.successful:
                print(f"{task_name}: failed with {result.exception}")
                continue
            found += len(result.value)
            print(f"{task_name}: found {len(result.value)} primes")

        print(f"Found: {found} primes in total")
        print(f"Execution time: {(time.time_ns() - start_time) / 1e9} seconds")
        session.destroy()


def find_primes_2():
    start_time = time.time_ns()
    with with_process_pool_executor(max_workers=5) as manager:
        session = manager.map(
            "task-",
            find_primes,
            None,
            [
                range(1, 1000000),
                range(1000000, 2000000),
                range(2000000, 3000000),
                range(3000000, 4000000),
                range(4000000, 5000000),
            ],
        )
        session.wait_for_all()
        total_primes = 0
        for task_name, result in session.results().items():
            if not result.successful:
                print(f"{task_name}: failed with {result.exception}")
                continue
            total_primes += len(result.value)
            print(f"{task_name}: found {len(result.value)} primes")
        print(f"Found: {total_primes} primes in total")
        print(f"Execution time: {(time.time_ns() - start_time) / 1e9} seconds")
        session.destroy()


if __name__ == "__main__":
    find_primes_0()
    print("=" * 80)
    find_primes_1()
    print("=" * 80)
    find_primes_2()
```

`with_thread_pool_executor()` also available. But it's not recommended to use it if your function is CPU-bound.

`with_process_pool_executor()` and `with_thread_pool_executor()` will create a `TaskManagerBase` object, which uses the
`TaskSession` object to manager the tasks.

To create a `TaskSession`, we can use the `session()` method or the `map()` method of the `TaskManagerBase` object.

```python

def my_task(a, b, c):
    pass


with with_process_pool_executor(max_workers=5) as manager:
    session = manager.session()
    # or
    session = manager.map("task-", my_task, None, [(1,2,3), (4,5,6)])
```

Here are the signatures of `TaskManagerBase.session()` and `TaskManagerBase.map()`:

```python
class TaskManagerBase(object):
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
        ...

    def map(
        self,
        task_name_prefix: str,
        func: TaskFuncTypes,
        scopes: Optional[Scopes] = None,
        *iterables,
    ) -> TaskSession:
        ...
```

The `TaskSession` object has the following methods or properties which are very useful:

- `submit(task_name: str, func: TaskFuncTypes, scopes: Optional[Scopes] = None, *args, **kwargs) -> "TaskSession"`

This method is used to submit a task function to the `TaskSession`, it will create a `Task` object and add it to the task
list of the `TaskSession`.

- `any_done`

This property returns `True` if any task in the task list of the `TaskSession` is done, otherwise it returns `False`.

- `all_done`

This property returns `True` if all tasks in the task list of the `TaskSession` are done, otherwise it returns `False`.

we can use this property to wait for all tasks to finish in our main process(thread):

```python
with with_process_pool_executor() as manager:
    session = manager.session()
    session.submit("task1", my_task, 1, 2, 3)
    session.submit("task2", my_task, 4, 5, 6)
    ...
    
    while not session.all_done:
        # do something in the main process(thread) until all tasks are done
        ...
```

- `done_tasks`

This property returns a list of `Task` objects which are done.

- `wait_for_all(timeout: Optional[float] = None, return_when: str = "ALL_COMPLETED") -> Tuple[List[Task], List[Task]]`

This method waits for all tasks in the task list of the `TaskSession` to finish, it returns a tuple of two lists, The 
first one, contains the tasks that completed (is finished or cancelled) before the wait completed. The second one, 
contains uncompleted tasks.

- `results(timeout: Optional[float] = None) -> Dict[str, TaskResult]`

This method returns a dictionary of `TaskResult` objects, the key is the task name, the value is the `result` of the task.

The `result` is not the direct return value of the task function, it's a `TaskResult` object which is a wrapper of the return
value of the task function or the exception raised by the task function.

we can access the actual return value of the task function by doing:

```python

...
results = session.results()
# assuming you have submitted a task named "task_name"
result = results["task_name"]

if result.successful:
    ret_value = result.value
else:
    task_exception = result.exception
...
```

- `done_results() -> Dict[str, TaskResult]`

This method returns a dictionary of `TaskResult` objects, the key is the task name, the value is the `result` of the task.
Only the tasks that are done at the moment this method is called will be included in the returned dictionary.

- `is_name_available(task_name: str) -> bool`

The name of a task should be unique in a `TaskSession`. This method is used to check if a name is available or not before
submitting a task to a `TaskSession`. 

This method returns `True` if the name is available, otherwise it returns `False`.

- `cancel_all(with_cancel_event_set: bool = False) -> None`

This method is used to cancel all tasks (by calling the `cancel()` method of the underlying `Future` object of a task) 
in the task list of the `TaskSession`. If `with_cancel_event_set` is `True`, it will try to set the `cancel_event` of all tasks to `True`.

- `context`

This property returns the `CombinedContext` object which is used to communicate between tasks and main process(thread) with
session-wide `input_queue`, `output_queue` and `cancel_event`.

Any task function submitted to a `TaskSession` will be represented by a `Task` object, which has the following properties:

- `name`

The property holds the name of the task. The name of a task should be unique in a `TaskSession`.
 
- `future`

The property holds the `Future` object of the task.

- `submitted`

This property indicates whether the task has been submitted to a `TaskSession` or not.

- `done`

This property indicates whether the task is done or not.

- `cancelled`

This property indicates whether the task is cancelled or not. `cancelled` means a `CancelledError` has been raised in 
the task function.

- `running`

This property indicates whether the task is running or in a pending state.

- `result(timeout: Optional[float] = None) -> Optional[TaskResult]`

This method is used to get the result of the task. It returns a `TaskResult` object which is a wrapper of the return value
of the task function or the exception raised by the task function.

This method will wait for the task until it is done or wait for the specified time if `timeout` is not `None`.

- `result_or_none() -> Optional[TaskResult]`

This method is similar to `result()` but it never waits for the task to finish, it returns `None` if the task is not done yet.


- `cancel(with_cancel_event_set: bool = True) -> bool`

This method is used to cancel the task. It returns `True` if the task is cancelled successfully, otherwise it returns `False`.
This method will try to call the `cancel()` method of the underlying `Future` object of the task.

If `with_cancel_event_set` is `True`, it will also try to set the `cancel_event` of the task to `True`.

- `context`

This property returns the `MainContext` object which is used to communicate between tasks and main process(thread) with
the `input_queue`, `output_queue` and `cancel_event` of the `Task` object.

### Communicate between tasks and main process(thread)

```python
import time
from random import randint

from py_multitasking import TaskContext, with_process_pool_executor, Scopes


def process_order(ctx: TaskContext, factor: int) -> None:
    print(f"order processor started, factor: {factor}")
    # quit if cancel event is set
    while not ctx.is_cancel_event_set():
        # receive a new order from main process
        new_order, is_empty = ctx.read_input()
        # if is_empty is True, it means the input queue is empty (no new order in the input queue)
        if is_empty:
            time.sleep(0.1)
            continue
        # process the new order, simulate processing time
        time.sleep(randint(1, 10))
        # send the processed order to output queue
        # it will be received by main process later
        success = ctx.write_output(new_order * factor)
        # if the output queue is full, ctx.write_output() may fail with a return value of False
        if not success:
            # wait for a while and try again
            time.sleep(0.1)
            success = ctx.write_output(new_order * factor)
            if not success:
                # if the output queue is still full, drop the order
                print(f"Dropping order {new_order} because output queue is full")


def produce_orders() -> None:
    with with_process_pool_executor(max_workers=5) as manager:
        session = manager.session()
        # start 5 processes to process orders
        for i in range(5):
            session.submit(f"task-{i}", process_order, Scopes.Session(), factor=i * 2)
        # wait for all processes to start
        time.sleep(1)

        while True:
            # get new order from user input
            user_input = input(
                "Enter new order count (q to quit; r to receive processed orders): "
            )
            if user_input.lower() == "q":
                # set cancel event to stop all processes
                session.cancel_all(with_cancel_event_set=True)
                session.wait_for_all()
                break
            elif user_input.lower() == "r":
                print("Waiting for new orders...")
                time.sleep(1)
                # get all processed orders from output queue
                processed_orders = session.context.read_output_until_empty()
                if processed_orders:
                    print(f"Received {len(processed_orders)} processed orders")
                    for order in processed_orders:
                        print(f"Processed order: {order}")
                else:
                    print("No processed orders yet")
                    continue
            else:
                try:
                    user_input = int(user_input)
                except ValueError:
                    print("Invalid input, please enter a number or 'q' to quit")
                    continue
                else:
                    # add new orders to input queue
                    for i in range(user_input):
                        session.context.write_input(i, block=False)

        session.wait_for_all()
        print("All processes have stopped")
        session.destroy()


if __name__ == "__main__":
    produce_orders()
```

When the task function has the following signatures:

```python
from py_multitasking import TaskContext


def foo1(ctx: TaskContext, *args, **kwargs) -> None:
    ...


def foo2(ctx, *args, **kwargs) -> None:
    ...


def foo3(context, *args, **kwargs) -> None:
    ...


def foo4(task_context, *args, **kwargs) -> None:
    ...


def foo5(task_ctx, *args, **kwargs) -> None:
    ...
```

The `TaskContext` object will be created and passed as the first argument to the task function automatically.

`mutitasking` using 2 queues and 1 event to communicate between tasks and main process(thread), the two queues are:

- `input_queue`: used to send msg to tasks from main process(thread).
- `output_queue`: used to send msg from tasks to main process(thread).

The event is called `cancel_event` and it's used to notify the running tasks that the main process(thread) wants to stop them now.

Those 2 queues and 1 event can be accessed from the various places, it depends on the `Scope`s, the possible values are:

- `Scope.Null`: Don't create the queue or the event.
- `Scope.Global`: Use the global queue or event, it's shared by all tasks in the same `TaskManagerBase` object.
- `Scope.Session`: Use the queue or event belongs to a session, all tasks in the same session share the same queue or event.
- `Scope.PerTask`: Use the queue or event belongs to a task, each task has its own queue or event.

we can use `Scopes` to specify the scope of the `input_queue`, `output_queue` and `cancel_event`:

```python
from py_multitasking import Scopes, TaskContext, with_process_pool_executor, Scope


def foo(ctx: TaskContext, a: int, b: int) -> None:
    pass


if __name__ == "__main__":
    with with_process_pool_executor(max_workers=5) as manager:
        session = manager.session()
        # create a task with a per-task input_queue, no output_queue and a session-wide cancel_event
        session.submit("task1", foo,
                       Scopes(input_queue=Scope.PerTask, output_queue=Scope.Null, cancel_event=Scope.Session), 1, 2)
        # create a task with a session-wide input_queue, no output_queue and a session-wide cancel_event
        session.submit("task2", foo,
                       Scopes(input_queue=Scope.Session, output_queue=Scope.Null, cancel_event=Scope.Session), 3, 4)
```

> Note: To use global input_queue, output_queue and cancel_event, we should turn on the related options, like:
> ```python
> with_process_pool_executor(global_input_queue=True, global_output_queue=True, global_cancel_event=True)
>```
> or we will get a `NoSuchObjectException`

How to access the `input_queue`, `output_queue` and `cancel_event`? Besides the `Scope`s, it also depends on where we want to
access them.

1. In the task function, we can access the `input_queue`, `output_queue` and `cancel_event` using the `TaskContext` object:

```python
from py_multitasking import TaskContext


def foo(ctx: TaskContext, a: int, b: int) -> None:
    while True:
        # check if the cancel_event is set
        if ctx.is_cancel_event_set():
            # stop the task
            break
        # read a value from the input_queue
        value, is_empty = ctx.read_input()
        if not is_empty:
            # write a value to the output_queue
            success = ctx.write_output(value * a * b)
            if not success:
                # if the output_queue is full, drop the value
                print("Output queue is full, dropping value")
        else:
            # is_empty is True, it means the input_queue is empty (no value in the input_queue)
            print("Input queue is empty, waiting for new values")
```

2. In the main process(thread):

For global `input_queue`, `output_queue` and `cancel_event`, we can access them using `TaskManagerBase.context`

```python
if __name__ == "__main__":
    with with_process_pool_executor(max_workers=5, global_input_queue=True, global_output_queue=True,
                                    global_cancel_event=True) as manager:
        session = manager.session()
        session.submit("task1", foo, Scopes.Global(), 1, 2)
        
        global_ctx = manager.context
        # write a value to the global input_queue
        global_ctx.write_input(100)
        time.sleep(0.1)
        # read a value from the global output_queue
        value, is_empty = global_ctx.read_output()
        if not is_empty:
            print(f"Received value: {value}")
        else:
            print("No value in output queue")
        # set the global cancel_event to stop all tasks
        global_ctx.set_cancel_event()
        session.wait_for_all()
        session.destroy()
```

For session-wide `input_queue`, `output_queue` and `cancel_event`, we can access them using `TaskSession.context`

```python
if __name__ == "__main__":
    with with_process_pool_executor(max_workers=5) as manager:
        session = manager.session()
        session.submit("task1", foo, Scopes.Session(), 1, 2)
        
        session_ctx = session.context
        # write a value to the session input_queue
        session_ctx.write_input(100)
        time.sleep(0.1)
        # read a value from the session output_queue
        value, is_empty = session_ctx.read_output()
        if not is_empty:
            print(f"Received value: {value}")
        else:
            print("No value in output queue")
        # set the session cancel_event to stop all tasks in the session
        session_ctx.set_cancel_event()
        session.wait_for_all()
        session.destroy()
```

For per-task `input_queue`, `output_queue` and `cancel_event`, we can access them using `Task.context`

```python
if __name__ == "__main__":
    with with_process_pool_executor(max_workers=5) as manager:
        session = manager.session()
        session.submit("task1", foo, Scopes.PerTask(), 1, 2)
        
        task = session.get_task("task1")
        
        task_ctx = task.context
        # write a value to the task input_queue
        task_ctx.write_input(100)
        time.sleep(0.1)
        # read a value from the task output_queue
        value, is_empty = task_ctx.read_output()
        if not is_empty:
            print(f"Received value: {value}")
        else:
            print("No value in output queue")
        # set the task cancel_event to stop the task
        task_ctx.set_cancel_event()
        session.wait_for_all()
        session.destroy()
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.