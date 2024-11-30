import math
import time
from concurrent.futures import ProcessPoolExecutor

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
        scopes = Scopes.Null()
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
    with ProcessPoolExecutor(max_workers=5) as executor:
        results = executor.map(
            find_primes,
            (
                range(1, 1000000),
                range(1000000, 2000000),
                range(2000000, 3000000),
                range(3000000, 4000000),
                range(4000000, 5000000),
            ),
        )
    end_time = time.time_ns()
    for result in results:
        print(f"Found: {len(result)} primes in total")
    print(f"Execution time: {(end_time - start_time) / 1e9} seconds")


def find_primes_3():
    start_time = time.time_ns()
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = []
        for num_range in [
            range(1, 1000000),
            range(1000000, 2000000),
            range(2000000, 3000000),
            range(3000000, 4000000),
            range(4000000, 5000000),
        ]:
            future = executor.submit(find_primes, num_range)
            futures.append(future)

        while True:
            if all(future.done() for future in futures):
                break
        total_primes = 0
        for future in futures:
            total_primes += len(future.result())
        print(f"Found: {total_primes} primes in total")
        print(f"Execution time: {(time.time_ns() - start_time) / 1e9} seconds")


def find_primes_4():
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
    print("=" * 80)
    find_primes_3()
    print("=" * 80)
    find_primes_4()
