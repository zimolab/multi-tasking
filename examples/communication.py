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
