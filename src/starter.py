import asyncio
from time import time

import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from src.processors import CommonProcessor, AsyncProcessor


def runner(cli_args):
    if cli_args.withasync:
        asynchronius_runner(cli_args)
    else:
        processor = CommonProcessor(cli_args.file, cli_args.copyfrom)
        processor.run_process()

def asynchronius_runner(cli_args):
    loop = asyncio.get_event_loop()
    processor = AsyncProcessor(cli_args.file, loop)
    starting_time = time()
    loop.create_task(processor.run_process())
    loop.run_forever()
    pending = asyncio.Task.all_tasks()
    group = asyncio.gather(*pending)
    loop.run_until_complete(group)
    loop.close()
    print(f'Async document parse DONE in {(time() - starting_time):.3f}')
