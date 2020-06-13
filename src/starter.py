import asyncio
from time import time

import uvloop

from src.processors import CommonProcessor, AsyncProcessor


def runner(cli_args):
    if cli_args.withasync:
        asynchronius_runner(cli_args)
    else:
        processor = CommonProcessor(cli_args.file, cli_args.copyfrom)
        processor.run_process()

def asynchronius_runner(cli_args):
    uvloop.install()
    loop = asyncio.get_event_loop()
    processor = AsyncProcessor(cli_args.file, loop)
    starting_time = time()
    loop.run_until_complete(processor.run_process())
    print(f'Async document parse DONE in {(time() - starting_time):.3f}')
