import sys
import asyncio
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from hailtop.utils import time_msecs
from hailtop.aiotools.fs import RouterAsyncFS, LocalAsyncFS, Transfer
from hailtop.aiogoogle import GoogleStorageAsyncFS
from hailtop.aiotools.s3asyncfs import S3AsyncFS


async def main():
    src = sys.argv[1]
    dest_base = sys.argv[2]

    n_trials = 3

    token = secrets.token_hex(16)

    with ThreadPoolExecutor() as thread_pool:
        async with RouterAsyncFS('file', [LocalAsyncFS(thread_pool),
                                          GoogleStorageAsyncFS(),
                                          S3AsyncFS(thread_pool)]) as fs:
            sema = asyncio.Semaphore(15)

            times = []
            for i in range(n_trials):
                start = time_msecs()
                await fs.copy(sema, Transfer(src, f'{dest_base}/{token}/{i}', treat_dest_as=Transfer.DEST_IS_TARGET))
                duration = time_msecs() - start
                times.append(duration / 1000.0)

            print(f'times: {times}')
            print(f'time mean {np.mean(times)} std {np.std(times)}')

            await fs.rmtree(dest_base)


if __name__ == '__main__':
    asyncio.run(main())
