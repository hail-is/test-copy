import json
import secrets
import sys
import asyncio
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from hailtop.utils import time_msecs
from hailtop.aiotools.fs import RouterAsyncFS, LocalAsyncFS, Transfer
from hailtop.aiogoogle import GoogleStorageAsyncFS
from hailtop.aiotools.s3asyncfs import S3AsyncFS


async def main():
    n_trials = int(sys.argv[1])
    src = sys.argv[2]
    dest_base = sys.argv[3]

    token = secrets.token_hex(16)

    with ThreadPoolExecutor() as thread_pool:
        async with RouterAsyncFS('file', [LocalAsyncFS(thread_pool),
                                          GoogleStorageAsyncFS(),
                                          S3AsyncFS(thread_pool)]) as fs:
            sema = asyncio.Semaphore(50)

            times = []
            for i in range(n_trials):
                dest = f'{dest_base}/{token}'
                await fs.rmtree(sema, dest)

                start = time_msecs()
                copy_report = await fs.copy(sema, Transfer(src, dest, treat_dest_as=Transfer.DEST_IS_TARGET))
                copy_report.summarize()
                duration = time_msecs() - start
                times.append(duration / 1000.0)

            print(f'times: {times}')
            print(f'time mean {np.mean(times)} std {np.std(times)}')

            with open('/home/ubuntu/times.json', 'w') as f:
                f.write(json.dumps(times))

            await fs.rmtree(sema, dest_base)


if __name__ == '__main__':
    asyncio.run(main())
