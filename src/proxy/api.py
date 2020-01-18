import itertools
import logging
import sched
import threading
import time
from typing import Final, List

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse

scheduler = sched.scheduler()
app: Final = Starlette(debug=True)


class FileInfoCache:
    def __init__(self, index: int, file_id: str, status: str):
        self.index = index
        self.file_id = file_id
        self.status = status

    def is_complete(self) -> bool:
        return self.status == 'FINISHED'

    def is_processing(self) -> bool:
        return self.status == 'PROCESSING'


class FileInfoHolder:
    def __init__(self, base_uri='http://interview-api.snackable.ai/api'):
        self.base_uri = base_uri
        self.files: List[FileInfoCache] = []
        self.files_unprocessed: List[FileInfoCache] = []
        self.last_file_index = 0

        threading.Thread(target=self.poll_event_loop).start()
        threading.Thread(target=self.check_event_loop).start()

    def poll_event_loop(self):
        while True:
            try:
                do_sleep = self.poll_for_new_files()
                if do_sleep:
                    time.sleep(5.0)
            except BaseException as ex:
                logging.exception("File listing failed", ex)

    def poll_for_new_files(self) -> bool:
        last_index = self.last_file_index
        logging.info("fetch files from index: %s", last_index)
        files = self.fetch_files_info(last_index)
        for idx, f in enumerate(files):
            self.files_unprocessed.append(FileInfoCache(
                index=last_index + idx,
                file_id=f.get('fileId'),
                status=f.get('processingStatus')
            ))
        self.last_file_index += len(files)
        return len(files) == 0

    def check_event_loop(self):
        while True:
            # noinspection PyBroadException
            try:
                self.check_file_status()
            except BaseException as ex:
                logging.exception("File checking failed", ex)

            time.sleep(5.0)

    def check_file_status(self):
        completed = []
        failed = []
        for f in self.files_unprocessed:
            if f.is_processing():
                status = self.fetch_files_info(f.index, 1)[0]
                if status.get('fileId') != f.file_id:
                    raise ValueError("Server returned invalid fileId at position: %s", f.index)
                f.status = status.get('processingStatus')

            if f.is_complete():
                completed.append(f)
            elif not f.is_processing():
                failed.append(f)

        for f in itertools.chain(completed, failed):
            self.files_unprocessed.remove(f)

        for f in completed:
            self.files.append(f)

        if len(completed):
            self.check_file_status()

    def fetch_files_info(self, offset: int, limit: int = 5) -> List:
        r = httpx.get(self.base_uri + '/file/all',
                      params={'offset': offset, 'limit': limit})
        if r.status_code != 200:
            logging.warning("Request failed: %s", r)
            return []
        return r.json()

    async def fetch_and_combine_file_info(self, file_id: str):
        async with httpx.AsyncClient() as client:
            details = await self.fetch_file_details(file_id, client)
            details['segments'] = await self.fetch_file_segments(file_id, client)
            return details

    async def fetch_file_details(self, file_id: str, client: httpx.AsyncClient) -> dict:
        r = await client.get(self.base_uri + '/file/details/' + file_id)
        assert r.status_code == 200
        return r.json()

    async def fetch_file_segments(self, file_id: str, client: httpx.AsyncClient) -> List:
        r = await client.get(self.base_uri + '/file/segments/' + file_id)
        assert r.status_code == 200
        return r.json()


fileInfo: Final = FileInfoHolder()


@app.route('/{file_id}')
async def get_file(request):
    file_id: str = request.path_params['file_id']
    file = next((x for x in fileInfo.files if x.file_id == file_id), None)
    if not file:
        return JSONResponse({"error": 404, "text": "File info not found"}, status_code=404)

    details = await fileInfo.fetch_and_combine_file_info(file_id)
    return JSONResponse(details)


if __name__ == '__main__':
    uvicorn.run(app, port=5000)
