#  ToDo list
#  1. Ditch processes in favor of async
#  2. Make it "standalone" add program input so there will be no need to change playlist url in code
#  3. Think about adding ffmpeg and ripping highest quality video (with separate audio stream :<)

from pytube import Playlist
from multiprocessing import Process
from decimal import Decimal
import asyncio
from functools import wraps, partial
import time


def worker(task):
    print(f"Downloading '{task.title}'")
    retries = 3
    while retries > 0:
        try:
            task.stream.download()
            retries = 0
        except Exception as e:  # ToDo I know it is bad but right now I do not have time to find possible exceptions
            # urllib.error.HTTPError: HTTP Error 503: Service Unavailable
            print(e)
            retries -= 1
            if retries == 0:
                print(f"Reached maximum retires for '{task.title}' {task.watch_url}")


class Task:
    def __init__(self, title, stream, watch_url):
        self.title = title
        self.stream = stream
        self.watch_url = watch_url


def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run


def create_task(video):
    #  ToDo it would be nice to download best quality and mux with separate audio
    stream = video.streams.filter(progressive=True, file_extension='mp4').last()
    res = Task(video.title, stream, video.watch_url)
    return res


async_create_task = async_wrap(create_task)


def get_tasks_from_playlist(playlist):
    startstart = time.time()
    def get_progress(current, total):
        progress = Decimal((current/total)*100).quantize(Decimal(10) ** -2)
        return f"progress {progress}"

    max_async_jobs = 32
    videos = [video for video in playlist.videos]
    videos_count = len(videos)
    tasks = []

    print(f"Current progress: {get_progress(len(tasks), videos_count)}% ({len(tasks)}/{videos_count})")
    while len(videos) > 0:
        start_time = time.time()
        current_list = []
        if len(videos) >= max_async_jobs:
            for _ in range(max_async_jobs):
                current_list.append(videos.pop())
        else:
            for _ in range(len(videos)):
                current_list.append(videos.pop())
        loop = asyncio.get_event_loop()

        current_tasks = [async_create_task(video) for video in current_list]
        result = loop.run_until_complete(asyncio.gather(*current_tasks))

        tasks += result

        time_delta = time.time() - start_time
        print(f"Current progress: {get_progress(len(tasks), videos_count)}% ({len(tasks)}/{videos_count}) last iteration done in {(Decimal(time_delta).quantize(Decimal(10) ** -2))}s")
    print(f"Current progress: last iteration done in {(Decimal(time.time() - startstart).quantize(Decimal(10) ** -2))}s")
    return tasks


def get_tasks_from_playlist_synch(playlist):
    tasks = []
    print(f'Generating streams for {playlist.title}')
    start_time = time.time()
    videos = [video for video in playlist.videos]
    for video in videos:
        #  ToDo it would be nice to download best quality and mux with separate audio
        stream = video.streams.filter(progressive=True, file_extension='mp4').last()
        tasks.append(Task(video.title, stream, video.watch_url))
        print(f"Current size: {len(tasks)}/{len(videos)}")

    time_delta = time.time() - start_time
    print(f"Current progress: last iteration done in {(Decimal(time_delta).quantize(Decimal(10) ** -2))}s")
    return tasks


def main():
    # playlist_url = "https://www.youtube.com/watch?v=VB_GWz25B3Q&list=UUsXVk37bltHxD1rDPwtNM8Q&index=2&ab_channel=Kurzgesagt%E2%80%93InaNutshell"
    # playlist_url =  "https://www.youtube.com/watch?v=ER8Tu3z5r98&list=PLQGf3CUqgd8ewnfterOy49b7KhMZ-fZOQ&ab_channel=TERENWIZJA"
    # playlist_url = "https://www.youtube.com/watch?v=dUnGvH8fUUc&list=UUBa659QWEk1AI4Tg--mrJ2A"
    playlist = Playlist(playlist_url)
    tasks_to_process = get_tasks_from_playlist(playlist)
    print(tasks_to_process)
    print(f"Starting download of {len(tasks_to_process)} videos")
    while len(tasks_to_process) > 0:
        processList = []  # ToDo processList contains tuple(process, task) so using name process is kinda misleading
        current_list = []
        if len(tasks_to_process) >= 5:
            for _ in range(5):
                current_list.append(tasks_to_process.pop())
        else:
            for _ in range(len(tasks_to_process)):
                current_list.append(tasks_to_process.pop())
        print(f"Downloading {len(current_list)} videos")
        for stream in current_list:
            p = Process(target=worker, args=(stream,))
            processList.append((p, stream))

        for process in processList:
            process[0].start()

        timeout_seconds = 300
        for process in processList:
            process[0].join(timeout_seconds)

        for process in processList:
            if process[0].is_alive():
                try:
                    print(f"Something bad has happened with {process[1].title}, {process[1].watch_url}")
                except Exception as e:
                    print(e)
                process[0].terminate()

        print(f"{len(tasks_to_process)} videos left")


if __name__ == '__main__':
    # main()
    # playlist_url =  "https://www.youtube.com/watch?v=ER8Tu3z5r98&list=PLQGf3CUqgd8ewnfterOy49b7KhMZ-fZOQ&ab_channel=TERENWIZJA"
    playlist_url = "https://www.youtube.com/watch?v=dUnGvH8fUUc&list=UUBa659QWEk1AI4Tg--mrJ2A&index=2"
    # playlist_url = "https://www.youtube.com/watch?v=bd0HA_n2cjw&list=PL96C35uN7xGIo2odDuuPeYtb7BtQ1kBhp&ab_channel=TomScott"
    playlist_url =  "https://www.youtube.com/watch?v=ER8Tu3z5r98&list=PLQGf3CUqgd8ewnfterOy49b7KhMZ-fZOQ&ab_channel=TERENWIZJA"
    playlist = Playlist(playlist_url)
    tasks_to_process = get_tasks_from_playlist(playlist)
    # tasks_to_process = get_tasks_from_playlist_synch(playlist)