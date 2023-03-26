# batch_reencode.py
# Copyright (c) 2023 Mamy André-Ratsimbazafy
# Licensed and distributed under either of
#   * MIT license (http://opensource.org/licenses/MIT).
#   * Apache v2 license (http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

# ############################################################
#
#                    Video Reencoder
#
# ############################################################
#
# This recursively compress videos in a target directory encoded almost-losslessly
# by "Microsoft Game DVR", the Windows native video capture tool.
#
# Requirements:
# - pymediainfo
# - ffmpeg
# - libaom-av1
# - libopus

# We use AV1 codec which allows real-time encoding (200fps on a i9-11980HK 8-core laptop CPU)
# while preserving excellent quality on slideshow with very low bitrates
# i.e. a 1080p slideshow can be encoded with 32kbps video + 32 kbps audio
# The largest presentation/video/slideshow of 2 hours 8.74GiB (bitrate 10.4 Mb/s) has been reduced to 55MB
#
# For audio we use Opus tuned for voice.

# See parameters
#   https://trac.ffmpeg.org/wiki/Encode/AV1
#   https://ffmpeg.org/ffmpeg-codecs.html#libaom_002dav1
#   https://gist.github.com/shssoichiro/a46ff01db70243c1719479f6518ea34d

# Standalone ffmpeg command (note: AFAIK lag-in-frames is ignored in realtime mode but oh well)
# We use an empty artist metadata to drop the Microsoft Game DVR tag
# ffmpeg -i <input.mp4> \
#        -c:v libaom-av1 \
#        -usage realtime -crf 32 -lag-in-frames 32 \
#        -aom-params cpu-used=5:row-mt=1:tune-content=screen:deltaq-mode=0:enable-chroma-deltaq=1:enable-qm=1:quant-b-adapt=1:sharpness=3:arnr-strength=1:arnr-maxframes=4:disable-trellis-quant=0 \
#        -c:a libopus -application voip -b:a 32k \
#        -metadata title=<input> -metadata artist= \
#        -y <output.webm>

# Unfortunately AV1 encoders don't fully use all cores.
# For efficiency, several applications split the input into different scenes
# and encode each on a separate core, however scene detection adds a large overhead
# that is not justified when encoding slideshow / powerpoint presentations.
# Instead, we encode multiple files concurrently, but not too many to not overwhelm the CPU.
# a ratio of ⌈num_cores/3⌉ (i.e. ceil division, division rounded up) should be decent.
#
# This in turn severely complexifies the code as now we need to use async, capture each output and progress bars
# and render them in a clear way to the user.

# Script parameters
# ------------------------------------------------------------

LOG_FILE_PREFIX = 'reencoding'
SRC_EXT         = '.mp4'
FILTER_PROPERTY = 'performer'
FILTER_VALUE    = 'Microsoft Game DVR'
CORES_PER_JOB = 3 # Spawn one encoding job per 3 cores (Hyper-Threading included), rounded up
                        # as AV1 encoders do not fully use all CPU cores yet

GENERAL_PARAMS = ['ffmpeg', '-loglevel', 'info', '-stats','-hide_banner']
VIDEO_PARAMS = (['-c:v','libaom-av1'] +
                ['-usage','realtime','-crf','32','-lag-in-frames','32'] +
                ['-aom-params', 'cpu-used=5:row-mt=1:tune-content=screen:deltaq-mode=0:enable-chroma-deltaq=1:enable-qm=1:quant-b-adapt=1:sharpness=3:arnr-strength=1:arnr-maxframes=4:disable-trellis-quant=0'])
AUDIO_PARAMS = (['-c:a','libopus'] +
                ['-application','voip','-b:a','32k'])

# ############################################################
#
#                    Implementation
#
# ############################################################

# Standard library
from typing import List, Tuple, TypeVar, Optional, Type, TextIO
from types import TracebackType
import os, datetime, time, signal, re, traceback, sys
from datetime import timedelta, datetime
import asyncio
from asyncio import Semaphore, StreamReader
from asyncio.subprocess import Process

# Third-party
from pymediainfo import MediaInfo
from rich.console import Console, ConsoleOptions, RenderResult
from rich.panel import Panel
from rich.table import Table
from rich.status import Status
from rich.progress import Progress, TaskID, SpinnerColumn, TimeElapsedColumn, MofNCompleteColumn
from rich.live import Live

# 1. Collect inputs
# -------------------------------------------------------------

def sizeof_fmt(num: int, suffix="B") -> str:
    for unit in ["", "Ki", "Mi", "Gi", "Ti"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0

def collect_inputs_size_duration(base_dir: str) -> Tuple[List[str], int, int]:
    print(f'Collecting input files in "{base_dir}"')
    result = []
    cumulated_size = 0
    cumulated_duration = 0
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if os.path.splitext(file)[1] == SRC_EXT:
                filepath = os.path.join(root, file)
                media = MediaInfo.parse(filepath)
                if media.general_tracks[0].__getattribute__(FILTER_PROPERTY) == FILTER_VALUE:
                    human_size = media.general_tracks[0].other_file_size[0]
                    human_duration = media.general_tracks[0].other_duration[0]
                    size = media.general_tracks[0].file_size
                    duration = media.general_tracks[0].duration

                    print(f'{" "*8} -> Found a {human_size} video for {human_duration}: "{filepath}"')
                    result.append(filepath)
                    cumulated_size += size
                    cumulated_duration += duration
    return result, cumulated_size, cumulated_duration

# 2. UI for output multiplexing
# -------------------------------------------------------------

# The progress line is displayed using invisible delete characters which makes them invisible in a pipe
# The line_regexp removes them. progress_regexp is unused at the moment but may be used in the future to improve display
progress_regexp = re.compile(r'(frame|fps|size|time|bitrate|speed)\s*\=\s*(\S+)')
line_regexp = re.compile(br'[\r\n]+')

class EncodingProgress:
    """A (file, status) pair
       We need an object instead of a tuple for reference semantics.
    """
    file: str
    status: str

    def __init__(self, file: str):
        self.file = file
        self.status = ''

class ParallelEncodingProgress:
    encodes: List[EncodingProgress]

    def __init__(self):
      self.encodes = []

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        """Rich library display API
        """
        for encode in self.encodes:
            yield f'[{encode.file:<42}]: {encode.status}'

    def append(self, progress_ref: EncodingProgress) -> None:
        self.encodes.append(progress_ref)

    def remove(self, file: str) -> None:
        """O(n) but unless we process thousands of files in parallel (so we have 3000+ cores)
           a linear scan on a List is likely fast enough or even faster due to L1 cache/prefetching
        """
        self.encodes = [elem for elem in self.encodes if elem.file != file]

class EncodingContext:
    encodes: ParallelEncodingProgress
    display: Live
    table: Table
    progress: Progress
    progress_task: TaskID
    completed: int
    status: Status
    log_file: TextIO

    def __init__(self, total: int):
        self.encodes = ParallelEncodingProgress()
        self.progress = Progress(SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn(), MofNCompleteColumn(), expand=True)
        self.progress_task = self.progress.add_task("[green]Overall progress ...", total=total)
        self.completed = 0
        self.status = Status('Initializing ...')

        start_time = time.strftime("%Y-%m-%d_%H%M")
        self.log_file = open(LOG_FILE_PREFIX + '-' + start_time + '.log', 'w', buffering=1) # Use line buffering
        self.table = Table.grid()
        self.table.add_row(Panel.fit(self.status))
        self.table.add_row(Panel.fit(self.progress))
        self.display = Live(self.table)

    def update_status(self) -> None:
        self.status.update(self.encodes)

    def update_completed(self) -> None:
        self.completed += 1
        self.progress.update(self.progress_task, completed=self.completed)

    def log(self, line: str) -> None:
        line = f'[{datetime.now()}] {line}'
        self.log_file.write(line + '\n')
        print(line)

    def __enter__(self) -> 'EncodingContext':
        self.display.start()
        return self

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        self.display.stop()
        self.log_file.close()

# 3. Encode a video & capture output
# -------------------------------------------------------------

def timedelta_fmt(duration: timedelta) -> str:
    data = {}
    data['days'], remaining = divmod(duration.total_seconds(), 86_400)
    data['hours'], remaining = divmod(remaining, 3_600)
    data['minutes'], data['seconds'] = divmod(remaining, 60)

    time_parts = [f'{round(value)} {name}' for name, value in data.items() if value > 0]
    return ' '.join(time_parts)

def monotonic_ns_fmt(duration: int) -> str:
    return timedelta_fmt(timedelta(microseconds=duration//1000))

async def log_progress(ctx: EncodingContext, fd: StreamReader, progress_ref: EncodingProgress) -> None:
    """Log the output of a file descriptor, stdout or stderr
    """
    # The idiomatic "async for line in fd:"
    # or "while line := await fd.readline():" get rekt by the removal character
    # so we need our own custom buffer reader that strips it out
    buf = bytearray()
    while not fd.at_eof():
        lines = line_regexp.split(buf)
        buf[:] = lines.pop(-1)

        for line in lines:
            line = line.rstrip().decode('utf-8')
            if line.startswith('frame='):
                progress_ref.status = line
                ctx.update_status()
            else:
                print(f'[{progress_ref.file}]: {line}')

        buf.extend(await fd.read(1024))

async def encode(ctx: EncodingContext, sem: Semaphore, video_path: str):
    """Once a slot is free, start encoding the next video
       Report progress to the status bar
    """
    async with sem:
        (dir, filename) = os.path.split(video_path)
        basename = os.path.splitext(filename)[0]
        basepath = os.path.splitext(video_path)[0]
        encoded_file = basepath + '.webm'

        # Metadata: Replace title='Zoom' and drop Artist='Microsoft Game DVR'
        cmd = GENERAL_PARAMS + ['-i',video_path] + VIDEO_PARAMS + AUDIO_PARAMS + ['-metadata', 'title='+basename, '-metadata','artist=','-y', encoded_file]

        progress_ref = EncodingProgress(basename)
        ctx.encodes.append(progress_ref)
        ctx.log(f'"{video_path}" - Starting encode')
        start = time.monotonic_ns()

        # Spawn an encoding process, ffmpeg seems to only use stderr but redirect all to stdout just in case
        p: Process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)

        # p.wait() can deadlock if p.stdout is not consumed fast enough
        # We unfortunately can't use p.communicate (which would buffer) unless we drop down to low-level asyncio
        # as by default only a single reader can await the pipe data
        await asyncio.gather(log_progress(ctx, p.stdout, progress_ref), p.wait())

        stop = time.monotonic_ns()
        ctx.log(f'"{video_path}" - Finished encode in {monotonic_ns_fmt(stop-start)}')

        if p.returncode != 0:
            raise ChildProcessError(f'Encoding of "{video_path}" exited with error code {p.returncode}')

        ctx.encodes.remove(basename)
        ctx.update_completed()

        os.rename(video_path, video_path + '.reencoded')
        ctx.log(f'"{video_path}" - Renamed to "{video_path}.reencoded"')

# 4. Putting it all together
# -------------------------------------------------------------

def ceil_div(a: int, b: int) -> int:
    return (a+b-1) // b

async def main(base_dir):
    videos, cumulated_size, cumulated_duration = collect_inputs_size_duration(base_dir)
    print(f'[{datetime.now()}] Found {len(videos)} videos to reencode of cumulated size {sizeof_fmt(cumulated_size)} and duration {timedelta(milliseconds=cumulated_duration)}')

    with EncodingContext(len(videos)) as ctx:
        sem = Semaphore(ceil_div(os.cpu_count(), CORES_PER_JOB))
        jobs = [asyncio.create_task(encode(ctx, sem, video)) for video in videos]

        start = time.monotonic_ns()
        await asyncio.gather(*jobs)
        stop = time.monotonic_ns()

        ctx.log(
            f'Finished reencoding {sizeof_fmt(cumulated_size)} in {len(videos)} videos' +
            f' of total duration {timedelta_fmt(timedelta(milliseconds=cumulated_duration))}' +
            f' in {monotonic_ns_fmt(stop-start)}')


# 5. Calling the code
# -------------------------------------------------------------

if __name__ == "__main__":
    os.setpgrp() # create new process group
    try:
        asyncio.run(main(os.getcwd()))
    except Exception:
        traceback.print_exc()

        # Kill all processes in the group, in particular if we get killed by Ctrl+C ourselves
        signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGTERM,)) # Don't kill ourselves
        os.killpg(0, signal.SIGTERM)                                # kill all processes in our group
        sys.exit(1)
