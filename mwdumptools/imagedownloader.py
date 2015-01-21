# -*- coding: utf-8 -*-
"""
=====================================
python-mwdump-tools - imagedownloader
=====================================

Naively downloads images by parsing a Mediawiki dump and reading all the 
File:XXX articles.

Uses concurrency to download and scale images.

The final output is the SQL to reconstruct the Mediawiki image table. You 
should do this because the script is not guaranteed to successfully download 
all images.


Usage:
  imagedownloader [--dlurl=URL] 
                  [--output=PATH] 
                  [--scale] 
                  [--ext=EXT]...
                  [--resume=N]
                  [--namespaces=NS]...
  imagedownloader (-h | --help)
  imagedownloader --version

Options:
  -h --help          Show this screen.
  --version          Show version.
  --dlurl=URL        The root URL from where to find images. Python string
                     formatting is required! Example:
                     "http://upload.wikimedia.org/wikipedia/commons/{h1:s}/{h2:s}/{fname:s}"
  --savepath=PATH    Where to save images
  --scale            Should images be scaled after downloading?
  --ext=EXT          Specify which extensions (not case sensitive) to include
                     (remember that JPG and JPEG are different strings)
                     [default: jpg, jpeg, png, gif, bmp, tiff]
  --resume=N         Resume from line no (if the script has been interrupted)
  --namespaces=NS    The mediawiki dump namespace to read from
                     [default: 6]
"""
from mwdumptools import settings
from mwdumptools import streamparser
from mwdumptools import VERSION
from docopt import docopt
import sys
import concurrent.futures
import urllib.request
import urllib.error
import time
import socket
from hashlib import md5
import os
from PIL import Image
import traceback
import threading

################################################################
# Default values, set using kwargs in ImageDownloader.__init__ #
################################################################

DEFAULT_DOWNLOAD_PATH = "http://upload.wikimedia.org/wikipedia/commons/{h1:s}/{h2:s}/{fname:s}"
GET_EXTENSIONS = [".jpg", "jpeg", "png", "gif", "bmp"]

DEFAULT_THUMBNAIL_PATH = "http://upload.wikimedia.org/wikipedia/commons/{h1:s}/{h2:s}/{fname:s}"

OUTPUT_ROOT = os.path.abspath("./images")

# Convert images before saving
SCALE = True
MAX_IMAGE_SIZE = (1024, 1024)

# Should match your CPU count and bandwidth speed.
# If threads are handling image scaling and you have many cores, you
# can increase the number.
# The higher your bandwidth, the less threads you want, because they
# finish faster!!
MAX_THREADS = 8

# Output
OUTPUT_SQL = True

SQL_UPDATE = (
    "UPDATE image "
    "SET img_width='{width:d}', img_height='{height:d}', img_size='{filesize}' "
    "WHERE img_name='{name:s}'"
)

DOWNLOAD_TIMEOUT = 10  # seconds

DOWNLOAD_RETRIES = 2

# Retrieve a single page and report the url and contents


def load_url(url, local_path, timeout):
    for __ in range(DOWNLOAD_RETRIES):
        try:
            conn = urllib.request.urlopen(url, timeout=timeout)
            data = conn.readall()
            settings.logger.debug("Got image, length: {0:d}".format(len(data)))
            os.makedirs(os.path.dirname(local_path), mode=0o755, exist_ok=True)
            f = open(local_path, "wb")
            f.write(data)
            f.close()
            return local_path
        except urllib.error.URLError:
            continue  # DNS error
        except urllib.error.HTTPError:
            return None  # Probably 404, so do not retry
        except socket.gaierror:
            continue  # Network error


# Scale image bytes and save to local_path
def scale_image(fname, local_path, size, output_to=None):
    if not output_to:
        output_to = local_path
    img = Image.open(local_path)
    img.thumbnail(size, Image.ANTIALIAS)
    img.save(output_to, format=local_path.split(".")[-1])
    return img.size


# Decorator to add blocking waits when the job pool is saturated
def job(fn):
    def decorated(self, *args, **kwargs):
        # Don't let the main thread add millions of jobs... just block it
        while self.jobs_running > self.processes:
            time.sleep(1)
        self.jobs_running += 1
        fn(self, *args, **kwargs)
        self.jobs_running -= 1
    return decorated


# http://docs.python.org/dev/library/concurrent.futures#processpoolexecutor
# The ProcessPoolExecutor class is an Executor subclass that uses a pool of
# processes to execute calls asynchronously. ProcessPoolExecutor uses the
# multiprocessing module, which allows it to side-step the
# Global Interpreter Lock but also means that only picklable objects can
# be executed and returned.
#
# http://docs.python.org/dev/library/concurrent.futures#concurrent.futures.Future.add_done_callback
# Added callables are called in the order that they were added and are always
# called in a thread belonging to the process that added them. If the callable
# raises a Exception subclass, it will be logged and ignored. If the callable
# raises a BaseException subclass, the behavior is undefined.
class ImagePoolWorker:

    def __init__(self, processes, dlurl, output_dir,
                 max_image_size, timeout, output_stream):
        self.processes = processes
        self.executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=processes)
        self.jobs_running = 0
        self.dlurl = dlurl
        self.output_dir = output_dir
        self.max_image_size = max_image_size
        self.timeout = timeout
        self.max_processes = processes
        self.output_lock = threading.Lock()
        self.output_stream = output_stream

    @job
    def get_image(self, url, fname, local_path, timeout, callback, error_callback):
        settings.logger.debug("Downloading {:s}".format(url))
        try:
            future = self.executor.submit(load_url, url, local_path, timeout)
            future.add_done_callback(
                lambda future: callback(fname, future.result(), url))
        except Exception as exc:
            error_callback(url, exc)

    @job
    def scale_image(self, fname, local_path, size, callback, error_callback):
        try:
            future = self.executor.submit(scale_image, fname, local_path, size)
            future.add_done_callback(
                lambda future: callback(future, fname, local_path))
        except Exception as exc:
            error_callback(local_path, exc)

    def shutdown(self, timeout):
        time.sleep(timeout)
        self.executor.shutdown(wait=True)

    def image_downloaded(self, fname, local_path, url):
        """Callback from WorkerThread"""
        self.scale_image(
            fname,
            local_path,
            self.max_image_size,
            self.image_resized,
            self.image_resize_error
        )

    def image_download_error(self, url, exception):
        """Callback from WorkerThread"""
        settings.logger.error("Could not download: {0:s}".format(url))

    def image_resized(self, future, fname, local_path):
        if future.exception() is not None:
            settings.logger.error("got exception: %s" % future.exception())
            return
        size = future.result()
        if not OUTPUT_SQL:
            return
        self.output_lock.acquire()
        self.output_stream.write(SQL_UPDATE.format(
            width=size[0],
            height=size[1],
            filesize=os.stat(local_path).st_size,
            name=os.path.split(local_path)[-1]
        ))
        self.output_lock.release()

    def image_resize_error(self, fname, local_path):
        settings.logger.error("Error resize: {}".format(local_path))

    def get_hash(self, filename):
        m = md5()
        m.update(filename.encode('utf-8'))
        c = m.hexdigest()
        return c[0], c[0:2]

    def get_local_path(self, h1, h2, fname):
        return os.path.join(self.output_dir, h1, h2, fname)


class ImageDownloader(streamparser.XmlStreamParser, ImagePoolWorker):

    def __init__(self, in_file=None, output=OUTPUT_ROOT, namespaces=["6"],
                 dlurl=DEFAULT_DOWNLOAD_PATH, max_image_size=MAX_IMAGE_SIZE,
                 processes=MAX_THREADS, timeout=DOWNLOAD_TIMEOUT, **kwargs):

        self.namespaces = namespaces

        streamparser.XmlStreamParser.__init__(self, in_file=in_file,
                                              out_file=sys.stdout, **kwargs)
        ImagePoolWorker.__init__(self, processes, dlurl, output,
                                 max_image_size, timeout, sys.stdout)

    def parse_site_info(self, lines):
        streamparser.XmlStreamParser.parse_site_info(self, lines)

    def handle_page(self, page):
        if page.find("ns").text in self.namespaces:
            try:
                title = page.find("title").text
            except AttributeError:
                settings.logging.warning(
                    "No title in <page>, line:", self.line_no)
            fname = title.replace("File:", "")
            h1, h2 = self.get_hash(fname)
            local_path = self.get_local_path(h1, h2, fname)
            url = DEFAULT_DOWNLOAD_PATH.format(h1=h1, h2=h2, fname=fname)
            settings.logger.debug("Trying to get: {}".format(url))
            self.get_image(
                url,
                fname,
                local_path,
                self.timeout,
                self.image_downloaded,
                self.image_download_error
            )

    def execute(self):
        streamparser.XmlStreamParser.execute(self)
        self.shutdown(self.timeout)


if __name__ == "__main__":
    arguments = docopt(__doc__, version='python-mw-tools ' + str(VERSION))
    arguments = dict((k.replace("--", ""), v) for k, v in arguments.items())
    arguments = dict(
        filter(lambda kv: not kv[1] is None, [(k, v) for k, v in arguments.items()]))
    p = ImageDownloader(**arguments)
    try:
        p.execute()
    except Exception as e:
        settings.logger.error("Failed to parse, line no: {}".format(p.line_no))
        settings.logger.error(e)
        settings.logger.debug(traceback.print_tb(sys.exc_info()[2]))
        settings.logger.error(
            "You can set --resume={} after fixing to resume".format(p.line_no))
