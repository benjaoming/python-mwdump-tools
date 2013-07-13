# -*- coding: utf-8 -*-
"""python-mwdump-tools imagedownloader

Downloads images by parsing a Mediawiki dump and reading all the File:XXX
articles.

Uses concurrency to download and scale images.

The final output is the SQL to reconstruct the Mediawiki image table. You should
do this because the script is not guaranteed to successfully download all images.


Usage:
  imagedownloader ship new <name>...
  imagedownloader ship <name> move <x> <y> [--speed=<kn>]
  imagedownloader ship shoot <x> <y>
  imagedownloader mine (set|remove) <x> <y> [--moored | --drifting]
  imagedownloader (-h | --help)
  imagedownloader --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  --speed=<kn>  Speed in knots [default: 10].
  --moored      Moored (anchored) mine.
  --drifting    Drifting mine.

"""
from mwdumptools import settings
from mwdumptools import streamparser
from docopt import docopt
import concurrent.futures
import urllib.request
import time
from hashlib import md5
import os
from PIL import Image

################################################################
# Default values, set using kwargs in ImageDownloader.__init__ #
################################################################

DEFAULT_DOWNLOAD_PATH = "http://upload.wikimedia.org/wikipedia/commons/{h1:s}/{h2:s}/{fname:s}"
GET_EXTENSIONS = [".jpg", "jpeg", "png", "gif", "bmp"]

OUTPUT_ROOT = os.path.abspath("./images")

# Convert images before saving
CONVERT = True
MAX_IMAGE_SIZE = (1024, 1024)

# Should match your CPU count and bandwidth speed.
# If threads are handling image scaling and you have many cores, you
# can increase the number.
# The higher your bandwidth, the less threads you want, because they
# finish faster!!
MAX_THREADS = 8

# Output
OUTPUT_SQL = True


# Retrieve a single page and report the url and contents
def load_url(url, timeout):
    conn = urllib.request.urlopen(url, timeout=timeout)
    return conn.readall()

def scale_image(local_path, size, output_to=None):
    if not output_to:
        output_to = local_path
    img = Image.open(local_path)
    img.thumbnail(size, Image.ANTIALIAS)
    img.save(output_to)


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
class PoolWorker():
    
    def __init__(self, processes):
        self.processes = processes
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=processes)
        self.jobs_running = 0
    
    @job
    def get_image(self, url, fname, local_path, callback, error_callback):
        try:
            future = self.executor.submit(load_url, url, 60)
            future.add_done_callback(lambda future: callback(future.result(), fname, local_path))
        except Exception as exc:
            error_callback(url, exc)
    
    @job
    def scale_image(self, fname, local_path, size, callback, error_callback):
        try:
            future = self.executor.submit(scale_image, size, local_path, 60)
            future.add_done_callback(lambda future: callback(future.result(), fname, local_path))
        except Exception as exc:
            error_callback(local_path, exc)
    
    

class ImageDownloader(streamparser.XmlStreamParser):
    
    def __init__(self, in_file=None, out_file=None, namespace=6, 
        dlurl=DEFAULT_DOWNLOAD_PATH, output_dir=OUTPUT_ROOT,
        max_image_size=MAX_IMAGE_SIZE,
        processes=MAX_THREADS, **kwargs):
        self.namespace = "6"
        self.dlurl = DEFAULT_DOWNLOAD_PATH
        self.output_dir = OUTPUT_ROOT
        self.max_image_size = max_image_size
        streamparser.XmlStreamParser.__init__(self, in_file=in_file, out_file=out_file, **kwargs)
        self.max_processes = processes
        self.worker = PoolWorker(processes)
    
    def image_downloaded(self, data, fname, local_path):
        """Callback from WorkerThread"""
        full_path = os.path.join(self.output_dir, local_path)
        os.makedirs(os.path.dirname(full_path), mode=0o755, exist_ok=True)
        open(full_path, "wb").write(data)
        settings.logger.debug("Got image, length: {0:d}".format(len(data)))
        self.worker.scale_image(
            fname,
            local_path,
            self.max_image_size, 
            self.image_resized, 
            self.image_resize_error
        )
    
    def image_download_error(self, url, exception):
        """Callback from WorkerThread"""
        settings.logger.error("Could not download: {0:s}".format(url))
    
    def image_resized(self, fname, local_path):
        pass
    
    def image_resize_error(self, fname, local_path):
        settings.logger.error("Error resize: {}".format(local_path))
    
    def parse_site_info(self, lines):
        streamparser.XmlStreamParser.parse_site_info(self, lines)
        if not self.namespaces.get(int(self.namespace)):
            settings.logger.error("Dump does not specify namespace: {}".format(self.namespace))
    
    def get_hash(self, filename):
        m = md5()
        m.update(filename.encode('utf-8'))
        c = m.hexdigest()
        return c[0], c[0:2]
    
    def get_local_path(self, h1, h2, fname):
        return os.path.join(h1, h2, fname)
    
    def handle_page(self, page):
        if page.find("ns").text == self.namespace:
            # This is a file page, namespace = 6
            try:
                title = page.find("title").text
            except AttributeError:
                settings.logging.warning("No title in <page>, line:", self.line_no)
            fname = title.replace("File:", "")
            h1, h2 = self.get_hash(fname)
            local_path = self.get_local_path(h1, h2, fname)
            url = DEFAULT_DOWNLOAD_PATH.format(h1=h1, h2=h2, fname=fname)
            settings.logger.debug("Trying to get: {}".format(url))
            self.worker.get_image(
                url, 
                fname,
                local_path,
                self.image_downloaded,
                self.image_download_error
            )


if __name__ == "__main__":
    arguments = docopt(__doc__, version='python-mw-tools imagedownloader 1.0')
    p = ImageDownloader()
    try:
        p.execute()
    except streamparser.ParseError as e:
        settings.logger.error("Failed to parse, line no: {}".format(p.line_no))
        settings.logger.error(e)
        settings.logger.error("You can set resume={} after fixing to resume".format(p.line_no))