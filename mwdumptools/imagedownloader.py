# -*- coding: utf-8 -*-
"""

Downloads images by parsing a Mediawiki dump and reading all the File:XXX
articles.

Since Python is single-threaded, we spawn a curl command for every image found
and if resizing is required, each image is passed to imagemagick for scaling.

The final output is the SQL to reconstruct the Mediawiki image table. You should
do this because the script is not guaranteed to successfully download all images.

"""
from mwdumptools import settings
from mwdumptools import streamparser
import concurrent.futures
import urllib.request
import time
from hashlib import md5
import os

################################################################
# Default values, set using kwargs in ImageDownloader.__init__ #
################################################################

DEFAULT_DOWNLOAD_PATH = "http://upload.wikimedia.org/wikipedia/commons/{h1:s}/{h2:s}/{fname:s}"
GET_EXTENSIONS = [".jpg", "jpeg", "png", "gif", "bmp"]

OUTPUT_ROOT = os.path.abspath("./images")

# Convert images before saving
CONVERT = True
MAX_X = 1024
MAX_Y = 1024

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


def job(fn):
    def decorated(self, *args, **kwargs):
        # Don't let the main thread add millions of jobs... just block it
        while self.jobs_running > self.processes:
            time.sleep(1)
        self.jobs_running += 1
        fn(self, *args, **kwargs)
        self.jobs_running -= 1
    return decorated

class WorkerThread():
    def __init__(self, processes):
        self.processes = processes
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=processes)
        self.jobs_running = 0
    
    @job
    def get_image(self, url, local_path, callback, error_callback):
        self.jobs_running += 1
        try:
            future = self.executor.submit(load_url, url, 60)
            future.add_done_callback(lambda future: callback(future.result(), local_path))
        except Exception as exc:
            error_callback(url, exc)


class ImageDownloader(streamparser.XmlStreamParser):
    
    def __init__(self, in_file=None, out_file=None, namespace=6, 
        dlurl=DEFAULT_DOWNLOAD_PATH, output_dir=OUTPUT_ROOT,
        processes=MAX_THREADS, **kwargs):
        self.namespace = "6"
        self.dlurl = DEFAULT_DOWNLOAD_PATH
        self.output_dir = OUTPUT_ROOT
        streamparser.XmlStreamParser.__init__(self, in_file=in_file, out_file=out_file, **kwargs)
        self.max_processes = processes
        self.worker = WorkerThread(processes)
    
    def image_downloaded(self, data, local_path):
        full_path = os.path.join(self.output_dir, local_path)
        os.makedirs(os.path.dirname(full_path), mode=0o755, exist_ok=True)
        open(full_path, "wb").write(data)
        settings.logger.debug("Got image, length: {0:d}".format(len(data)))
    
    def image_error(self, url, exception):
        settings.logger.error("Could not download: {0:s}".format(url))
        
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
                local_path,
                self.image_downloaded,
                self.image_error
            )


if __name__ == "__main__":

    p = ImageDownloader()
    try:
        p.execute()
    except streamparser.ParseError as e:
        settings.logger.error("Failed to parse, line no: {}".format(p.line_no))
        settings.logger.error(e)
        settings.logger.error("You can set resume={} after fixing to resume".format(p.line_no))