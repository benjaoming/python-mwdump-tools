python-mwdump-tools
===================

Quick parsing of Mediawiki XML dumps


## imagedownloader

Usage:

    python3 -m mwdumptools.imagedownloader

## Python 3

You need Python 3 to use this because it's running futures.concurrent stuff for
parallel processing.

You need pip for Python 3, consider setting up a virtual env:

The following is essential for your Pillow install to process images 
with imagedownloader:

    libjpeg provides JPEG functionality.
    zlib provides access to compressed PNGs
    libtiff provides group4 tiff functionality


