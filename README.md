python-mwdump-tools
===================

Quick parsing of Mediawiki XML dumps: Parses stdin XML dumps using simple
string searching and Python's elementree C implementation for parsing each
`<page>` node.


## imagedownloader

Usage:

    python3 -m mwdumptools.imagedownloader --help

Takes a mediawiki from stdin and parses all titles as file names, so you
need to feed it the namespace of all File:XXX pages. For instance:

    cat dump.xml > python3 -m mwdumptools.imagedownloader --namespaces=6

It will download and place all images in the destined location and send SQL
INSERT statements for populating the images table.

## Python 3

You need Python 3 to use this because it's running futures.concurrent stuff for
parallel processing.

You need pip for Python 3, consider setting up a virtual env:

The following is essential for your Pillow install to process images 
with imagedownloader:

    libjpeg provides JPEG functionality.
    zlib provides access to compressed PNGs
    libtiff provides group4 tiff functionality

    sudo apt-get install libjpeg-dev libtiff4-dev

To reinstall Pillow after adding dependencies, run:

    pip install pillow --force-reinstall --upgrade
