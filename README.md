python-mwdump-tools
===================

Quick parsing of Mediawiki XML dumps: Parses stdin XML dumps using simple
string searching and Python's C implementation of elementree for parsing each
`<page>` node.

## TODO

 - docs
 - Examples of other parsing than just image downloading
 - Packaging for PIP

## Features

### Fast

The outermost parsing will not try to parse the whole XML dump but simply
moves from `<page>` to `</page>` to allow for small buffers and quick
deployment of jobs.

### Mulitiprocessing

Since Python 3 has truly parallel job tasking, all these I/O heavy
tasks for parsing revision texts, downloading related files etc. can be
performed with maximum utility of a single server.

### Resuming and skipping

Where applicaple, jobs can be resumed by parsing in a line number from which
the job should start.

If a job finds that something has already been processed, it will skip this.

### Super configurable

Most behaviour can be configured.

## Commands

### imagedownloader

Downloads and downsamples images found in an XML dump.

Usage:

    ./imagedownloader --help

Takes a mediawiki from stdin and parses all titles as file names, so you
need to feed it the namespace of all File:XXX pages. For instance:

    ./imagedownloader --namespaces=6 < mywiki.dump

It will download and place all images in the destined location and send SQL
INSERT statements for populating the images table.

### patternmatcher (TODO)

Reads a list of Python regular expressions and counts their occurences in a dump.

### patternreplacer (TODO)

Replaces a list of (search, replace) pairs of Python

### autotranslator (TODO)

Idea sketch: Call online API or other cloud translation service for translation
of article text to output a translated XML dump.

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
