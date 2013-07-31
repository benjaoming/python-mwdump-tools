# -*- coding: utf-8 -*-
import sys

from xml.etree import cElementTree as etree

from mwdumptools import settings
import io
from datetime import datetime

class ParseError(Exception):
    pass


class Parser:
    """
    Extend from this class to create your own parser methods
    
    NB! Each parser method has its own special contract
    
    E.g.
    
    class MyParser(Parser):
        def parse_schema(self, lines, *args, **kwargs):
            # Pre processing here...
            output = super(MyParser, self).parse_schema(lines, *args, **kwargs)
            # Post processing here...
            return output
    """
    
    def __init__(self, in_file=None, out_file=None, err_file=None, **kwargs):
        
        if isinstance(in_file, str):
            self._in_stream  = open(in_file)
        elif not in_file is None:
            self._in_stream = in_file
        else:
            self._in_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        if isinstance(out_file, str):
            self._out_stream  = open(out_file)
        elif not out_file is None:
            self._out_stream = in_file
        else:
            self._out_stream = sys.stdout.encoding

    def execute(self):
        raise NotImplementedError("You need to overwrite execute with your own calls")


class XmlStreamParser(Parser):
    
    def __init__(self, in_file=None, out_file=None, **kwargs):
        Parser.__init__(self, in_file=in_file, out_file=out_file, **kwargs)
        self._schema_version = kwargs.get(
            "schema_version", "0.8"
        )
        self._schema_location = kwargs.get(
            "schema_location", 
            "http://www.mediawiki.org/xml/export-%s/" % self._schema_version
        )
        self._schema = kwargs.get(
            "schema", 
            "http://www.mediawiki.org/xml/export-%s.xsd" % self._schema_version
        )
        self._generator = kwargs.get(
            "generator",
            "MediaWiki 1.22wmf8"
        )
        self.resume = kwargs.get("resume", 0)
        self.line_no = 0 # Maintain line count for resuming
        self.pages_processed = 0
    
    def parse_etree(self, lines, start_tag):
        start_tag = "<" + start_tag + ">"
        if not start_tag in lines[0]:
            raise ParseError("Expected: " + start_tag)
        lines[0].replace(start_tag, "")
        return etree.fromstring("\n".join(lines))
                
    
    def parse_site_info(self, lines):
        siteinfo = self.parse_etree(lines, "siteinfo")
        namespaces = siteinfo.find("namespaces")
        if not namespaces:
            raise ParseError("No namespaces defined")
        self._namespaces = {}
        for namespace in namespaces.findall("namespace"):
            self._namespaces[namespace.get("key")] = namespace.text
        try:
            self.generator = siteinfo.find("generator").text
        except AttributeError:
            raise ParseError("No siteinfo generator")
        if not self.generator == self._generator:
            settings.logger.warning(
                "Expected generator: " + self._generator + \
                ", generator found:" + self.generator
            )
        self.base = siteinfo.find("base").text
        self.case = siteinfo.find("case").text
        self.sitename = siteinfo.find("sitename").text
        settings.logger.info("Now parsing dump: {}".format(self.sitename))
            
        
    def parse_schema(self, lines):
        beginning = (
           "<mediawiki xmlns=\"{}\" xmlns:xsi=\"http://www.w3.org/2001/"
           "XMLSchema-instance\" xsi:schemaLocation=\"{} {}\" version=\"{}\"".format(
                self._schema_location, 
                self._schema_location, 
                self._schema, 
                self._schema_version
            )
        )
        is_ok = any([beginning in line for line in lines])
        if not is_ok:
            raise ParseError("Illegal schema")
        return is_ok


    def execute(self):
        """ Check schema - just check the first line
        """
        ln = self._in_stream.readline().strip()
        if not self.parse_schema([ln]):
            return
        
        started_on = datetime.now()
        
        # Site info
        lines = []
        while ln != "</siteinfo>":
            ln = self._in_stream.readline().strip()
            lines.append(ln)
        self.parse_site_info(lines)
        
        if self.resume:
            settings.logger.debug("Spooling forward to line: {}".format(self.resume))
        
        # Main parser
        while not self._in_stream.closed:
            ln = self._in_stream.readline().strip()
            self.line_no += 1
            if ln == "<page>":
                page_lines = [ln]
                while ln != "</page>":
                    ln = self._in_stream.readline()
                    page_lines.append(ln)
                    ln = ln.strip()
                page = self.parse_etree(page_lines, "page")
                self.handle_page(page)
                if self.pages_processed % 1000 == 0:
                    process_time = started_on - datetime.now()
                    pps = self.pages_processed / process_time.total_seconds()
                    settings.logger.info("Processed {} pages - {} pages per second".format(
                        self.pages_processed, pps
                    ))
                self.pages_processed += 1
            elif ln == "</mediawiki>":
                settings.logger.debug("Successfully finished parsing -- waiting for sub processes to finish")
                break
            else:
                settings.logger.debug("Did not understand " + ln)
            
            
    def handle_page(self, page):
        settings.logger.debug(page.find("title").text)
        
    
if __name__ == "__main__":

    p = XmlStreamParser()
    try:
        p.execute()
    except ParseError as e:
        settings.logger.error("Failed to parse, line no: {}".format(p.line_no))
        settings.logger.error(e)
        settings.logger.error("You can set resume={} after fixing to resume".format(p.line_no))