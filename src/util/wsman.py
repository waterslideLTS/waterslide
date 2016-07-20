#!/usr/bin/env python

#
# wsman.py: a python version of WaterSlide's wsman
#
# wsman.py is a prototype and is missing features such as searching and alias
# lookup that are found in the original wsman. These functionalities would be 
# trivial to implement if they're desired. 

# In a perfect world, We would be able to use the dl or ctypes python modules
# to load the waterslide .so's, but these are modules are deprecated or don't
# work correctly with different dynamic load bindings (respectively). So
# instead we're left to parse the ELF sections manually :( therefore making
# this script only work on Linux systems that just happen to have the elftools
# python library installed. Which is basically zero...

from __future__ import print_function

import optparse
import os
import struct
import sys
import textwrap

from elftools.common.py3compat import str2bytes
from elftools.elf.elffile      import ELFFile
from elftools.elf.sections     import SymbolTableSection

if sys.stdout.isatty():
    CYAN   = "\33[36m{0}\33[m\33[K"
    YELLOW = "\33[33m{0}\33[m\33[K"
else:
    CYAN   = "{0}"
    YELLOW = "{0}"

def print_wrap(text, width=80, init_indent="    ", subseq_indent="    "):
    """ A wrapper around text.wrap with app WS-specific formatting. """
    print(textwrap.fill(text, width=width,initial_indent=init_indent, 
        subsequent_indent=subseq_indent))

def print_header(text):
    """ Helper function to print documentation section headers. """
    print(CYAN.format(text))

def print_ports(port_list):
     """ Helper function to print input ports. """
     if not port_list: 
         print("    none")
         return
     while ii < len(port_list):
          print("    :%-4s - %s" % (port_list[ii], port_list[ii+1]))
          ii += 2

def highlight_synopsis(raw_string):
    """ Highlight the dash flags in a synopsis. """
    result = []
    rest = raw_string
    while True:
        head, sep, tail = rest.partition("-")
        if not sep: break
        flag = YELLOW.format(sep + tail[0])
        result.append(head + flag)
        rest = tail[1:]
    result.append(rest)
    return ''.join(result)

def print_synopsis(synopsis):
    """ Helper function to print a synopsis. """
    # I was doing something fancier here but textwrap loses its mind when it
    # sees non ascii so we have to live with synopsis long lines. ugh python
    print("    %s" % highlight_synopsis(synopsis))

class KidDoc(object):
    # I think I'm breaking a some PEP rule by using kargs in __init__
    # but YOLO or whatever the kids say these days
    def __init__(self, **kargs):
        self.description   = kargs.pop("proc_description")
        self.input_ports   = kargs.pop("proc_input_ports", [])
        self.input_types   = kargs.pop("proc_input_types", ["none"])
        self.name          = kargs.pop("proc_name")
        self.output_types  = kargs.pop("proc_output_types", ["none"])
        self.purpose       = kargs.pop("proc_purpose")
        self.requires      = kargs.pop("proc_requires", "none")
        self.synopsis      = kargs.pop("proc_synopsis", ["none"])
        self.tags          = kargs.pop("proc_tags", ["none"])
        self.version       = kargs.pop("proc_version")

        self.print_verbose = kargs.pop("print_verbose", False)

    def __str__(self):
        return self.name

    def print_kid(self):
        print_header("PROCESSOR NAME:")
        print_wrap("%s - %s" % (self.name, self.purpose))
        print("")
        print_header("SYNOPSIS:")
        print_synopsis(self.synopsis[0])
        print("")
        #print_header("VERSION:")
        #print_wrap(self.version)
        #print("")
        if self.print_verbose:
            print_header("DESCRIPTION:")
            # Print each paragraph word wrapped
            for paragraph in self.description.split("\n"):
                print_wrap(paragraph)
            print("")
        #if self.print_verbose:
        #    print_header("REQUIRES:")
        #    print_wrap(self.requires)
        #    print("")
        #if self.print_verbose:
        #    print_header("TAGS:")
        #    print_wrap(", ".join(map(str, self.tags)))
        #    print("")
        print_header("INPUT PORTS:")
        print_ports(self.input_ports)
        print("")
        if self.print_verbose:
            print_header("INPUT TYPES:")
            print_wrap(", ".join(map(str, self.input_types)))
            print("")
        if self.print_verbose:
            print_header("OUTPUT TYPES:")
            print_wrap(", ".join(map(str, self.output_types)))
            print("")

def get_list(data_data, rodata_data, rodata_section_addr, offset, size):
    results = []
    while True:
        rodata_offset_str = data_data[offset:offset+4]
        if len(rodata_offset_str) == 0:
            break

        ro_st_value = struct.unpack("<L", rodata_offset_str)[0]

        if ro_st_value == 0:
            break

        ## get the offset into .rodata
        ro_offset = ro_st_value - rodata_section_addr

        # iterate until a NULL terminator is found
        ii = 0; result = ""; byte = ""
        while byte != "\x00":
            byte = rodata_data[ro_offset+ii]
            result += byte
            ii += 1
        results.append(result[:-1])
        offset += 8
    return results

def kiddoc_from_shared_library(libpath, verbose_print):
    """ Do ELF symbol resolution to build a KidDoc class instance 
        from a compiled .ws_so. hc svnt dracones, etc..."""
    fp = open(libpath, "rb")
    elf = ELFFile(fp)

    # First, find the .data and .rodata sections
    data_section = elf.get_section_by_name(str2bytes(".data"))
    rodata_section = elf.get_section_by_name(str2bytes(".rodata"))

    # We found .data and .rodata, now get their addresses and bytes.
    data_section_addr = data_section.header.sh_addr
    data_data = data_section.data()
    rodata_section_addr = rodata_section.header.sh_addr
    rodata_data = rodata_section.data()

    # Next, look for the symbol table.
    symbol_table_section = None
    for section in elf.iter_sections():
        if not isinstance(section, SymbolTableSection):
            continue
        #if section["sh_entsize"] == 0:
        #    continue
        symbol_table_section = section
        break

    if not symbol_table_section:
         return None

    # By this point we've found the symbol table.

    docsymbols = [ # symbols in .data
         "proc_description",
         "proc_name",
         "proc_purpose",
         "proc_requires",
         "proc_version"
    ]

    docsymbolsptr = [ # symbols in .data that need to be dereferenced 
         "proc_input_ports",
         "proc_input_types",
         "proc_synopsis"
    ]

    doc_dict = {}

    # We found the symbol table, now iterate through its symbols.
    # When we encounter a symbol needed for documentation (see docsymbols),
    # we look for the corresponding bytes in the .data and .rodata sections.
    for symbol in symbol_table_section.iter_symbols():
        if (section["sh_type"] == "SHT_DYNSYM"):
            name = str(symbol.name)

            # Skip if this isn't a symbol needed for documentation.
            if name not in docsymbols and name not in docsymbolsptr:
                continue

            offset = symbol["st_value"] - data_section_addr
            size = symbol["st_size"]

            if name in docsymbolsptr:
                 symdata = get_list(data_data, rodata_data, rodata_section_addr, offset, size)
            else:
                # the -1 is to remove the NUL terminator (we already know it's
                # a string)
                symdata = data_data[offset:offset+size-1]
                if symdata == "":
                    symdata = "none"

            doc_dict[name] = symdata

    doc_dict["print_verbose"] = verbose_print 
    return KidDoc(**doc_dict)

def _build_parser():
    """Return a parser for the command-line interface."""
    usage = "usage: %prog [-v] kid"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", 
                      default=False, help="give detailed documentation")
    return parser

def kidname_to_libpath(kidname):
    """ Given the name of a WS kid, return the path to shared library. """
    serial_libpath = os.path.join("procs", "proc_" + kidname + ".ws_so")
    parallel_libpath = os.path.join("procs", "proc_" + kidname + ".wsp_so")
    if os.path.exists(serial_libpath):
         return serial_libpath
    if os.path.exists(parallel_libpath):
         return parallel_libpath
    return None 

def _main():
     """ Run the command line interace. """
     print("wsman : the WaterSlide documentation system. (IN PYTHON)")
     print("")

     (options, args) = _build_parser().parse_args()

     for arg in args:
         libpath = kidname_to_libpath(arg)
         if libpath:
             kiddoc = kiddoc_from_shared_library(libpath, options.verbose)
             kiddoc.print_kid()
         else: 
             print("[ERROR] Cannot find %s." % arg )
             sys.exit(1)

if __name__ == "__main__": 
    _main()
