# WaterSlide — a lightweight streaming metadata processor

Updated March 2020

WaterSlide is an event-at-a-time architecture for processing metadata. It is
designed to take in a set of streaming events from multiple sources,
process them through a set of modules ("kids"), and return meaningful outputs.
The user specifies a directed processing graph ("pipeline") of kids used to
process data, both raw content (e.g., files, binary structures) and metadata 
about content. Kids can be filters, aggregators, annotators, decoders,
translators, and collectors. Code is only executed when data is made available
to a processing operation.

WaterSlide can be used as a streaming MapReduce framework for complex event
processing. It is designed to efficiently process data by minimizing copies,
grouping data, and reusing memory. It contains specially designed data
structures intended to explore event correlation on a massive scale with data
that is fragmented across process and systems. As with most stream processing
frameworks, many WaterSlide processing functions favor efficient approximate
computation over less-efficient exact computations.

WaterSlide can be used for a variety of purposes. It is used to generate
interesting metadata from live data streams.

Key Features:

* processing graph built at execution via command line or config file
* processing graph can have feedback loops
* zero-copy data processing, multiple "in flight" references to data
* generic processing functions that can work on any datatype
* anything that can be hashed can be used as a key for tracking state
* simple plug-in style development model
* built to handle text and binary metadata types
* expiring data structures for state tracking
* capable of reading from multiple sources
* Graphviz visualization of processing graph
* dynamic by-label sub-selection of data
* data garbage collection/reuse

## 1.0 WaterSlide SIGNIFICANT CHANGE LOG

### 1.1 VERSION 1.0

#### 1.1.1 version 1.0.0 (Release date: 18 July 2016)

## 2.0 BUILDING AND INSTALLING WaterSlide

### 2.1 BASIC WaterSlide BUILD

#### 1) Set up the build environment

The WaterSlide package is distributed as either a compressed tarball (typically
named `waterslide<datetag>.tar.bzip2`) or via a git repository. Set up your build
environment based on your distribution environment.

**If using Debian or Ubuntu you will need the following packages:**

* build-essential
* flex
* bison
* zlib1g-dev

**Extract the tarball:**

```console
$ tar xvfj waterslide<datetag>.tar.bzip2
```

**Or, clone the git repository:**

```console
$ git clone ssh://<server>/<path-to-repository>
```

A directory named "waterslide" will be created in the current directory.

waterslide relies on the following libraries that must be installed in the build 
environment:

* bison
* flex

#### 2) Build the package

```console
$ cd waterslide
$ make -j
```

Note: In most situations, the `-j` option should be used to take advantage of
multi-threaded compilation in order to complete the building process more 
efficiently.  If there are resource limitations, the number of threads can 
be limited via a parameter (e.g., `-j 8`).

Note: In cases where your build environment already has protobuf libraries, you 
will need to add the `waterslide/bin` directory to the `$PATH` before compiling
(see [Step 3](#3-add-waterslide-to-the-path)). This will ensure that the waterslide
protobuf libraries are properly linked during compilation.

When compilation is complete, the following executables will be placed in the
`waterslide/bin` directory, with symbolic links in the waterslide directory:

* `waterslide`
* `waterslide-parallel`
* `wsman`
* `wsalias`

### 3) Add waterslide to the path

The WaterSlide tools can be directly invoked within the waterslide directory. For
more flexibility, add the `waterslide/bin` directory to the execution `$PATH` shell
variable.

```console
$ source wssetup.sh
```


### 2.2 INSTALLATION

Once the WaterSlide tools have been built, they can be installed into a central
location for general use:

```console
$ cd waterslide
$ sudo make install     # must be privileged user
```

The waterslide files and libraries will be copied into the central location
`/usr/local/waterslide`.

Note: If make install is invoked by an unprivileged user, the environment will
be "installed" into the user's `$(HOME)/local/waterslide` directory.

If desired, the WaterSlide tools can be linked to enable general access to waterslide
functionality:

```console
$ ln -s /usr/local/waterslide/bin/* /usr/local/bin
```

Note: For use in a different environment, waterslide can be installed into a
location of your choosing by setting the environment variable
`WS_INSTALL=<destination_path>` before invoking make install. Each user would
need to add `<destination_path>/bin` to their executable `$PATH` in order to easily
invoke the tools.


### 2.3 BEHIND THE SCENES

The build process for the WaterSlide code base consists of a series of Makefiles
in various subdirectories to create the WaterSlide components. To provide a
consistent set of parameters, there is a make directive file
`src/Makefile.common` that is included by each Makefile. The default build
process starts at the top level directory and invokes the build process twice:
(1) to create the `waterslide` executable and support files ("kids" or "procs") for
use in serial processing; and (2) to create the `waterslide-parallel` environment.

By default, the build output is succinct. To display the specific compilation
commands:

```console
$ export WS_VERBOSEBUILD=1
```

A number of kids that depend on non-standard components are only built by
setting specific environment variables (e.g., `HASPLPLOT=1`) during compilation.
View `src/procs/Makefile` or the output of the make process for details of the 
various build options.

The directory structure for the installed WaterSlide environment is:

* `waterslide/bin`: compiled executables (e.g., `waterslide`, `waterslide-parallel`,
  `wsman`)
* `waterslide/lib`: compiled libraries and special datatypes
* `waterslide/procs`: compiled kids
* `waterslide/config`: processing graphs

This formulaic structure allows the executables to find the support files by
searching relative to the executable's location at runtime. If some components
are stored in a non-standard location, that can be specified by setting
environment variables including `WATERSLIDE_PROC_PATH`, `WATERSLIDE_ALIAS_PATH`, and/or
`WATERSLIDE_CONFIG_PATH`.


### 2.4 CLEANING OR UNINSTALLING WaterSlide

If changes are made to the core WaterSlide source code, you may need to "clean" 
your waterslide build before recompiling the code base:

```console
$ make clean
```

The compiled WaterSlide libraries (not including the waterslide/protobuflib/lib libraries) 
and binaries will be deleted. In some extreme cases, you may also need to rebuild 
the waterslide protobuf libraries:

```console
$ make scour
```

Finally, the WaterSlide tools can also be uninstalled (see [Section 2.2](#22-installation)):

```console
$ sudo make uninstall
```

In this case, the waterslide files and libraries will be deleted from the central 
location `/usr/local/waterslide` (or `$(HOME)/local/waterslide`, if run by an 
unprivileged user).

Note: Uninstalling waterslide does not delete the waterslide binaries or libraries 
that were built in the `$(HOME)/local/waterslide` directory. Use `make clean` or 
`make scour` as described above.


## 3.0 DOCUMENTATION

WaterSlide documentation can be found in the following locations:

* Build/installation process: this document (`waterslide/README`)

* Guide for WaterSlide users: `waterslide/doc/users_guide/`

* Guide for WaterSlide kid developers: `waterslide/doc/developer/`

Additionally, each kid has command-line help that you can access with the `wsman`
tool:

```console
$ wsman -h       # documentation for wsman
$ wsman          # list of all kids
$ wsman print    # documentation for print kid
$ wsman -v print # verbose documentation
$ wsman -t input # input kids
$ wsman -s count  # string search
```

