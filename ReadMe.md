Spring Batch Sort-Merge
=======================

Overview
--------
This package contains general purpose flat file sorting and merging
tasklets for use within Spring Batch applications.  These tasklets
are designed to function in out-of-core scenarios where very large
input files may not fit into main memory.

Building
--------
This project is built using <a href="http://www.gradle.org">Gradle</a>.
To build, simply type the following at the command prompt while in
the directory with the `build.gradle` file:
	gradle build

The jar will subsequently be generated in build/libs.

To build an Eclipse project file, use the following command:
	gradle eclipse

Usage
-----

### Sorting ###
Sorting is accomplished by creating a new instance of `FlatFileSortTasklet`
and setting the following properties:

*inputResource* - A `FileSystemResource` indicating the flat file that
should be sorted.

*outputResource* - A `FileSystemResource` indicating the output file that
should be created.

*inputIoFactory* - An instance of `FlatFileItemIoFactory` that corresponds
with the *inputResource*.  Implementations of `FlatFileItemIoFactory` can
be written to define an item reader and writer that correspond to a given
file format (eg. a csv file with columns: id,age,name).

*outputIoFactory* - An instance of `FlatFileItemIoFactory` that corresponds
with the output file.

*tmpDirectory* - A temporary writeable directory available to the tasklet in
the event that the entire input file does not fit into main memory and
temporary files must be created.

*comparator* - Defines the relationship between file record objects and how they should be sorted.

### Merging ###
Merging files into a single file can be accomplished by creating an instance
of `FlatFileMergeTasklet` and setting the following properties:

*readers* - A list of `FlatFileItemReader` corresponding to the input files
to merge.

*writer* - A `FlatFileItemWriter` that corresponds to the output (merged)
file.

*comparator* - A Comparator that defines the relationship between input
file records and how they should be ordered.

Note that `FlatFileMergeTasklet` assumes that its input files are already
sorted.  If this cannot be guaranteed, sort them witih `FlatFileSortTasklet`
prior to the merge operation.

License
-------
The MIT License (MIT)

Copyright (c) 2013 Rob Upcraft

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
