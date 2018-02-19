# Introduction

filesdb is a simple utility to of files generated under different experimental
conditions.

# Motivation

Keeping track of files generated in computational experiments is very important,
and gets complicated quickly. For simple problems, using filenames is sufficient
(e.g., `a-1_b-2.txt`). However, sometimes code changes, or more parameters
become important, and now filenames may look like
`a-1_b-2_c-5_version-2.txt`, breaking your naming scheme and
complicating any code you might have that parses file names.

filesdb solves these problems by tracking all your files in an sqlite database.
Additional parameters can be added at any time. The database can quickly be
searched by parameter values using a command line or python interface.

# Examples

Say you are have some code that generates an output file and takes two
parameters, a and b. To get a new filename and add it to the database:

| Bash | Python |
| - | - |
| `filename=$(filesdb add a=${a} b=${b})` | `filename = filesdb.add({'a': a, 'b': b})` |

Then `${filename}` can be passed as the output parameter to your code. (filesdb
does not actually create the file.) This process can be repeated for different
values of a and b. You can also specify the filename yourself:

| Bash | Python |
| - | - |
| `filename=$(filesdb add --filename=myfile.txt a=${a} b=${b})` | `filesdb.add({'a': a, 'b': b}, filename='myfile.txt')` |

Note that keys cannot end in "!".

To list all the files with a=`${a}`

| Bash | Python |
| - | - |
| `filesdb search a=${a}` | `entries = filesdb.search({'a': a})` |

Later, if you decided you also want to track files by a new parameter, (e.g,
c), you can simply add a new parameter:

| Bash | Python |
| - | - |
| `filename=$(filesdb add a=${a} b=${b} c=${c})` | `filename = filesdb.add({'a': a, 'b': b, 'c': c})` |

Finally, let's say you discover a bug and your code, and all files generated
with a=0 are invalid and you decide to delete them.

| Bash | Python |
| - | - |
| `filesdb delete a=0` | `deleted_entries = filesdb.delete({'a': 0})` |

This WILL delete the file from the file system as well as remove it from the
database. The bash version will also print the deleted entries. To make sure
that you're only deleting the files you want, delete has a dry run option, which
prints (or returns, in the python case) the entries to be deleted but does not
actually delete them.

| Bash | Python |
| - | - |
| `filesdb delete --dry_run a=0` | `entries = filesdb.delete({'a': 0}, dryrun=True)` |

# Advanced Features

## not equal comparisons

Both search and delete support "not equal" comparisons.

| Bash | Python |
| - | - |
| `filesdb search a!=${a}` | `entries = filesdb.search({'a!': a})` |

Note that this syntax prevents keys from ending in "!".

## copy

The copy command copies a file and its data base entry to a new directory. For
example:

| Bash | Python |
| - | - |
| N/A | `filesdb.copy(filename, outdir, db='files.db', wd='.', outdb='files.db')` |

Will copy the `${filename}` in the current directory to `${outdir}`,
and copy its row entry to `files.db` in the current director to
`files.db` in `${outdir}`.

Note that this method does not currently have a command line interface

## merge

Merge one database into another.

| Bash | Python |
| - | - |
| `filesdb --wd='.' --db=files.db merge files2.db` | `filesdb.merge('files2.db', 'files.db', wd='.')` |

will add all entries from `files2.db` to `files.db` (if they aren\'t already
present)
