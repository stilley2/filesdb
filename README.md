# Introduction

filesdb is a simple utility to of files generated under different experimental
conditions.

# Motivation

Keeping track of files generated in computational experiments is very important,
and gets complicated quickly. For simple problems, using filenames is sufficient
(e.g., `alpha-1_beta-2.txt`). However, sometimes code changes, or more parameters
become important, and now filenames may look like
`alpha-1_beta-2_delta-5_version-2.txt`, breaking your naming scheme and
complicating any code you might have that parses file names.

filesdb solves these problems by tracking all your files in an sqlite database.
Additional parameters can be added at any time. The database can quickly be
searched by parameter values using a command line or python interface.

# Examples

Say you are have some code that generates an output file and takes two
parameters, alpha and beta. To get a new filename and add it to the database:

| Bash | Python |
| - | - |
| `filename=$(filesdb add alpha=${alpha} beta=${beta})` | `filename = filesdb.add({'alpha': alpha, 'beta': beta})` |

Then `${filename}` can be passed as the output parameter to your code. (filesdb
does not actually create the file.) This process can be repeated for different
values of alpha and beta. You can also specify the filename yourself:

| Bash | Python |
| - | - |
| `filename=$(filesdb add --filename=myfile.txt alpha=${alpha} beta=${beta})` | `filesdb.add({'alpha': alpha, 'beta': beta}, filename='myfile.txt')` |

Note that keys cannot end in "!".

To list all the files with alpha=`${alpha}`

| Bash | Python |
| - | - |
| `filesdb search alpha=${alpha}` | `entries = filesdb.search({'alpha': alpha})` |

Later, if you decided you also want to track files by a new parameter, (e.g,
delta), you can simply add a new parameter:

| Bash | Python |
| - | - |
| `filename=$(filesdb add alpha=${alpha} beta=${beta} delta=${delta})` | `filename = filesdb.add({'alpha': alpha, 'beta': beta, 'delta': delta})` |

Finally, let's say you discover a bug and your code, and all files generated
with alpha=0 are invalid and you decide to delete them.

| Bash | Python |
| - | - |
| `filesdb delete alpha=0` | `deleted_entries = filesdb.delete({'alpha': 0})` |

This WILL delete the file from the file system as well as remove it from the
database. The bash version will also print the deleted entries. To make sure
that you're only deleting the files you want, delete has a dry run option, which
prints (or returns, in the python case) the entries to be deleted but does not
actually delete them.

| Bash | Python |
| - | - |
| `filesdb delete --dry_run alpha=0` | `entries = filesdb.delete({'alpha': 0}, dryrun=True)` |

# Advanced Features

## not equal comparisons

Both search and delete support "not equal" comparisons.

| Bash | Python |
| - | - |
| `filesdb search alpha!=${alpha}` | `entries = filesdb.search({'alpha!': alpha})` |

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
