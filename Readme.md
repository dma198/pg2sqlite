Data export utility PostgreSQL => SQLite
========================================

**USAGE:**

    pg2sqlite [FLAGS] [OPTIONS] <PG URL> <sqlite file> <tables>

**Arguments:**

    <PG URL>         PostgreSQL Connection URL: postgres://[[user]:pass]@<host>/<db>

    <sqlite file>    SQLite File Name

    <tables>         Comma separated table names

**Flags:**


    -c, --compress    Compress destination file. Default is false. NOT IMPLEMENTED!

    -h, --help        Prints help information

    -i, --indexes     Export indexes. Default is false

    -V, --version     Prints version information

**Options:**

    --batchsize <batchsize>    Max count of records in one SQLite transaction [default: 10000]

