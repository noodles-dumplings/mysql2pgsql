#!/usr/bin/env python

import sys
import optparse, logging
import re, collections
import MySQLdb, psycopg2
from MySQLdb.cursors import DictCursor

def pg_execute(pg_conn, options, sql, args=()):
    """(Connection, Options, str, tuple)

    Log and execute a SQL command on the PostgreSQL connection.
    """
    print sql
    if not options.dry_run:
        pg_cur = pg_conn.cursor()
        pg_cur.execute(sql, args)

# XXX need to expand this set of words.
_reserved_words = set("""end""".split())

def is_reserved_word(word):
    """(str): bool

    Returns true if this word is a PostgreSQL reserved-word.
    """
    return word in _reserved_words
    
def convert_type(typ):
    """(str): str

    Parses a MySQL type declaration and returns the corresponding PostgreSQL
    type.
    """
    if re.match('tinyint([(]\d+[)])?', typ):
        # MySQL tinyint is 1 byte, -128 to 127; we'll use the 2-byte int.
        return 'smallint'
    elif re.match('smallint([(]\d+[)])?', typ):
        return 'smallint'
    elif re.match('mediumint([(]\d+[)])?', typ):
        # MySQL medium int is 3 bytes; we'll use the 4-byte int.
        return 'integer'
    elif re.match('bigint([(]\d+[)])?', typ):
        # XXX use the parametrized number?
        # XXX 'bigint NOT NULL auto_increment' -> bigserial
        return 'bigint'
    elif re.match('integer([(]\d+[)])?', typ):
        return 'integer'
    elif re.match('int([(]\d+[)])?', typ):
        return 'integer'
    elif typ == 'float':
        return 'real'
    elif typ == 'double':
        return 'double precision'
    elif typ == 'datetime':
        return 'timestamp'
    elif typ == 'mediumtext':
        return 'text'
    elif typ == 'blob':
        return 'bytea'

    # Give up and just return the input type.
    return typ

def convert_data(col, data):
    """(Column, any) : any

    Convert a Python value retrieved from MySQL into a PostgreSQL value.
    """

class Column:
    """
    Represents a column.

    Instance attributes:
    name : str
    type : str
    position : int
    default : str
    is_nullable : bool

<    """

    def __init__(self, **kw):
        for k,v in kw.items():
            setattr(self, k, v)

    def pg_decl(self):
        """(): str

        Return the PostgreSQL declaration syntax for this column.
        """
        typ = convert_type(self.type)
        name = self.name
        if is_reserved_word(name):
            name += '_'
        decl = '  %s %s' % (name, typ)
        if self.default:
            decl += ' DEFAULT %s' % self.default
        if not self.is_nullable:
            decl += ' NOT NULL'
        return decl

class Index:
    """
    Represents an index.

    Instance attributes:
    name : str
    table : str
    type : str
    column_names : [str]
    non_unique : bool
    nullable : bool

    """

    def __init__(self, **kw):
        self.column_names = []
        for k,v in kw.items():
            setattr(self, k, v)

    def pg_decl(self):
        """(): str

        Return the PostgreSQL declaration syntax for this index.
        """
        # We'll ignore the MySQL index name, and invent a new name.
        name = 'idx_' + '_'.join([self.table] + self.column_names)
        sql = 'CREATE INDEX %s ON %s(%s)' % (name, self.table,
                                             ','.join(self.column_names))
        if self.type:
            # XXX convert index_type:
            # BTREE, etc.
            pass
        return sql



def main ():
    parser = optparse.OptionParser(
        '%prog [options] mysql-host mysql-db pg-host pg-db')
    parser.add_option('--mysql-user',
                      action="store",
                      dest="mysql_user",
                      help="User for login if not current user.")
    parser.add_option('--mysql-password',
                      action="store",
                      dest="mysql_password",
                      help="Password to use when connecting to server.")
    parser.add_option('--pg-user',
                      action="store",
                      dest="pg_user",
                      help="User for login if not current user.")
    parser.add_option('--pg-password',
                      action="store", default='',
                      dest="pg_password",
                      help="Password to use when connecting to server.")
    parser.add_option('-n', '--dry-run',
                      action="store_true", default=False,
                      dest="dry_run",
                      help="Make no changes to PostgreSQL database")
    parser.add_option('--drop-tables',
                      action="store_true", default=False,
                      dest="drop_tables",
                      help="Drop existing PostgreSQL tables (if any) before creating")
    parser.add_option('--starting-table',
                      action="store", default=None,
                      dest="starting_table",
                      help="Name of table to start conversion with")

    options, args = parser.parse_args()
    if len(args) != 4:
        parser.print_help()
        sys.exit(1)

    mysql_host, mysql_db, pg_host, pg_db = args

    # Set up connections
    mysql_conn = MySQLdb.Connection(
        user=options.mysql_user,
        passwd=options.mysql_password,
        db=mysql_db,
        host=mysql_host,
        )
    pg_conn = psycopg2.connect(
        database=pg_db,
        host=pg_host,
        user=options.pg_user,
        password=options.pg_password,
        )
    mysql_cur = mysql_conn.cursor(cursorclass=DictCursor)

    # Make list of tables to process.
    mysql_cur.execute("""
SELECT * FROM information_schema.tables WHERE table_schema = %s
""", mysql_db)
    rows = mysql_cur.fetchall()
    tables = sorted(row['TABLE_NAME'] for row in rows)
    if options.starting_table:
        tables = [t for t in tables if options.starting_table <= t]

    # Convert tables
    table_cols = {}
    for table in tables:
        mysql_cur.execute("""
SELECT * FROM information_schema.columns
WHERE table_schema = %s and table_name = %s
""", (mysql_db, table))
        cols = table_cols[table] = []
        for row in mysql_cur.fetchall():
            c = Column()
            cols.append(c)
            c.name = row['COLUMN_NAME']
            c.type = row['COLUMN_TYPE']  #
            c.position = row['ORDINAL_POSITION']
            c.default = row['COLUMN_DEFAULT']
            c.is_nullable = bool(row['IS_NULLABLE'] == 'YES')
            # XXX character set?

        # Sort columns into left-to-right order.
        cols.sort(key=lambda c: c.position)

        # Convert indexes
        mysql_cur.execute("""
SELECT * FROM information_schema.statistics
WHERE table_schema = %s AND table_name = %s
""", (mysql_db, table))
        d = collections.defaultdict(Index)
        for row in mysql_cur.fetchall():
            index_name = row['INDEX_NAME']
            i = d[index_name]
            i.table = table
            i.name = index_name
            i.column_names.append(row['COLUMN_NAME'])
            i.type = row['INDEX_TYPE']
            i.non_unique = bool(row['NON_UNIQUE'])
            i.nullable = bool(row['NULLABLE'] == 'YES')
        indexes = d.values()

        # Drop table if necessary.
        if options.drop_tables:
            sql = "DROP TABLE IF EXISTS %s" % table
            pg_execute(pg_conn, options, sql)
            
        # Assemble into a PGSQL declaration
        sql = "CREATE TABLE %s (\n" % table
        sql += ',\n'.join(c.pg_decl() for c in cols) + '\n'

        # Look for index named PRIMARY, and add PRIMARY KEY if found.
        primary_L = [i for i in indexes if i.name == 'PRIMARY']
        if len(primary_L):
            if len(primary_L) >  1:
                logging.warn('%s: Multiple PRIMARY indexes on table',
                             table)
            else:
                primary = primary_L.pop()
                sql = sql.rstrip() + ',\n'
                sql += '  PRIMARY KEY (%s)' % ','.join(primary.column_names)

        sql += ');'
        pg_execute(pg_conn, options, sql)

        # Create indexes
        for i in indexes:
            if i.name == 'PRIMARY':
                continue

            sql = i.pg_decl()
            pg_execute(pg_conn, options, sql)


    for table in tables:
        # Convert data.
        mysql_cur.execute("SELECT * FROM %s", table)
        cols = table_cols[table]

        # Assemble the INSERT statement once.
        ins_sql = ('INSERT INTO %s (%s) VALUES (%s);' %
                   (table,
                    ', '.join(c.name for c in cols),
                    ','.join(['%s'] * len(cols))))

        # We don't do a fetchall() since the table contents are
        # very likely to not fit into memory.
        while True:
            row = mysql_cur.fetchone()
            if row is None:
                continue

            # Assemble a list of the output data that we'll subsequently
            # convert to a tuple.
            output_L = []
            for c in cols:
                data = row[c.name]
                newdata = convert_data(c, data)
                output_L.append(newdata)

            pg_execute(pg_conn, options, ins_sql, tuple(output_L))

        pass

    # Close connections
    mysql_conn.close()
    pg_conn.close()





if __name__ == '__main__':
    main()
