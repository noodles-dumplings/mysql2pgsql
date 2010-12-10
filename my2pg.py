#!/usr/bin/env python

"""my2pg.py: MySQL to PostgreSQL database conversion

Copyright (c) 2010 Matrix Group International.
Licensed under the MIT license; see LICENSE file for terms.

"""

import sys, os
import optparse, logging, traceback
import re, collections, pickle

import MySQLdb, psycopg2
from MySQLdb.cursors import DictCursor

def pg_execute(pg_conn, options, sql, args=()):
    """(Connection, Options, str, tuple)

    Log and execute a SQL command on the PostgreSQL connection.
    """
    #print sql
    if not options.dry_run:
        pg_cur = pg_conn.cursor()
        pg_cur.execute(sql, args)

# XXX need to expand this set of words.
_reserved_words = set("""end user""".split())

def is_reserved_word(word):
    """(str): bool

    Returns true if this word is a PostgreSQL reserved-word.
    """
    return word in _reserved_words

def fix_reserved_word(S):
    """(str): str

    Takes a MySQL name, and adds an underscore if it's a PostgreSQL
    reserved word.
    """
    if is_reserved_word(S.lower()):
        S += '_'
    return S

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
    elif typ in ('tinytext', 'text', 'mediumtext', 'longtext'):
        return 'text'
    elif typ in ('tinyblob', 'blob', 'mediumblob', 'longblob'):
        return 'bytea'

    # Give up and just return the input type.
    return typ

def convert_data(col, data):
    """(Column, any) : any

    Convert a Python value retrieved from MySQL into a PostgreSQL value.
    """
    if isinstance(data, str):
        data = data.decode('latin-1')
        data = data.encode('utf-8')

    if col.type in ('tinyblob', 'blob', 'mediumblob', 'longblob') and data:
        # Convert to a BYTEA literal.  We just use octal escapes for
        # everything.
        L = [('\\%03o') % ord(ch) for ch in data]
        data = ''.join(L)
    return data

class Column:
    """
    Represents a column.

    Instance attributes:
    name : str
    type : str
    position : int
    default : str
    is_nullable : bool

    """

    def __init__(self, **kw):
        for k,v in kw.items():
            setattr(self, k, v)

    def pg_decl(self):
        """(): str

        Return the PostgreSQL declaration syntax for this column.
        """
        typ = convert_type(self.type)
        decl_typ = typ
        decl = '  %s %s' % (fix_reserved_word(self.name), decl_typ)
        if self.default:
            default = self.get_default()
            decl += ' DEFAULT %s' % default
        if not self.is_nullable:
            decl += ' NOT NULL'
        return decl

    def get_default(self):
        if (self.type in ('datetime', 'timestamp') and
            self.default == "0000-00-00 00:00:00"):
            return 'NULL'

        typ = convert_type(self.type)
        if typ.startswith(('char', 'varchar')):
            return "'" + self.default + "'"
            
        return self.default

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
        sql = 'CREATE INDEX %s ON %s(%s)' % (fix_reserved_word(name),
                                             fix_reserved_word(self.table),
                                             ','.join(self.column_names))
        if self.type:
            # XXX convert index_type:
            # BTREE, etc.
            pass
        return sql

def read_mysql_tables(mysql_cur, mysql_db, options):
    """(Cursor):
    """
    logging.info('Reading structure of MySQL database')
    mysql_cur.execute("""
SELECT * FROM information_schema.tables WHERE table_schema = %s
""", mysql_db)
    rows = mysql_cur.fetchall()
    tables = sorted(row['TABLE_NAME'] for row in rows)
    if options.starting_table:
        tables = [t for t in tables if options.starting_table <= t]
        
    # Convert tables
    table_cols = {}
    table_indexes = {}
    for table in tables:
        logging.debug('Reading table %s', table)
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
        table_indexes[table] = d.values()

    return tables, table_cols, table_indexes
    

def main ():
    parser = optparse.OptionParser(
        '%prog [options] mysql-host mysql-db pg-host pg-db')
    parser.add_option('--data-only',
                      action="store_true", default=False,
                      dest="data_only",
                      help="Assume the tables already exist, and only convert data")
    parser.add_option('--drop-tables',
                      action="store_true", default=False,
                      dest="drop_tables",
                      help="Drop existing PostgreSQL tables (if any) before creating")
    parser.add_option('-n', '--dry-run',
                      action="store_true", default=False,
                      dest="dry_run",
                      help="Make no changes to PostgreSQL database")
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
    parser.add_option('--pickle=',
                      action="store", default='',
                      dest="pickle",
                      help="Password to use when connecting to server.")
    parser.add_option('--starting-table',
                      action="store", default=None,
                      dest="starting_table",
                      help="Name of table to start conversion with")
    parser.add_option('-v', '--verbose',
                      action="count", default=0,
                      dest="verbose",
                      help="Display more output as the script runs")

    options, args = parser.parse_args()
    if len(args) != 4:
        parser.print_help()
        sys.exit(1)

    mysql_host, mysql_db, pg_host, pg_db = args

    # Set logging level.
    if options.verbose:
        logging.basicConfig(level=logging.INFO)
        if options.verbose > 1:
            logging.basicConfig(level=logging.DEBUG)

    # Set up connections
    logging.info('Connecting to databases')
    
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
    if options.pickle and os.path.exists(options.pickle):
        f = open(options.pickle, 'rb')
        tables, table_cols, table_indexes = pickle.load(f)
        f.close()
    else:
        tables, table_cols, table_indexes = read_mysql_tables(mysql_cur,
                                                              mysql_db,
                                                              options)
        if options.pickle and not options.starting_table:
            f = open(options.pickle, 'wb')
            t = (tables, table_cols, table_indexes)
            pickle.dump(t, f)
            f.close()
        
    if not options.data_only:
        for table in tables:
            cols = table_cols[table]
            indexes = table_indexes[table]

            # Drop table if necessary.
            if options.drop_tables:
                sql = "DROP TABLE IF EXISTS %s" % fix_reserved_word(table)
                pg_execute(pg_conn, options, sql)

            # Assemble into a PGSQL declaration
            pg_table = fix_reserved_word(table)
            sql = "CREATE TABLE %s (\n" % pg_table
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

    logging.info('Converting data')
    for table in tables:
        # Convert data.
        logging.debug('Converting data in table %s', table)
        pg_table = fix_reserved_word(table)
        cols = table_cols[table]

        # Assemble the INSERT statement once.
        ins_sql = ('INSERT INTO %s (%s) VALUES (%s);' %
                   (pg_table,
                    ', '.join(fix_reserved_word(c.name) for c in cols),
                    ','.join(['%s'] * len(cols))))

        # Ensure the table is empty.
        pg_execute(pg_conn, options, "DELETE FROM %s" % pg_table)

        mysql_cur.execute("SELECT * FROM %s" % table)

        # We don't do a fetchall() since the table contents are
        # very likely to not fit into memory.
        row_count = 0
        errors = 0
        while True:
            row = mysql_cur.fetchone()
            if row is None:
                break

            # Assemble a list of the output data that we'll subsequently
            # convert to a tuple.
            output_L = []
            for c in cols:
                data = row[c.name]
                newdata = convert_data(c, data)
                output_L.append(newdata)

            try:
                pg_execute(pg_conn, options, ins_sql, tuple(output_L))
            except KeyboardInterrupt:
                raise
            except:
                logging.error('Failure inserting row into table %s', table,
                              exc_info=True)
                errors += 1
            else:
                row_count += 1

        logging.info("Table %s: %i rows converted (%i errors)",
                     table, row_count, errors)
        
    # Close connections
    logging.info('Closing database connections')
    mysql_conn.close()
    pg_conn.close()


if __name__ == '__main__':
    main()
