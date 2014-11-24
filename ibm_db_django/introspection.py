# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2013.                                      |
# +--------------------------------------------------------------------------+
# | This module complies with Django 1.0 and is                              |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi              |
# +--------------------------------------------------------------------------+

import sys

try:
    import ibm_db_dbi as Database
except ImportError as e:
    raise ImportError(
        "ibm_db module not found. Install ibm_db module from http://code.google.com/p/ibm-db/. Error: %s" % e)

from django.db.backends import BaseDatabaseIntrospection, FieldInfo
from django import VERSION as djangoVersion


class DatabaseIntrospection(BaseDatabaseIntrospection):
    """
    This is the class where database metadata information can be generated.
    """
    data_types_reverse = {
        Database.STRING: "CharField",
        Database.TEXT: "TextField",
        Database.XML: "XMLField",
        Database.NUMBER: "IntegerField",
        Database.FLOAT: "FloatField",
        Database.DECIMAL: "DecimalField",
        Database.DATE: "DateField",
        Database.TIME: "TimeField",
        Database.DATETIME: "DateTimeField",
    }
    if djangoVersion[0:2] > (1, 1):
        data_types_reverse[Database.BINARY] = "BinaryField"
        data_types_reverse[Database.BIGINT] = "BigIntegerField"
    else:
        data_types_reverse[Database.BIGINT] = "IntegerField"


    def __get_col_index(self, cursor, schema, table_name, col_name):
        """Private method. Getting Index position of column by its name"""
        cursor.execute("""SELECT colno
                        FROM syscolumns
                        WHERE colname='%s'
                        AND tabid=(SELECT tabid FROM systables
                            WHERE tabname='%s')""" %(col_name, table_name))
        return(int(cursor.fetchone()[0]) - 1)

    def get_field_type(self, data_type, description):
        if data_type == Database.NUMBER:
            if description.precision == 5:
                return 'SmallIntegerField'
        return super(DatabaseIntrospection, self).get_field_type(data_type, description)

    def get_table_list(self, cursor):
        """Returns a list of table names in the current database."""
        table_list = []
        for table in cursor.connection.tables():
            table_list.append(table['table_name'].lower())
        return table_list

    def get_table_description(self, cursor, table_name):
        """Returns a description of the table, with the DB-API cursor.description interface."""
        qn = self.connection.ops.quote_name
        cursor.execute("SELECT FIRST 1 * FROM %s" % qn(table_name))
        description = []
        if djangoVersion < (1, 6):
            for desc in cursor.description:
                description.append([desc[0].lower(), ] + desc[1:])
        else:
            for desc in cursor.description:
                description.append(FieldInfo(*[desc[0].lower(), ] + desc[1:]))
        return description

    def get_indexes(self, cursor, table_name):
        """ This query retrieves each index on the given table, including the
            first associated field name """
        cursor.execute("""select c1.colname, i1.idxtype,
                        (select constrtype from sysconstraints where idxname=i1.idxname) as pkey
                        FROM sysindexes i1, syscolumns c1
                        WHERE i1.tabid=c1.tabid AND i1.part1=c1.colno
                        AND i1.part2 = 0 and i1.tabid = (select tabid from systables where tabname='%s')""" % table_name)
        indexes = {}
        for row in cursor.fetchall():
            indexes[row[0]] = {
                'primary_key': True if row[2] == 'P' else False,
                'unique': True if row[1] == 'U' else False
            }
        return indexes

    def get_key_columns(self, cursor, table_name):
        relations = []
        cursor.execute("""
        SELECT col1.colname as column_name, t2.tabname, col2.colname as referenced_column
        FROM syscolumns col1, sysindexes idx1, sysconstraints const1, systables t1, syscolumns col2,
         sysindexes idx2, sysconstraints const2, sysreferences ref, systables t2
        WHERE col1.tabid=idx1.tabid
        AND col1.colno=idx1.part1
        AND idx1.idxname=const1.idxname
        AND const1.tabid=t1.tabid
        AND const1.constrtype='R'
        AND col2.tabid=idx2.tabid
        AND col2.colno=idx2.part1
        AND idx2.idxname=const2.idxname
        AND const2.constrid=ref.primary
        AND ref.constrid = const1.constrid
        AND t2.tabid=idx2.tabid
        AND t1.tabname = '%s'
        """ % table_name)
        relations.extend(cursor.fetchall())
        return relations

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        relations = {}
        kc_relations = self.get_key_columns(cursor, table_name)
        for rel in kc_relations:
            row0 = self.__get_col_index(cursor, None, table_name, rel[0])
            row1 = self.__get_col_index(cursor, None, rel[1], rel[2])
            row2 = rel[1]
            relations[row0] = (row1, row2)
        return relations

    def get_constraints(self, cursor, table_name):
        constraints = {}
        schema = cursor.connection.get_current_schema()
        sql = "SELECT CONSTNAME, COLNAME FROM SYSCAT.COLCHECKS WHERE TABSCHEMA='%(schema)s' AND TABNAME='%(table)s'" % {'schema': schema.upper(), 'table': table_name.upper()}
        cursor.execute(sql)
        for constname, colname in cursor.fetchall():
            if constname not in constraints:
                constraints[constname] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': True,
                    'index': False
                }
            constraints[constname]['columns'].append(colname.lower())

        sql = "SELECT KEYCOL.CONSTNAME, KEYCOL.COLNAME FROM SYSCAT.KEYCOLUSE KEYCOL INNER JOIN SYSCAT.TABCONST TABCONST ON KEYCOL.CONSTNAME=TABCONST.CONSTNAME WHERE TABCONST.TABSCHEMA='%(schema)s' and TABCONST.TABNAME='%(table)s' and TABCONST.TYPE='U'" % {'schema': schema.upper(), 'table': table_name.upper()}
        cursor.execute(sql)
        for constname, colname in cursor.fetchall():
            if constname not in constraints:
                constraints[constname] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': True,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[constname]['columns'].append(colname.lower())

        for pkey in cursor.connection.primary_keys(None, schema, table_name):
            if pkey['PK_NAME'] not in constraints:
                constraints[pkey['PK_NAME']] = {
                    'columns': [],
                    'primary_key': True,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[pkey['PK_NAME']]['columns'].append(pkey['COLUMN_NAME'].lower())

        for fk in cursor.connection.foreign_keys( True, schema, table_name ):
            if fk['FK_NAME'] not in constraints:
                constraints[fk['FK_NAME']] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': (fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()),
                    'check': False,
                    'index': False
                }
            constraints[fk['FK_NAME']]['columns'].append(fk['FKCOLUMN_NAME'].lower())
            if fk['PKCOLUMN_NAME'].lower() not in constraints[fk['FK_NAME']]['foreign_key']:
                fkeylist = list(constraints[fk['FK_NAME']]['foreign_key'])
                fkeylist.append(fk['PKCOLUMN_NAME'].lower())
                constraints[fk['FK_NAME']]['foreign_key'] = tuple(fkeylist)

        for index in cursor.connection.indexes( True, schema, table_name ):
            if index['INDEX_NAME'] not in constraints:
                constraints[index['INDEX_NAME']] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            elif constraints[index['INDEX_NAME']]['unique'] :
                continue
            elif constraints[index['INDEX_NAME']]['primary_key']:
                continue
            constraints[index['INDEX_NAME']]['columns'].append(index['COLUMN_NAME'].lower())
        return constraints