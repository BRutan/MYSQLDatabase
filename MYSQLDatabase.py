#################################################
# MYSQLDatabase.py
#################################################
# Description:
# * Object wraps functionality for mysql.connector
# library, capable of performing all DML/DDL/DCL functionality. 

import csv
from datetime import datetime, date
from pandas import DataFrame
import re
import os
import string
import mysql.connector as msc
from mysql.connector import errorcode

__all__ = ['MYSQLDatabase']

class MYSQLDatabase(object):
    """
    * Object wraps functionality of mysql connector.
    """
    #######################################
    # Static Variables:
    #######################################
    # Map appropriate wrapping in insert query when dealing with these variable types
    __TypeWrapMap = { "varchar" : "\"", "text" : "\"", "char" : "\"", "blob" : "\"", "binary" : "\"", "varbinary" : "\"",
                     "date" : "\"", "time" : "\"", "datetime" : "\"", "timestamp" : "\""}
    __InvalidOrds = [43, 64]
    __selectStmtFilter = { 'inner' : 0, 'outer' : 0}
    __tableTokenStop = { 'group' : 0, 'having' : 0 }
    __funcSignature = re.compile('[a-z]+\(.+\)')
    __stripPunct = ''.join(list(set(string.punctuation + ' ')))
    __invalidCharsRE = re.compile('(\#|\(|\)|\*|\"|\')')
    
    #######################################
    # Constructors/Destructors:
    #######################################
    def __init__(self, userName, password, hostName, schema):
        """
        * Connect to schema at passed hostName, using provided credentials.
        Get all existing tables, with column attributes, in the schema.
        """
        errMsgs = []
        # Validate all input parameters:
        if not isinstance(userName, str):
            errMsgs.append("userName must be a string.")
        if not isinstance(password, str):
            errMsgs.append("password must be a string.")
        if not isinstance(hostName, str):
            errMsgs.append("hostName must be a string.")
        if not isinstance(schema, str):
            errMsgs.append("schema must be a string.")
        # Raise exception if issues occur:
        if len(errMsgs) > 0:
            raise BaseException('\n'.join(errMsgs))
        self.__user = userName
        self.__pw = password
        self.__host = hostName
        # Attempt to initially open connection:
        connect = self.__Reconnect(schema)
        # Map {table -> {columns -> typeInfo}}:
        self.__tables = {}
        # Get all existing tables in the schema:
        cursor = connect.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        for table in tables:
            table = table[0].lower()
            # Pull column information from table:
            cursor.execute("SHOW COLUMNS FROM " + table)
            columns = cursor.fetchall()
            columnEntries = {}
            for column in columns:
                columnEntries[column[0].lower()] = self.__ColToDictKey(column)
            self.__tables[table] = columnEntries
        
        # Create map for existing schemas:
        self.__schemas = { }
        # Get all existing schemas in the database:
        cursor.execute("SHOW DATABASES")
        dbs = cursor.fetchall()
        for db in dbs:
            self.__schemas[db[0].lower()] = True
        
        cursor.close()
        # Set the active schema that gets switched whenever a different schema is queried:
        self.__activeSchema = schema

    def __del__(self):
        """
        * Close the connection to the database.
        """
        try:
            connect.close()
        except:
            pass

    #######################################
    # Mutators:
    #######################################
    def CreateSchema(self, schema):
        """
        * Create schema if does not exist for current mysql instance.
        """
        if not isinstance(schema, str):
            raise BaseException("schema must be a string.")
        elif not schema:
            raise BaseException("Please provide schema name.")

        schema = schema.strip()
        
        connect = self.__Reconnect(schema)
        cursor = connect.cursor()
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS " + schema + ";")
            schema  = schema.lower()
            self.__schemas[schema] = 0
            cursor.close()
        except Exception:
            pass

    def CreateTable(self, tableName, columns, schema = None):
        """
        * Create table with name for given database, with columns and types specified as strings
        in the columnNameToType map. If table exists then will skip creation. 
        Inputs:
        * tableName: String to name table.
        * schema: String with name of database (if omitted uses ActiveSchema).
        * columns: Dictionary mapping { ColumnName -> (Type [Str], IsPKey [Bool or Tuple], FKeyRef [Str]) }.
        If IsPKey is true then column will be primary key of table. 
        If IsPKey is a string then will make a composite key using all comma separated columns in string.
        If FKeyRef is not blank then column will be foreign key referencing passed table (as RefTable(Column).
        """
        # Validate function inputs:
        if not schema:
            schema = self.ActiveSchema
        errMsgs = self.__CheckParams(tableName, schema, columns)
        if len(errMsgs) > 0:
            raise BaseException('\n'.join(errMsgs))

        # Ensure that schema exists already and table does not exist yet:
        tableName = tableName.lower()
        schema = schema.lower()
        # Ensure that schema exists in database, table does not exist yet:
        errMsgs = self.__CheckDBObjectsExist(tableName, schema, False)
        if len(errMsgs) > 0:
            raise BaseException('\n'.join(errMsgs))

        # Ensure that at least one column is specified:
        numColumns = len(columns.keys())
        if numColumns == 0:
            raise BaseException("At least one column must be specified.")
    
        # Reopen the connection:
        connect = self.__Reconnect(schema)
        cursor = connect.cursor()
        
        fKeyStrings = []
        variables = []
        pKeyStr = ""
        createTableStrings = ["USE ", schema, "; CREATE TABLE "]
        createTableStrings.append(tableName)
        createTableStrings.append("(")
        ##############
        # Append all column names, type declaration strings:
        ##############
        lowerCols = {}
        for columnName in columns.keys():
            variables.append(columnName + " " + columns[columnName][0])
            pKey = columns[columnName][1]
            # Check if column is the primary key, skip if one was already specified:
            if pKey and not pKeyStr: 
                if isinstance(pKey, str):
                    compCols = ','.join([col.strip() for col in pKey.split(',')])
                    pKeyStr = ''.join(["PRIMARY KEY(", compCols, ")"])
                elif isinstance(pKey, bool) and pKey == True:
                    pKeyStr = ''.join(["PRIMARY KEY(", columnName, ")"]) 
            elif pKey and pKeyStr:
                raise BaseException('Only one primary key can exist per table.')
            # Check if column should use foreign key relationship with another table:
            if columns[columnName][2]:
                fKeyStrings.append("FOREIGN KEY(" + columnName + ") REFERENCES " + columns[columnName][2])
            lowerCols[columnName.lower()] = columns[columnName]
        if pKeyStr:
            variables.append(pKeyStr)
        if len(fKeyStrings) > 0:
            variables.append(','.join(fKeyStrings))
        createTableStrings.append(','.join(variables))
        createTableStrings.append(");")

        ##############
        # Execute table creation command:
        ##############
        cursor.execute(''.join(createTableStrings))
        tableName = tableName.lower()
        self.__tables[tableName] = lowerCols
        cursor.close()
        
    def ExecuteQuery(self, query, schema = None, getResults = False, shouldCommit = False, useDataFrame = False, dataframeIndex = None):
        """
        * Execute query on passed table for given schema.
        Required Inputs:
        * query: Expecting string with query for schema.
        Optional Inputs:
        * schema: Expecting string naming existing schema in mysql instance.
        * getResults: Put True if executing a SELECT statement, and expecting results.
        * shouldCommit: Put True if executing INSERT/DELETE/UPDATE statement, and should
        commit the changes to the database.
        * useDataFrame: Put True if you want SELECT statement results to be stored in a dataframe.
        * dataframeIndex = If a SELECT statement as a dataframe, specify the column to use as an index (string).
        """
        if not schema:
            schema = self.ActiveSchema
        query = query.lower()
        connect = self.__Reconnect(schema)
        cursor = connect.cursor()
        cursor.execute(query)
        # Commit transaction if requested:
        if shouldCommit:
            connect.commit()
        if 'drop table' in query:
            table = query[len('drop table ') : len(query)].strip(' ;')
            del self.__tables[table]

        # Return results if expecting any.
        if getResults:
            # Return empty dictionary if a select statement wasn't entered:
            if "select" not in query:
                return None
            rawResults = cursor.fetchall()
            if not rawResults:
                return None
            # Remove INNER/OUTER reserved words:
            tokens = [token.strip(MYSQLDatabase.__stripPunct) if not MYSQLDatabase.__funcSignature.match(token) else token for token in str.split(query, ' ')]
            tokens = list(filter(lambda a : not a in MYSQLDatabase.__selectStmtFilter.keys(), tokens))
            fromIndex = tokens.index("from")
            output = {}
            # Output results as dictionary mapping column name to value.
            if '*' not in query:
                # Extract selected columns from select stmt:
                columnTokens = {}
                # If join was performed, table aliases must have been used. 
                # Rename <Alias>.<ColumnName> to <Table>.<ColumnName>:
                rawColTokens = tokens[tokens.index("select") + 1: fromIndex]
                for index in range(0, len(rawColTokens)):
                    if index + 2 < len(rawColTokens) and rawColTokens[index + 1] == 'as':
                        columnTokens[rawColTokens[index + 2]] = True
                    elif rawColTokens[index] != 'as' and rawColTokens[index] not in columnTokens:
                        columnTokens[rawColTokens[index]] = True
                columnTokens = list(columnTokens.keys())
                aliasToTable = {}
                index = fromIndex
                while 'as' in tokens[index : len(tokens)]:
                    # We assume table name appears to immediate left of AS, alias to right:
                    index += tokens[index : len(tokens)].index("as")
                    tableName = tokens[index - 1]
                    alias = tokens[index + 1]
                    aliasToTable[alias] = tableName
                    index += 1
                # Get table name from query:
                tableNames = []
                tableAreaStop = []
                for index, token in enumerate(tokens, 0):
                    if token in MYSQLDatabase.__tableTokenStop:
                        tableAreaStop.append(index)
                tableAreaStop.append(len(tokens) - 1)
                tableAreaStop = min(tableAreaStop)
                index = 0
                # Replace aliases in column names with full table name:
                columnNames = []
                for column in columnTokens:
                    index = column.find('.')
                    if index > -1:
                        alias = column[0:index]
                        validCol = column.replace(alias + '.', aliasToTable[alias] + '.')
                        columnNames.append(validCol)
                        output[validCol] = []
                    else:
                        output[column] = []
                        columnNames.append(column)
            else:
                # Selecting all columns: set output dictionary to map using all columns in table.
                tableIndex = fromIndex + 1
                for index, token in enumerate(tokens, 0):
                    if token in MYSQLDatabase.__tableTokenStop:
                        tableIndex = index
                        break
                table = tokens[tableIndex]
                columnNames = list(self.__tables[table].keys())
                for column in columnNames:
                    output[column] = []

            # Output data as dictionary mapping { ColName -> [Values] }:
            for result in rawResults:
                colNum = 0
                for column in columnNames:
                    output[column].append(result[colNum])
                    colNum += 1  
            if useDataFrame:
                # Use primary key as index if specified
                output = DataFrame.from_dict(output)
                if dataframeIndex and dataframeIndex.lower() in query:
                    # Skip using supplied dataframe index if column not used in SELECT statement:
                    output.set_index(dataframeIndex.lower())

            return output

    def InsertInChunks(self, tableName, columns, chunkSize, schema = None, skipExceptions = False, logFile = None):
        """
        * Insert rows into table using chunk size.
        Inputs:
        * tableName: Expecting a string that refers to target table to insert data.
        * columns: Expecting { ColumnName -> Values[] } map. Len(Values) must be
        uniform for all columns.
        * chunkSize: Number of rows to insert at a time. Must be numeric.
        Optional Inputs:
        * schema: Specify the schema for the table (string).
        * skipExceptions: Put True to continue inserting if some rows failed to insert (boolean).
        * logFile: Path to log file detailing exceptions (string, or None by default).
        """
        # Validate function parameters:
        if not schema:
            schema = self.ActiveSchema
        errMsgs = self.__CheckParams(tableName, schema, columns)
        tableName = tableName.lower()
        if not isinstance(chunkSize, int) and not isinstance(chunkSize, float):
            errMsgs.append('chunkSize must be numeric.')
        if not isinstance(skipExceptions, bool):
            errMsgs.append('skipExceptions must be boolean.')
        if len(errMsgs) > 0:
            raise BaseException('\n'.join(errMsgs))

        numRows = len(columns[list(columns.keys())[0]])
        chunkSize = min(int(chunkSize), numRows)
        row = 0
        currCols = {}
        if skipExceptions:
            while row < numRows:
                end = min(row + chunkSize, numRows)
                for col in columns.keys():
                    currCols[col] = []
                rowRng = range(row, end + 1 if end != numRows else numRows)
                for row in rowRng:
                    for col in columns.keys():
                        currCols[col].append(columns[col][row])
                # Continue to insert rows if failed, if requested:
                try:
                    self.InsertValues(tableName, currCols, schema)
                except:
                    pass
                row += chunkSize
        else:
            while row < numRows:
                end = min(row + chunkSize, numRows)
                for col in columns.keys():
                    currCols[col] = []
                for row in range(row, end + 1):
                    for col in columns.keys():
                        currCols[col].append(columns[col][row])
                self.InsertValues(tableName, currCols, schema)
                row += chunkSize
    def CreateTemporaryTables(self, tableName, columns):
        """
        * Create temporary table.
        """
        pass

    def InsertValues(self, tableName, columns, schema = None, skipChecks = False):
        """
        * Insert all values into the create database. 
        Inputs:
        * tableName: Expecting a string that refers to target table to insert data.
        * columns: Expecting { ColumnName -> Values[] } map. Len(Values) must be
        uniform for all columns.
        Optional Inputs:
        * schema: Specify the schema for the table (string).
        """
        # Validate function parameters:
        if not schema:
            schema = self.ActiveSchema
        errMsgs = []
        if not skipChecks:
            errMsgs = self.__CheckParams(tableName, schema, columns)
        tableName = tableName.lower()
        if len(errMsgs) > 0:
            raise BaseException('\n'.join(errMsgs))
        # Raise exception if schema and table do not exist:
        if not skipChecks:
            errMsgs = self.__CheckDBObjectsExist(tableName, schema)
        if len(errMsgs) > 0:
            raise BaseException('\n'.join(errMsgs))

        # Reopen the connection:
        connect = self.__Reconnect(schema)
        cursor = connect.cursor()

        insertVals = []
        # Get total number of rows (must be uniform for all columns):
        firstColName = list(columns.keys())[0]
        numRows = len(columns[firstColName])
        for col in columns.keys():
            if len(columns[col]) != numRows:
                raise BaseException('Number of rows must be uniform across all columns.')
        # Exit if no data was provided for columns:
        if numRows == 0:
            return None
        colString = "(" + ','.join(list(columns.keys())) + ")"
        tableInsertQuery = ["INSERT INTO " , tableName,  " ", colString, " VALUES "]
        # Determine appropriate wrapping for data given column type:
        wrapMap = {}
        for column in columns.keys():
            wrapMap[column] = self.__GetDataWrap(self.__tables[tableName][column.lower()][0])
            
        ###########################
        # Generate full insert query string using passed data:
        ###########################
        currRow = 0
        rows = []
        while currRow < numRows:
            currRowValue = []
            for column in columns.keys():
                wrap = wrapMap[column]
                # Convert values to list if not already:
                if not isinstance(columns[column], list):
                    columns[column] = list(columns[column])
                # Convert None to NULL:
                if columns[column][currRow] is None:
                    currRowValue.append('NULL')
                else:
                    currRowValue.append(wrap + str(columns[column][currRow]) + wrap)
            rows.append("(" + ",".join(currRowValue) + ")")
            currRow += 1
        tableInsertQuery.append(','.join(rows))
        cursor.execute(''.join(tableInsertQuery))
        connect.commit()
        cursor.close()

    def PrintSelectToCSV(self, query, csvPath, schema = None, unicode = False):
        """
        * Print select statement to specified CSV.
        """
        errMsgs = []
        if not schema:
            schema = self.ActiveSchema
        
        schema = schema.lower()

        if not isinstance(csvPath,str):
            errMsgs.append("csvPath must be a string.")
        elif '.' not in csvPath:
            csvPath += '.csv'
        elif '.csv' not in csvPath:
            csvPath = csvPath[0:csvPath.rfind('.')] + '.csv'
        if not isinstance(query, str):
            errMsgs.append("query must be a string.")
        elif "select" not in query.lower():
            errMsgs.append("query must be a select statement.")
        if schema and not isinstance(schema, str):
            errMsgs.append("schema must be a string if specified.")
        elif schema not in self.__schemas:
            errMsgs.append("schema does not exist in database.")
        if isinstance(csvPath, str) and os.path.exists(csvPath):
            errMsgs.append("csv at csvPath already exists.")
        if not isinstance(unicode, bool):
            errMsgs.append("unicode must be a boolean.")

        if len(errMsgs) > 0:
            msg = '\n'.join(errMsgs)
            raise BaseException(msg)

        # Pull data from select statement:
        query = query.lower()
        data = self.ExecuteQuery(query, schema, getResults=True)
        if data and len(data[list(data.keys())[0]]) > 0:
            with open(csvPath, 'w', newline='') as f:
                writer = csv.writer(f)
                # Write headers:
                columnHeaders = list(data.keys())
                writer.writerow(columnHeaders)
                rowLen = len(data[columnHeaders[0]])
                if not unicode:
                    for row in range(0, rowLen):
                        currRow = []
                        for column in data.keys():
                            if isinstance(data[column][row], (date, datetime)):
                                currRow.append(data[column][row].strftime('%Y-%m-%d'))
                            else:
                                currRow.append(str(data[column][row]))
                        writer.writerow(currRow)
                else:
                    for row in range(0, rowLen):
                        currRow = []
                        for column in data.keys():
                            if isinstance(data[column][row], (date, datetime)):
                                currRow.append(data[column][row].strftime('%Y-%m-%d'))
                            else:
                                currRow.append(str(data[column][row]).encode('utf8'))
                        writer.writerow(currRow)
            return True
        else:
            return False

    def TableExists(self, tableName):
        """
        * Return True if table exists in current schema.
        """
        return tableName.lower() in self.__tables.keys()


    #######################
    # Accessors:
    #######################
    @property
    def ActiveSchema(self):
        """
        * Return the schema most recently connected to.
        """
        return self.__activeSchema
    @property
    def Tables(self):
        """
        * Return copy of tables in database (as dictionary mapping of tables to column attributes).
        """
        return self.__tables.copy()
    @property
    def Schemas(self):
        """
        * Return copy of schemas in the database (as list containing all schema names).
        """
        return list(self.__schemas.keys()).copy()

    @staticmethod
    def RemoveInvalidChars(elems):
        """
        * Strip all invalid characters before insertion into
        database.
        Inputs:
        * elems: Can be string or list of strings.
        """
        if isinstance(elems, str):
            return MYSQLDatabase.__invalidCharsRE.sub('', elems)
        elif isinstance(elems, list):
            return [MYSQLDatabase.__invalidCharsRE.sub('', elem) for elem in elems]
        else:
            raise BaseException('elems must be string or list of strings.')

    #######################
    # Helper Functions:
    #######################
    def __CheckParams(self, tableName, schema, columns):
        """
        * Ensure that parameters to Create and Insert functions are valid.
        Returns list containing all error strings.
        """
        errMsgs = []
        if not isinstance(tableName, str):
            errMsgs.append("tableName must be a string.")
        if not isinstance(schema, str):
            errMsgs.append("schema must be a string.")
        if not isinstance(columns, dict):
            errMsgs.append("columns must be a dictionary with appropriate mapping.")
        return errMsgs

    def __CleanString(string):
        """
        * Clean string of all non-unicode characters.
        """
        pass

    def __CheckDBObjectsExist(self, tableName, schema, tableExists = True):
        """
        * Ensure that schema and table exists.
        Inputs:
        * tableName: Expecting string indicating table in schema.
        * schema: Expecting string indicating schema in current MySQL instance.
        * tableExists: Put True if want to check that table exists, otherwise put false.
        Output:
        * Returns list containing all error strings.
        """
        tableName = tableName.lower()
        schema = schema.lower()
        errMsgs = []
        if schema not in self.__schemas:
            errMsgs.append("Schema does not exist yet in database (please create with CreateSchema()).")
        # Ensure that schema exists, passed table does/does not exist (depending on tableExists):
        if tableExists and not tableName in self.__tables:
            errMsgs.append("Table does not exist in database.")
        elif not tableExists and tableName in self.__tables:
            errMsgs.append("Table already exists in schema.")

        return errMsgs

    def __GetDataWrap(self, columnType):
        """
        * Return wrap for data for INSERT queries.
        """
        # Remove quantity specification from columnType string:
        parenthIndex = columnType.find('(')
        parenthIndex = len(columnType) if parenthIndex == -1 else parenthIndex
        columnType = columnType[0 : parenthIndex]
        if columnType in MYSQLDatabase.__TypeWrapMap:
            return MYSQLDatabase.__TypeWrapMap[columnType]
        else:
            return ''

    def __Reconnect(self, schema):
        """
        * Return connection to mysql instance using stored parameters, at schema.
        """
        schema = schema.lower()
        return msc.connect(user=self.__user, password=self.__pw, host = self.__host, database = schema)

    def __ColToDictKey(self, columnList):
        """
        * Convert list containing column information to a dictionary key
        suitable for __tables object mapping.
        """
        colEntry = [columnList[1]]
        pKey = True if columnList[3] == 'PRI' else False
        fKey = True if columnList[3] == 'MUL' else False
        colEntry.append(pKey)
        if not pKey and columnList[2] == 'NO':
            colEntry[0] += ' NOT NULL'

        return colEntry

    def __CleanString(self, str):
        """
        * Clean all non-unicode characters and '#'.
        """
        return ''.join([ch if ord(ch) < 128 and ord(ch) != 35 else ' ' for ch in str])




class ResultSet(object):
    """
    * Result from SELECT statement.
    """
    def __init__(self, query, rawResults):
        self.__ExtractColumns(query)
        self.__Convert(rawResults)
    #################
    # Properties:
    #################
    @property
    def RowCount(self):
        return self.__rowCount
    @property
    def Columns(self):
        return self.__data

    #################
    # Interface Methods:
    #################
    def Print(self, filePath, columns = None):
        """
        * Print select statement to file
        """
        pass
    #################
    # Private Helpers:
    #################
    def __Convert(self, rawResults):
        pass

    def __ExtractColumns(self, query):
        pass