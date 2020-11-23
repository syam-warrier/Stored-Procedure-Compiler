#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
from collections import OrderedDict, MutableSet
from random import randint
import traceback
import pandas as pd
import numpy as np


# In[2]:


class OrderedSet(MutableSet):

    def __init__(self, iterable=None):
        self.end = end = [] 
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:        
            key, prev, next = self.map.pop(key)
            prev[2] = next
            next[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)


# In[3]:


aggregate_functions = ["SUM", "MAX", "MIN"]

keywords      =    ['SET', 'AS', 'IF', 'THEN', 'ELSE', 'ON', 'AND', 'LOOKUP', 'LOOKED-UP', 'FILTER-BY', 'WHERE', 'SUM', 'MIN', 'MAX', 'OF']
regex_dict    =    {
                        'STRING'       :    r'(\s\"\w+\"\s)',
                        'NUMBER'       :    r'(\s\d+(?:\.\d+)?\s)',
                        'COLUMN1'      :    r'(\s\w+\s)(\sFROM\s\s\w+\.\w+\s)',
                        'TABLE'        :    r'\sFROM\s(\s\w+\.\w+\s)',
                        'COLUMN2'      :    r'(\s\w+\s)',
                                    
                        'CONDITION'    :    r'\s(__\d+__)\s\s(=|<|>|<=|>=|!=)\s\s(__\d+__)\s',
                        'BOOLEAN'      :    r'\s(__\d+__)\s\s(AND|OR)\s\s(__\d+__)\s',
                        'AGGREGATE'    :    r'\s(\(\s*)?(' + '|'.join(aggregate_functions) + ')\s\sOF\s\s(__\d+__)(\s*\))?\s',
                        'FILTER'       :    r'\s(FILTER-BY|WHERE)\s\s(__\d+__)\s',
                        'IF'           :    r'\s(\(\s*)?(__\d+__)\s\sIF\s\s(__\d+__)\s\sELSE\s\s(__\d+__)(\s*\))?\s',
                        'LOOKUP'       :    r'\s(\(\s*)?(\sLOOKUP\s)?\s(__\d+__)\s(\sLOOKED-UP\s\s(__\d+__)\s)?\sON\s\s(__\d+__)(\s*\))?\s',
                        'SETXASY'      :    r'\sSET\s\s(__\d+__)\s\sAS\s\s(__\d+__)\s',
                        'SETAS'        :    r'\sSET\s\sAS\s\s(__\d+__)\s+[$|\n]'
                    }

def unique(lst: list):
    res_list = []
 
    for item in lst: 
        if item not in res_list:
            res_list.append(item)
        
    return res_list


# In[4]:


class TokenSet:
    @classmethod
    def __init__(cls):
        cls.tokens       =    OrderedDict()
    
    @classmethod
    def flush(cls):
        cls.tokens       =    OrderedDict()
    
    @classmethod
    def registerToken(cls, token):
        new_token         =    '__' + str(randint(100001, 1000000)) + '__'
        cls.tokens[new_token]    =    token
        return new_token
    
    @classmethod
    def updateTokensValues(cls, existingToken, newToken):
        for k in cls.tokens.keys():
            if existingToken in cls.tokens[k].getChildren():
                tokenDict = cls.tokens[k].__dict__
                for in_k, in_v in tokenDict.items():
                    if in_v == existingToken:
                        cls.tokens[k].__dict__[in_k] = newToken
    
    @classmethod
    def checkIfTokenExists(cls, tokenValue):
        for k, v in cls.tokens.items():
            if tokenValue == v.getChildren():
                return True
        return False
    
    @classmethod
    def getToken(cls, tokenValues):
        if isinstance(tokenValues, str):
            tokenValues = list(tokenValues)
        for k, v in cls.tokens.items():
            count = 0
            for val in tokenValues:
                if val in v.getChildren():
                    count = count + 1
            if count == len(tokenValues):
                return k
        return ''
    
    @classmethod
    def getAllTokens(cls):
        return [ (k, str(v.getChildren()), type(v)) for k, v in cls.tokens.items() ]


# In[5]:


class String:
    def __init__(self, valueId):
        self.valueId = valueId.strip()
        
    def getChildren(self):
        return [self.valueId]
    
    def getSelectPart(self):
        return self.valueId
    
    def getTablePart(self):
        return None
    
    def getFilterPart(self):
        return self.getSelectPart()
    
class Number:
    def __init__(self, valueId):
        self.valueId = valueId.strip()
    
    def getChildren(self):
        return [self.valueId]
    
    def getSelectPart(self):
        return self.valueId
    
    def getTablePart(self):
        return None
    
    def getFilterPart(self):
        return self.getSelectPart()

class Table:
    def __init__(self, tblDbId):
        tblDbParts = tblDbId.split('.')
        self.tblDbNameId = tblDbParts[0].strip() if len(tblDbParts) == 2 else ''
        self.tblNameId   = tblDbParts[1].strip() if len(tblDbParts) == 2 else tblDbParts[0].strip()
        
    def getChildren(self):
        return [self.tblDbNameId, self.tblNameId]
    
    def getSelectPart(self):
        return self.tblNameId
    
    def getTablePart(self):
        return (self.tblDbNameId + '.' if self.tblDbNameId != '' else '') + self.tblNameId
    
    def getFilterPart(self):
        return self.getSelectPart()
    
class Column:
    def __init__(self, colName, dbTblName):
        self.colName         =    colName.strip()
        dbTblName            =    list(map(lambda x : x.strip(), re.findall(regex_dict['TABLE'], dbTblName, flags=re.IGNORECASE)))
        dbTblName            =    dbTblName[0] if len(dbTblName) > 0 else ''
        
        if not TokenSet.checkIfTokenExists(dbTblName.split('.')) and dbTblName != '':
            self.tblToken    =    TokenSet.registerToken(Table(dbTblName))
        else:
            self.tblToken    =    ''
        
    def getChildren(self):
        #colToken = TokenSet.getToken(self.colName)
        #return [self.colName, colToken]
        return [self.colName, self.tblToken]
    
    def getSelectPart(self):
        return (TokenSet.tokens[self.tblToken].getSelectPart() + '.' + self.colName) if self.tblToken != '' else self.colName
    
    def getTablePart(self):
        dbTblName = TokenSet.tokens[self.tblToken].tblDbNameId + '.' + TokenSet.tokens[self.tblToken].tblNameId if self.tblToken != '' else ''
        return dbTblName if dbTblName != '' else None
    
    def getFilterPart(self):
        return self.getSelectPart()

class Condition:
    def __init__(self, leftId, op, rightId):
        self.leftId         =    leftId.strip()
        self.op             =    op.strip()
        self.rightId        =    rightId.strip()
        
    def getChildren(self):
        return [self.leftId, self.op, self.rightId]
        
    def getSelectPart(self):
        return self.leftId + ' ' + self.op + ' ' + self.rightId
    
    def getTablePart(self):
        return self.leftId + ' ' + self.rightId
    
    def getFilterPart(self):
        return self.getSelectPart()
    
class Boolean:
    def __init__(self, leftId, op, rightId):
        self.leftId          =    leftId.strip()
        self.op              =    op.strip()
        self.rightId         =    rightId.strip()
        
    def getChildren(self):
        return [self.leftId, self.op, self.rightId]
        
    def getSelectPart(self):
        return self.leftId + ' ' + self.op + ' ' + self.rightId
    
    def getTablePart(self):
        return self.leftId + ' ' + self.rightId
    
    def getFilterPart(self):
        return self.getSelectPart()
    
class Aggregate:
    def __init__(self, aggOp, columnId):
        self.aggOp           =    aggOp.strip()
        self.columnId        =    columnId.strip()
        
    def getChildren(self):
        return [self.aggOp, self.columnId]
        
    def getSelectPart(self):
        return self.aggOp + '(' + self.columnId + ')'
    
    def getTablePart(self):
        return self.columnId
    
    def getFilterPart(self):
        return self.getSelectPart()    
    
class Filter:
    def __init__(self, conditionId):
        self.conditionId    =    conditionId.strip()
        
    def getChildren(self):
        return [self.conditionId]
        
    def getSelectPart(self):
        return self.conditionId
    
    def getTablePart(self):
        return ''
    
    def getFilterPart(self):
        return self.getSelectPart()
    
class IfThenElse:
    def __init__(self, conditionPartId, thenPartId, elsePartId):
        self.conditionPartId    =    conditionPartId.strip()
        self.thenPartId         =    thenPartId.strip()
        self.elsePartId         =    elsePartId.strip()
        
    def getChildren(self):
        return [self.conditionPartId, self.thenPartId, self.elsePartId]
    
    def getSelectPart(self):
        return 'CASE WHEN ' + self.conditionPartId + ' THEN ' + self.thenPartId + ' ELSE ' + self.elsePartId + ' END'
    
    def getTablePart(self):
        return ''
    
    def getFilterPart(self):
        return self.getSelectPart()
    
class Lookup:
    def __init__(self, columnId, conditionId, tableId=''):
        if isinstance(TokenSet.tokens[columnId], Column):
            self.columnId        =    columnId.strip()
        elif isinstance(TokenSet.tokens[columnId], Condition):
            self.columnId        =    TokenSet.tokens[columnId].rightId
        self.conditionId     =    conditionId.strip()
        self.tableId         =    '' if tableId.strip() == '' else tableId.strip()
        
        if self.tableId.strip() != '':
            TokenSet.tokens[self.columnId].tblToken = self.tableId
        # To set the table on left part of condition as "tableId"
        TokenSet.tokens[TokenSet.tokens[self.conditionId].leftId].tblToken = TokenSet.tokens[self.columnId].tblToken
        
    def getChildren(self):
        return [self.columnId, self.conditionId]
    
    def getSelectPart(self):
        return self.columnId
    
    def getTablePart(self):
        #tblName = recurseTablePart(' ' + self.columnId + ' ') if self.tableId == '' else self.tableId
        tblName = recurseTablePart(self.columnId) if self.tableId == '' else self.tableId
        #return 'LEFT OUTER JOIN\n  ' + tblName + '\n    ON ' + recurseSelectPart(' ' + self.conditionId + ' ')
        return 'LEFT OUTER JOIN ' + tblName + '\n    ON ' + recurseSelectPart(self.conditionId)
    
    def getFilterPart(self):
        return self.getSelectPart()
    
class SetXasY:
    def __init__(self, variableId, valueId):
        self.variableId      =    variableId.strip()
        self.valueId         =    valueId.strip()
        
    def getChildren(self):
        return [self.variableId, self.valueId]
    
    def getSelectPart(self):
        return self.valueId
    
    def getTablePart(self):
        return ''
    
    def getFilterPart(self):
        return self.getSelectPart()
    
class SetAs:
    def __init__(self, valueId):
        self.valueId         =    valueId.strip()
        
    def getChildren(self):
        return [self.valueId]
    
    def getSelectPart(self):
        return self.valueId
    
    def getTablePart(self):
        return ''
    
    def getFilterPart(self):
        return self.getSelectPart()
    
def recurseSelectPart(statement):
        while True:
            prev_statement = statement
            for match in re.finditer('__\d+__', statement):
                statement = re.sub(match.group(), TokenSet.tokens[match.group()].getSelectPart(), statement)
            if prev_statement == statement:
                return statement;
    
def recurseTablePart(statement):
        while True:
            prev_statement = statement
            for match in re.finditer('__\d+__', statement):
                statement = re.sub(match.group(), TokenSet.tokens[match.group()].getTablePart(), statement)
            if prev_statement == statement:
                return statement;
            
def recurseWherePart(statement):
        while True:
            prev_statement = statement
            for match in re.finditer('__\d+__', statement):
                statement = re.sub(match.group(), TokenSet.tokens[match.group()].getFilterPart(), statement)
            if prev_statement == statement:
                return statement;


# In[6]:


class Parser:
    
    def __init__(self, keywords:list, regex_dict:dict):
        self.keywords      =    keywords
        self.regex_dict    =    regex_dict
    
    def parse(self, string, defaultDbTbl='', mode='Normal'):
        
        TokenSet.flush()
        
        std_string = ''
        for line in string.split('\n'):
            std_string = std_string + ' ' + line + ' \n'
        string = std_string
        if mode == 'Debug':
            print('Cleaned string : ' + string)
        
        loopRunCount = 1
        while True:
            prev_string = string
            for patternName, regexString in self.regex_dict.items():
                if mode == 'Debug':
                    print('\n' + patternName + ' : ' + regexString)
                    print('Before:' + string)
                    
                if loopRunCount == 2:
                    std_string = ''
                    for line in string.split('\n'):
                        std_string = std_string + '\n ' + re.sub(r' +','  ',line.upper()) + ' '
                    string = std_string
                    
                    for keyword in keywords:
                        string = string.replace(keyword.replace('-', '  '), keyword)                            
                    
                for match in re.finditer(regexString, string, flags=re.IGNORECASE):
                    try:
                        matchedString      = match.group()
                        if mode == 'Debug':
                            print('1. matched string : ' + matchedString)
                            
                        matchedStringParts = match.groups()
                        if mode == 'Debug':
                            print('2. matched string : ' + str(matchedStringParts))
                            
                        if matchedString.strip() not in self.keywords:
                            if mode == 'Debug':
                                print('3. Start loop : ' + matchedString)
                    
                            if patternName == 'STRING':
                                if mode == 'Debug':
                                    print('4. Detected as STRING')
                                token = TokenSet.registerToken(String(matchedStringParts[0]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + ' >.')
                                string = re.sub(matchedString, ' ' + token + ' ', string)
                                
                            if patternName == 'NUMBER':
                                if mode == 'Debug':
                                    print('4. Detected as NUMBER')
                                token = TokenSet.registerToken(Number(matchedStringParts[0]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <' + ' ' + token + ' >.')
                                string = re.sub(matchedString, ' ' + token + ' ', string)
                                
                            if patternName == 'TABLE':
                                if mode == 'Debug':
                                    print('4. Detected as TABLE')
                                token = TokenSet.registerToken(Table(matchedStringParts[0]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <' + ' ' + token + ' >.')
                                string = re.sub(matchedString, ' ' + token + ' ', string)
                                
                            elif (patternName.startswith('COLUMN') and not matchedString.strip().startswith('__')):
                                if mode == 'Debug':
                                    print('4. Detected as COLUMN')
                                column = matchedStringParts[0]
                                table = matchedStringParts[1] if len(matchedStringParts) == 2 else ' FROM  ' + defaultDbTbl + ' ' if defaultDbTbl != '' else ''
                                
                                token = TokenSet.registerToken(Column(column, table))
                                
                                if mode == 'Debug':
                                    print('5a. Replacing <' + matchedString + '> with <' + ' ' + token + ' >.')
                                string = re.sub(matchedString, ' ' + token + ' ', string)
                                if mode == 'Debug':
                                    print('5b. Replacing <' + matchedStringParts[0] + '> with <' + ' ' + token + ' >.')
                                string = re.sub(matchedStringParts[0], ' ' + token + ' ', string)
                                
                            elif patternName == 'CONDITION':
                                if mode == 'Debug':
                                    print('4. Detected as CONDITION')
                                token = TokenSet.registerToken(Condition(matchedStringParts[0], matchedStringParts[1], matchedStringParts[2]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + ' ' + '>.')
                                string = re.sub(matchedString, ' ' + token + ' ', string)
                                
                            elif patternName == 'BOOLEAN':
                                if mode == 'Debug':
                                    print('4. Detected as BOOLEAN')
                                token = TokenSet.registerToken(Boolean(matchedStringParts[0], matchedStringParts[1], matchedStringParts[2]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + ' ' + '>.')
                                string = re.sub(matchedString, ' ' + token + ' ', string)
                                
                            elif patternName == 'AGGREGATE':
                                if mode == 'Debug':
                                    print('4. Detected as AGGREGATE')
                                token = TokenSet.registerToken(Aggregate(matchedStringParts[1], matchedStringParts[2]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + '> in <' + string + '>.')
                                string = re.sub(matchedString, ' ' + token + ' ', string)
                                
                            elif patternName == 'FILTER':
                                if mode == 'Debug':
                                    print('4. Detected as FILTER')
                                token = TokenSet.registerToken(Filter(matchedStringParts[1]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + ' ' + '>.')
                                string = re.sub(matchedString, ' \n ' + token + ' ', string)
                                
                            elif patternName == 'IF':
                                if mode == 'Debug':
                                    print('4. Detected as IF')
                                token = TokenSet.registerToken(IfThenElse(matchedStringParts[2], matchedStringParts[1], matchedStringParts[3]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <' + token + '>.')
                                string = string.replace(matchedString, ' '  + token + ' ')
                                
                            elif patternName == 'LOOKUP':
                                if mode == 'Debug':
                                    print('4. Detected as LOOKUP')
                                if matchedStringParts[1] is not None:
                                    token = TokenSet.registerToken(Lookup(matchedStringParts[2], matchedStringParts[-2]))
                                elif matchedStringParts[3] is not None:
                                    token = TokenSet.registerToken(Lookup(matchedStringParts[2], matchedStringParts[-2], matchedStringParts[-3]))
                                    
                                TokenSet.updateTokensValues(matchedStringParts[2], token)
                                TokenSet.tokens[token].columnId = matchedStringParts[2].strip()
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ matchedStringParts[2] + '>.')
                                string = string.replace(matchedString.strip(), matchedStringParts[2])
                                
                            elif patternName == 'SETXASY':
                                if mode == 'Debug':
                                    print('4. Detected as SETXASY')
                                token = TokenSet.registerToken(SetXasY(matchedStringParts[0], matchedStringParts[1]))
                                TokenSet.updateTokensValues(matchedStringParts[0], token)
                                TokenSet.tokens[token].variableId = matchedStringParts[0].strip()
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <' + matchedStringParts[0] + '>.')
                                string = string.replace(matchedString.strip(), token)
                                if mode == 'Debug':
                                    print('6. Replacing <' + matchedStringParts[0] + '> with <' + ' ' + token + ' >.')
                                string = string.replace(matchedStringParts[0], token)
                                
                            elif patternName == 'SETAS':
                                if mode == 'Debug':
                                    print('4. Detected as SETAS')
                                token = TokenSet.registerToken(SetAs(matchedStringParts[0]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <' + token + '>.')
                                string = string.replace(matchedString.strip(), token)
                                
                    except Exception as e:
                        if mode == 'Debug':
                            print('Error : ' + str(e))
                        TokenSet.tokens.popitem()
                    
                loopRunCount = loopRunCount + 1
                
                if mode == 'Debug':
                    print('After:' + string)
            if prev_string == string:
                break
            
    def genSelectPart(self, mode='Normal'):
        statement = [token for token in TokenSet.tokens.keys() if isinstance(TokenSet.tokens[token], SetAs)]
        if (len(statement) > 0) and (statement[0].strip() in TokenSet.tokens.keys()):
            statement = statement[0]
            while True:
                if mode == 'Debug':
                    print('Before:' + statement)
                prev_statement = statement
                for match in re.finditer('__\d+__', statement):
                    statement = re.sub(match.group(), TokenSet.tokens[match.group()].getSelectPart(), statement)
                if mode == 'Debug':
                    print('After:' + statement)
                if prev_statement == statement:
                    std_statement = '\n'.join([re.sub('(?<!^)\s+', ' ', line).rstrip() for line in statement.split('\n')])
                    return std_statement
        else:
            return ''
    
    def genTablePart(self):
        tbls, lkp_tbls  = [], []
        for k in TokenSet.tokens.keys():
            if isinstance(TokenSet.tokens[k], Table):
                tbls.append(TokenSet.tokens[k].getTablePart())
            if isinstance(TokenSet.tokens[k], Lookup):
                lkp_tbls.append(recurseTablePart(TokenSet.tokens[k].getTablePart()))
        tbls = unique(tbls)
        lkp_tbls = unique(lkp_tbls)
        
        tbls_temp = []
        for t in tbls:
            match = False
            for l in lkp_tbls:
                if t in l:
                    match = True
            if match == False:
                tbls_temp.append(t)
        tbls = tbls_temp
        
        tbls.extend(lkp_tbls)
        #std_tblPart = [re.sub('(?<!^)\s+', ' ', line).rstrip() for line in tbls]
        return tbls
    
    def genWherePart(self):
        fltrs  = []
        for k in TokenSet.tokens.keys():
            if isinstance(TokenSet.tokens[k], Filter):
                fltrs.append(recurseWherePart(TokenSet.tokens[k].getFilterPart()))
        
        std_fltrPart = [re.sub('(?<!^)\s+', ' ', line).rstrip() for line in fltrs]
        return std_fltrPart

    #def genQuery(self, colName=''):
    #    selPart = (p.genSelectPart() + 'AS ' + colName) if colName != '' else p.genSelectPart()
    #    selPart = 'SELECT\n' + '\n'.join(selPart)
    #    
    #    fromPart = p.genTablePart()
    #    fromPart = 'FROM\n ' + '\n'.join(fromPart) if fromPart != [] else ''
    #    
    #    wherePart = p.genWherePart()
    #    wherePart = 'WHERE\n ' + '\n'.join(wherePart) if wherePart != [] else ''
    #    
    #    return selPart + '\n' + fromPart + '\n' + wherePart


# In[7]:


class QueryColumn:
    def __init__(self, colLogic, colName):
        self.colLogic = colLogic
        self.colName  = colName
    
    def __str__(self):
        return self.colLogic + (' AS ' + self.colName if self.colName != '' else '')

class Query:
    @classmethod
    def __init__(cls):
        cls.selectPart       =    []
        cls.fromPart         =    OrderedSet()
        cls.wherePart        =    []
        cls.groupByNeeded    =    False
        cls.groupByPart      =    []
        
    @classmethod
    def flush(cls):
        cls.selectPart       =    []
        cls.fromPart         =    OrderedSet()
        cls.wherePart        =    []
        cls.groupByNeeded    =    False
        cls.groupByPart      =    []
    
    @classmethod
    def parseColumn(cls, colSpec, colName, dfltDbTblName, mode='Normal'):
        p = Parser(keywords, regex_dict)
        p.parse(colSpec, dfltDbTblName, mode)
        
        cls.selectPart.append(QueryColumn(p.genSelectPart(mode), colName))
        for tbl in p.genTablePart():
            cls.fromPart.add(tbl)
        cls.wherePart.extend(p.genWherePart())      
    
    @classmethod
    def genSelectPart(cls):
        return 'SELECT\n  ' + '\n  '.join([ str(c) for c in cls.selectPart ])
    
    @classmethod
    def genFromPart(cls):
        return ('FROM\n  ' + '\n  '.join(cls.fromPart)) if cls.fromPart != [] else None
    
    @classmethod
    def genWherePart(cls):
        return ('WHERE\n  ' + '\n  AND '.join(cls.wherePart)) if cls.wherePart != [] else None
    
    @classmethod
    def genGroupByPart(cls):
        for stmt in cls.selectPart:
            groupByColNeeded = True
            for func in aggregate_functions:
                if func + '(' in stmt.colLogic:
                    cls.groupByNeeded = True
                    groupByColNeeded   = False
            if groupByColNeeded:
                cls.groupByPart.append(stmt.colName)
        return ('GROUP BY\n  ' + ', '.join(cls.groupByPart)) if cls.groupByNeeded and len(cls.groupByPart) != 0 else None
    
    @classmethod
    def genQuery(cls):
        return '\n'.join(filter(None, [cls.genSelectPart(), cls.genFromPart(), cls.genWherePart(), cls.genGroupByPart()])) + ';'
    
    @classmethod
    def parseAndGenQuery(cls, colSpec, colName, dfltDbTblName='', mode='Normal'):
        cls.parseColumn(colSpec, colName, dfltDbTblName, mode)
        return cls.genQuery()


# In[8]:


# ---------------------------------------------------- Spec Testing ---------------------------------------------------


# In[9]:


spec = pd.read_excel(r'C:\Users\Midhuna\Documents\Projects\Stored Procedure Builder\Data Specification Document.xlsx').fillna('')
spec


# In[10]:


spec['Source DB Table'] = spec['Source Database'] + '.' + spec['Source Table']
spec['Target DB Table'] = spec['Target Database'] + '.' + spec['Target Table']
spec


# In[11]:


spec['SP NAME'] = 'fn_' + spec['Target Database'].replace('DWH_','').str.lower() + '_' + spec['Target Table'].str.upper() + '_' + spec['Source Table'].str.lower()
spec


# In[12]:


Query.flush()
for index, row in spec.iterrows():
    Query.parseColumn(row['Logic'], row['Target Column'], row['Source DB Table'])
print(Query.genQuery())


# In[13]:


stored_proc = spec.groupby(['SP NAME'], sort=False)[['SP NAME']].agg(lambda x : '\n          ,'.join(OrderedSet(x.fillna(''))))
stored_proc['Target DB Table'] = spec.groupby(['SP NAME'], sort=False)['Target DB Table'].agg(lambda x : '\n      '.join(OrderedSet(x.fillna(''))))
stored_proc['Target Column'] = spec.groupby(['SP NAME'], sort=False)['Target Column'].agg(lambda x : '\n        ,'.join(OrderedSet(x.fillna(''))))

stored_proc['Query'] = Query.genQuery().replace('\n','\n      ')
stored_proc['Stored Procedure'] = 'CREATE OR REPLACE PROCEDURE ' + stored_proc['SP NAME'] + '\n  RETURNS VOID\n  LANGUAGE JAVASCRIPT\n  AS\n  $$\n    //Part 1 : Business Logic\n    var bus_query    = \'\n      INSERT INTO ' + stored_proc['Target DB Table'] + '\n      (\n        ' + stored_proc['Target Column'] + '\n      )\n      ' + stored_proc['Query'] + '\';\n    var statement   =    snowflake.createStatement( {sqlText: bus_query} );\n    statement.execute();\n  $$\n  ;'
stored_proc


# In[14]:


stored_proc.to_csv(r'C:\Users\Midhuna\Desktop\SP_test.txt', columns=['Stored Procedure'], header=None, index=None, sep=';', quotechar=' ')


# In[ ]:


# ------------------------------------------------------ Demo ---------------------------------------------------------


# In[15]:


# Verbatim - String
q = Query()
print(q.parseAndGenQuery('set as "ABC"', 'col_1'))


# In[19]:


# Verbatim - Number
q = Query()
print(q.parseAndGenQuery('set as 23.876', 'col_1'))


# In[16]:


# Verbatim - Column
q = Query()
print(q.parseAndGenQuery('set as ACCT_ID FROM CORE.TRANSACTION', 'col_1'))


# In[20]:


# Filter
q = Query()
print(q.parseAndGenQuery('''
set as ACCT_ID FROM CORE.TRANSACTION
FILTER by TRANSACTION_AMT > 1000000''', 'col_1'))


# In[22]:


# Filter - Chained
q = Query()
print(q.parseAndGenQuery('''set as ACCT_ID where TRANSACTION_AMT > 1000000 AND TRAN_AMT < 0''', 'col_1', 'CORE.TRANSACTION'))


# In[23]:


# Lookup
q = Query()
print(q.parseAndGenQuery('''Lookup PARTY_ROLE_CD from LVWS.PARTY_ROLE ON PARTY_ROLE_ID = PARTY_ROLE_ID
set as PARTY_ROLE_CD''', 'col_1', 'CORE.TRANSACTION'))


# In[24]:


# Lookup - Chained
q = Query()
print(q.parseAndGenQuery('''set as PARTY_ROLE_CD looked up from LVWS.PARTY_ROLE ON PARTY_ROLE_ID = PARTY_ROLE_ID''', 'col_1', 'CORE.TRANSACTION'))


# In[26]:


# Conditional
q = Query()
print(q.parseAndGenQuery('''set X as TRANS_AMT if PARTY_ROLE_CD = "DEBTOR" ELSE 0.00
Set as X''', 'col_1', 'CORE.TRANSACTION'))


# In[25]:


# Conditional - Chained
q = Query()
print(q.parseAndGenQuery('''set as TRANS_AMT if PARTY_ROLE_CD = "DEBTOR" ELSE 0.00''', 'col_1', 'CORE.TRANSACTION'))


# In[28]:


# Aggregation
q = Query()
print(q.parseAndGenQuery('''set X as SUM of TRANS_AMT
Set as X''', 'col_1', 'CORE.TRANSACTION'))


# In[27]:


# Aggregation - Chained
q = Query()
print(q.parseAndGenQuery('''set as SUM of TRANS_AMT''', 'col_1', 'CORE.TRANSACTION'))


# In[29]:


# Complex
q = Query()
print(q.parseAndGenQuery('''
Lookup PARTY_ROLE_CD from LVWS.PARTY_ROLE ON PARTY_ROLE_ID = PARTY_ROLE_ID
set X as TRANS_AMT if PARTY_ROLE_CD = "DEBTOR" ELSE 0.00
set as SUM of X
Filter by X > 5000000
''', 'col_1', 'CORE.TRANSACTION'))


# In[30]:


# Complex - Chained
q = Query()
print(q.parseAndGenQuery('''
set as SUM of ( TRANS_AMT if ( PARTY_ROLE_CD looked up from LVWS.PARTY_ROLE ON PARTY_ROLE_ID = PARTY_ROLE_ID ) = "DEBTOR" ELSE 0.00 ) where X > 5000000
''', 'col_1', 'CORE.TRANSACTION'))


# In[ ]:


# ------------------------------------------------------ Testing ------------------------------------------------------


# In[ ]:


import unittest
import pandas as pd
import numpy as np


# In[ ]:


def test(spec, colName, dfltTbl, mode='Normal'):
    Query.flush()
    Query.parseColumn(spec, colName, dfltTbl, mode)
    #for a, b, c in TokenSet.getAllTokens():
    #    print(a + " : " + b + " : " + c)
    return (Query.genSelectPart(), Query.genFromPart(), Query.genWherePart(), Query.genQuery())


# In[ ]:


unittest_data = pd.read_excel('Unit_Test_Cases.xlsx')
unittest_data = unittest_data.fillna('')
selPart, fromPart, wherePart, query = np.vectorize(test)(unittest_data['Spec'], 'col_1', unittest_data['Default Table'])
unittest_data['Select Part'] = selPart
unittest_data['From Part'] = fromPart
unittest_data['Where Part'] = wherePart
unittest_data['Query (Actual)'] = query
unittest_data['Query (Actual)'] = unittest_data['Query (Actual)'].str.strip()
unittest_data['Test Result'] = unittest_data['Query (Actual)'].str.strip() == unittest_data['Query (Expected)'].str.strip()
unittest_data.set_index('#').to_excel('Unit_Test_Cases (Results).xlsx')


# In[ ]:





# In[ ]:


Query.flush()
spec = '''
Lookup Y from Z.W ON V = X
set as A if B = Y else D
'''
print(Query.parseAndGenQuery(spec, 'col_1', 'DB.TBL'))
#TokenSet.getAllTokens()


# In[ ]:


Query.flush()
spec = '''
set X as A from B.C if D = E else F
lookup Y from Z.W ON V = X
set as P if Y = "RST" else "UVW"
'''
print(Query.parseAndGenQuery(spec, 'col_1', 'DB.TBL'))
#TokenSet.getAllTokens()


# In[ ]:


Query.flush()
spec = '''
set as P if Y = "RST" else "UVW"
set X as A from B.C if D = E else F
lookup Y from Z.W ON V = X
'''
print(Query.parseAndGenQuery(spec, 'col_1', 'DB.TBL'))


# In[ ]:


Query.flush()
spec = '''
set X as SUM of ABC from XYZ.DEF
Set as X
Filter by X > 5000000
'''
print(Query.parseAndGenQuery(spec, 'col_1', 'DB.TBL'))


# In[ ]:


Query.flush()
spec = '''
Set as A
Filter by DEF = "PQR"
'''
print(Query.parseAndGenQuery(spec, 'col_1', 'DB.TBL'))


# In[ ]:


Query.flush()
spec = 'set as A if ( sum of COL_1 ) < 1000 else D'
print(Query.parseAndGenQuery(spec, 'col_1', 'DB.TBL'))


# In[ ]:




