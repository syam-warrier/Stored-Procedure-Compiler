import re
from TokenSet import TokenSet
import Common
from Common import regex_dict

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
        
class Function:
    def __init__(self, funcName, funcParams):
        self.funcName       =    funcName.strip()
        self.funcParams     =    funcParams.strip()
        
    def getChildren(self):
        return [self.funcName, self.funcParams]
        
    def getSelectPart(self):
        return self.funcName + '(' + self.funcParams + ')'
    
    def getTablePart(self):
        return self.funcName + ' ' + self.funcParams
    
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
        
class Tuple:
    def __init__(self, leftId, rightId):
        self.leftId         =    leftId.strip()
        self.rightId        =    rightId.strip()
        
    def getChildren(self):
        return [self.leftId, self.rightId]
        
    def getSelectPart(self):
        return self.leftId + ', ' + self.rightId
    
    def getTablePart(self):
        return self.leftId + ' ' + self.rightId
    
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
