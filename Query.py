from OrderedSet import OrderedSet
from Parser import Parser
from Common import keywords, regex_dict, aggregate_functions

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
        p = Parser()
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