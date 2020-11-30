aggregate_functions = ["SUM", "MAX", "MIN"]

keywords      =    ['SET', 'AS', 'IF', 'THEN', 'ELSE', 'ON', 'AND', 'LOOKUP', 'LOOKED-UP', 'FILTER-BY', 'WHERE', 'SUM', 'MIN', 'MAX', 'OF']
regex_dict    =    {
                        'STRING'       :    r'(\"\w+\")',
                        'NUMBER'       :    r'\b((?!_)\d+(?:\.\d+)?)\b',
                        'COLUMN1'      :    r'(\b\w+\b)(\sFROM\s\s\w+\.\w+\s)',
                        'TABLE'        :    r'\sFROM\s(\s\w+\.\w+\s)',
                        'COLUMN2'      :    r'(\b\w+\b)(?!\()',
                        
                        'FUNCTION'     :    r'(\w+)\s*\(\s*(__\d+__)?\s*\)',
                        'CONDITION'    :    r'(__\d+__)\s*(=|<|>|<=|>=|!=|\|\|)\s*(__\d+__)',
                        'BOOLEAN'      :    r'\s(__\d+__)\s\s(AND|OR)\s\s(__\d+__)\s',
                        'AGGREGATE'    :    r'\s(\(\s*)?(' + '|'.join(aggregate_functions) + ')\s\sOF\s\s(__\d+__)(\s*\))?\s',
                        'FILTER'       :    r'\s(FILTER-BY|WHERE)\s\s(__\d+__)\s',
                        'IF'           :    r'\s(\(\s*)?(__\d+__)\s\sIF\s\s(__\d+__)\s\sELSE\s\s(__\d+__)(\s*\))?\s',
                        'LOOKUP'       :    r'\s(\(\s*)?(\sLOOKUP\s)?\s(__\d+__)\s(\sLOOKED-UP\s\s(__\d+__)\s)?\sON\s\s(__\d+__)(\s*\))?\s',
                        'SETXASY'      :    r'\sSET\s\s(__\d+__)\s\sAS\s\s(__\d+__)\s',
                        'SETAS'        :    r'\sSET\s\sAS\s\s(__\d+__)\s+[$|\n]',
                        'TUPLE'        :    r'(__\d+__)\s*,\s*(__\d+__)',
                    }

def unique(lst: list):
    res_list = []
 
    for item in lst: 
        if item not in res_list:
            res_list.append(item)
        
    return res_list

