import re
from TokenSet import TokenSet
from Common import keywords, regex_dict, unique
from DataTypes import *

class Parser:
    
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
            for patternName, regexString in regex_dict.items():
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
                        
                offset = 0
                for match in re.finditer(regexString, string, flags=re.IGNORECASE):
                    try:
                        matchedString      = match.group()
                        if mode == 'Debug':
                            print('1. matched string : ' + matchedString)
                            
                        matchedStringParts = match.groups()
                        if mode == 'Debug':
                            print('2. matched string : ' + str(matchedStringParts) + "(" + str(match.start()) + ", " + str(match.end()) + ")")
                            print('2a. Offset:' + str(offset))
                            
                        if matchedString.strip() not in keywords:
                            if mode == 'Debug':
                                print('3. Start loop : ' + matchedString)
                    
                            if patternName == 'STRING':
                                if mode == 'Debug':
                                    print('4. Detected as STRING')
                                token = TokenSet.registerToken(String(matchedStringParts[0]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + ' >.')
                                #string = re.sub(matchedString, ' ' + token + ' ', string)
                                string = string[0:match.start() + offset] + token + string[match.end() + offset:len(string) + offset]
                                offset += len(token) - (match.end() - match.start())
                                
                            if patternName == 'NUMBER':
                                if mode == 'Debug':
                                    print('4. Detected as NUMBER')
                                token = TokenSet.registerToken(Number(matchedStringParts[0]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <' + ' ' + token + ' >.')
                                string = string[0:match.start() + offset] + token + string[match.end() + offset:len(string) + offset]
                                offset += len(token) - (match.end() - match.start())
                                
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
                                string = re.sub(matchedString, token, string)
                                if mode == 'Debug':
                                    print('5b. Replacing <' + matchedStringParts[0] + '> with <' + ' ' + token + ' >.')
                                string = re.sub(matchedStringParts[0], token, string)
                                
                            elif patternName == 'CONDITION':
                                if mode == 'Debug':
                                    print('4. Detected as CONDITION')
                                token = TokenSet.registerToken(Condition(matchedStringParts[0], matchedStringParts[1], matchedStringParts[2]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + ' ' + '>.')
                                string = string[0:match.start() + offset] + token + string[match.end() + offset:len(string)]
                                offset += len(token) - (match.end() - match.start())
                                
                                break
                                
                            elif patternName == 'FUNCTION':
                                if mode == 'Debug':
                                    print('4. Detected as FUNCTION')
                                token = TokenSet.registerToken(Function(matchedStringParts[0], matchedStringParts[1]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + ' ' + '>.')
                                string = string[0:match.start() + offset] + token + string[match.end() + offset:len(string)]
                                offset += len(token) - (match.end() - match.start())
                                
                                break
                                
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
                                
                            elif patternName == 'TUPLE':
                                if mode == 'Debug':
                                    print('4. Detected as TUPLE')
                                token = TokenSet.registerToken(Tuple(matchedStringParts[0], matchedStringParts[1]))
                                
                                if mode == 'Debug':
                                    print('5. Replacing <' + matchedString + '> with <'+ ' ' + token + ' ' + '>.')
                                string = re.sub(matchedString, ' ' + token + ' ', string)
                            
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
