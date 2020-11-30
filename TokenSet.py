from collections import OrderedDict
from random import randint

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
