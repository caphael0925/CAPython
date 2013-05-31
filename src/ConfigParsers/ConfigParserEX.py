'''
Created on 2013-5-23

@author: caphael
'''

import ConfigParser


class ConfigParserEX():
    PARCONFSTR=''
    CONFFILE = ''
    CONFS = {}

    def __init__(self,parconf,conffile):
        self.PARCONFSTR = parconf
        self.CPARSER = ConfigParser.ConfigParser()
        self.get=self.CPARSER.get
        self.getint=self.CPARSER.getint
        self.getboolean = self.CPARSER.getboolean
        self.getfloat = self.CPARSER.getfloat
        
        self.loadConf(conffile)
        
    def getEX(self,option):
        return self.get(self.PARCONFSTR,option)
    
    def getintEX(self,option):
        return self.getint(self.PARCONFSTR,option)
    
    def getbooleanEX(self,option):
        return self.getboolean(self.PARCONFSTR,option) 

    def getfloatEX(self,option):
        return self.getfloat(self.PARCONFSTR,option)
    
    def read(self,filenames):
        return self.CPARSER.read(filenames)
    
    def loadConf(self,filenames):
        self.read(filenames)
        confnames = self.CPARSER.options(self.PARCONFSTR)
        for confname in confnames:
            self.CONFS[confname] = self.getEX(confname)
