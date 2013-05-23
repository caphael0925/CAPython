# -*- coding:utf-8 -*-
'''
Created on 2013-5-22

@author: caphael
'''

import ConfigParser

class ConfigParserEX():
    PARCONFSTR=''
    CPARSER = ConfigParser.ConfigParser()
    CONFS = {}

    def __init__(self,parconf):
        self.PARCONFSTR = parconf
        
        self.get=self.CPARSER.get
        self.getint=self.CPARSER.getint
        self.getboolean = self.CPARSER.getboolean
        self.getfloat = self.CPARSER.getfloat
        
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

class LineParserBase():
    '''
    classdocs
    '''
    TYPE='test'      #解析器的配置名称，对应配置文件中的Section
    CONFPARSER=ConfigParserEX(TYPE)
                    #配置解析器
    CONFS = {}         #用于存放配置信息的字典  
    LINECNT = 0
    
    SOURCE = ()
    TARGET = []    #放数据的元组，维度根据LINECNT而定 
    
    DATAPOS = ()      #输出数据在输入行中对应的位置下标，维度根据LINECNT而定

    def __init__(self,typename):
        '''
        Constructor
        '''
        self.TYPE = typename
        
    def getConf(self,filenames = '/home/caphael/workspace/CAPython/conf/LineParsers.conf'):
        self.CONFPARSER.loadConf(filenames)
    
        self.CONFS=self.CONFPARSER.CONFS
        posstr = self.CONFS.get('position_string','UNKNOWN')
        self.parsePos(posstr)
        
        print self.DATAPOS
        print self.CONFS
        
    def parsePos(self,posstr):
        datapos = []
        posstrs = posstr.split(';')
        
        for posstrl in posstrs:
            datapos.append(tuple([int(x) for x in posstrl.split(',')]))
        
        self.DATAPOS = tuple(datapos)
        self.LINECNT = len(self.DATAPOS)
        
    def genContainer(self):
        pass
        
        
    def parseLine(self,line=''):
        self.SOURCE = line.split(self.CONFS['input_separator'])
        self.genTarget()
    
    def genTarget(self):
        for idxrow in self.DATAPOS:
            row = []
            for idx in idxrow:
                row.append(self.SOURCE[idx])
            self.TARGET.append(row)

    def getOutList(self):
        return self.TARGET
        
class LineParserBSMS(LineParserBase):
    pass

def test():
    lp=LineParserBase('test')
    lp.getConf()
    lp.parseLine('0,1,2,3,4,5,6,7,8,9')
    print lp.getOutList()
    
test()