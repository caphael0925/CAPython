# -*- coding:utf-8 -*-
'''
Created on 2013-5-22

@author: caphael
'''

import json
import pprint
import time
import copy
from ConfigParsers.ConfigParserEX import ConfigParserEX

class LineParserBase():
    '''
    classdocs
    '''
    INSEP = ''
    CONFS = {}         #用于存放配置信息的字典  
    METAJSONSTR = ''       #Metadata的Json字符串
    METADATA = None     #由output_metadata配置字符串生成的字典，格式为json
    UNIDATEFMT = '%Y-%m-%d %H:%M:%S'    #标准日期格式化字符串
    COLCNT = 0
    COLMETA = {}
#    SOURCE = ()
    TARGET = []    #放数据的字典
    DEFMUL_LIMITS = 1000
    DATAPOS_COL = ()      #输出数据在输入行中对应的位置下标，维度根据LINECNT而定

    def __init__(self,confparser):
        '''
        Constructor
        '''
        self.CONFS = confparser.CONFS
        posstr = self.CONFS.get('position_string','UNKNOWN')
        self.parsePos(posstr)
        self.METAJSONSTR = self.CONFS.get('output_metadata','UNKNOWN')
        self.INSEP = self.CONFS.get('input_separator')
        
        if self.INSEP[0] == '\'':
            self.INSEP = self.INSEP[1:-1]
            
        self.parseMetadata(self.METAJSONSTR)
        
    def parsePos(self,posstr):
        datapos = []
        posstrs = posstr.split(';')
        
        for posstrl in posstrs:
            datapos.append(tuple([int(x) for x in posstrl.split(',')]))
        
        self.DATAPOS_COL = zip(*tuple(datapos))
            
    def parseMetadata(self,mdatastr):
        self.METADATA = json.loads(mdatastr,encoding='utf8')
        self.COLMETA = self.METADATA.get('table').get('columns')
        self.COLCNT = len(self.COLMETA)



    def parseLine(self,line=''):
        source = line.split(self.INSEP)
        
        self.genTarget(source)
        self.postDealTarget()
    
    def parseMultiLine(self,infile,limits = DEFMUL_LIMITS):
        linesrem = limits
        self.initTarget()
        for line in infile:
            if linesrem <= 0:
                break
            source = line.split(self.INSEP)
            self.genTarget(source)
            linesrem-=1
        self.postDealTarget()
        
    def postDealTarget(self):
        
        for idx,col in enumerate(self.TARGET):
            colmeta = self.COLMETA[idx]
            coltype = colmeta.get('type')
            if coltype == 'datetime':
                for i in range(len(col)):
                    col[i] = self.timeNormalize(col[i], colmeta.get('format'))
#                if coltype not in ('int','float'):
#                    row[colidx] = "'"+row[colidx]+"'"

    def timeNormalize(self,datestr,ofmt):
        try:
            t =  time.mktime(time.strptime(datestr,ofmt))
            s =  time.strftime(self.UNIDATEFMT,time.localtime(t))
            return s
        except Exception,e:
            print e.message
            return '0000-00-00 00:00:00'
        
    def genTarget(self,source):
        for i,colidxes in enumerate(self.DATAPOS_COL):
            col = []
            for idx in colidxes:
                col.append(source[idx])
            self.TARGET[i].extend(col)
    
    
    def initTarget(self):
        self.TARGET = [[] for i in range(self.COLCNT)]
    
    def getOutCList(self):
        return self.TARGET
        
    def getOutRList(self):
        return zip(*self.TARGET)
    
    def getColMeta(self):
        return self.COLMETA
    
    def getTableData(self):
        coldicts = copy.deepcopy(self.COLMETA)
        for i in range(len(coldicts)):
            coldicts[i]['data'] = self.TARGET[i]
        return coldicts
        
def test():
    pass

if __name__=='__main__':
    test()