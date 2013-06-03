# -*- coding:utf-8 -*-
'''
Created on 2013-5-22

@author: caphael
'''

import json
import pprint
import datetime
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
    TABMETA = {}
#    SOURCE = ()
    TARGET = []    #放数据的字典
    DEFMUL_LIMITS = 1000
    
    TARGET_TYPE = 'C'  
    DATAPOS = None
    #输出数据在输入行中对应的位置下标，维度根据LINECNT而定
    
    def __init__(self,confdict,targettype = 'R'):
        '''
        Constructor
        '''
        self.TARGET_TYPE = targettype
        self.CONFS = confdict
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
        
        if self.TARGET_TYPE == 'R':
            self.DATAPOS = tuple(datapos)
        else:
            self.DATAPOS = zip(*tuple(datapos))
            
    def parseMetadata(self,mdatastr):
        self.METADATA = json.loads(mdatastr,encoding='utf8')
        self.TABMETA = self.METADATA.get('table')
        self.COLMETA = self.TABMETA.get('columns')
        self.COLCNT = len(self.COLMETA)

    def parseLine(self,line=''):
        source = line.split(self.INSEP)
        
        self.genTarget(source)
        self.postDealTarget()
    
    def parseMultiLine(self,infile,limits = DEFMUL_LIMITS):
        linesrem = limits
        self.initTarget()
        for line in infile:
            source = line.split(self.INSEP)
            self.genTarget(source)
            linesrem-=1
            if linesrem <= 0:
                break
        self.postDealTarget()
        return linesrem != limits
        
    def postDealTarget(self):
        if self.TARGET_TYPE == 'R':
            self.postDealTargetByRow()
        else:
            self.postDealTargetByColumn()
        
    def postDealTargetByColumn(self):
        for idx,col in enumerate(self.TARGET):
            colmeta = self.COLMETA[idx]
            coltype = colmeta.get('type')
            if coltype == 'datetime':
                for i in range(len(col)):
                    col[i] = self.timeNormalize(col[i], colmeta.get('format'))
#                if coltype not in ('int','float'):
#                    row[colidx] = "'"+row[colidx]+"'"

    def postDealTargetByRow(self):
        for row in self.TARGET:
            for i,colmeta in enumerate(self.COLMETA):
                if colmeta.get('type') == 'datetime':
                    row[i] = self.timeNormalize(row[i], colmeta.get('format'))

    def timeNormalize(self,datestr,ofmt):
        try:
            t =  datetime.datetime.strptime(datestr,ofmt)
            return t
        except Exception,e:
            print e.message
            return datetime.datetime.strptime('0001-01-01 00:00:00',ofmt)
    
    def genTarget(self,source):    
        if self.TARGET_TYPE == 'R':
            self.genTargetByRow(source)
        else:
            self.genTargetByColumn(source)

    def genTargetByColumn(self,source):
        for i,colidxes in enumerate(self.DATAPOS):
            col = []
            for idx in colidxes:
                col.append(source[idx])
            self.TARGET[i].extend(col)

    def genTargetByRow(self,source):
        for rpos in self.DATAPOS:
            row = []
            for idx in rpos:
                row.append(source[idx])
            self.TARGET.append(row)
    
    def initTarget(self):
        if self.TARGET_TYPE == 'R':
            self.initTargetByRow()
        else:
            self.initTargetByColumn()
                
    def initTargetByColumn(self):
        self.TARGET = [[] for i in range(self.COLCNT)]
    
    def initTargetByRow(self):
        self.TARGET = []
    
    def getOutCList(self):
        if self.TARGET_TYPE == 'R':
            return zip(*self.TARGET)
        else:
            return self.TARGET
        
    def getOutRList(self):
        if self.TARGET_TYPE =='R':
            return self.TARGET
        else:
            return zip(*self.TARGET)
        
    def getColMeta(self):
        return self.COLMETA
    
    def getTableData(self):
        if self.TARGET_TYPE == 'R':
            return self.getTableDataByRow()
        else:
            return self.getTableDataByColumn()
            
    def getTableDataByColumn_old(self):
        tabdict = copy.deepcopy(self.TABMETA)
        tabdict['style'] = 'C'
        coldicts = tabdict.get('columns')
        for i in range(len(coldicts)):
            coldicts[i]['data'] = self.getOutCList()[i]
        return tabdict

    def getTableDataByColumn(self):
        tabdict = {}
        tabdict['mdata'] = copy.deepcopy(self.TABMETA)
        tabdict['mdata']['style'] = 'C'
        tabdict['data'] = self.getOutCList()
        return tabdict

    def getTableDataByRow(self):
        tabdict = {}
        tabdict['mdata'] = copy.deepcopy(self.TABMETA)
        tabdict['mdata']['style'] ='R'
        tabdict['data'] = self.getOutRList()
        return tabdict

def test():
    pass

if __name__=='__main__':
    test()