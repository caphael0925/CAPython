# -*- coding:utf-8 -*-
'''
Created on 2013-5-22

@author: caphael
'''

import json
import pprint
import datetime
import copy
from TextFileParser import CodecUtils
from ConfigParsers.ConfigParserEX import ConfigParserEX


class LineParserBase():
    '''
    classdocs
    '''
    INSEP = ''
    METAJSONSTR = ''       #Metadata的Json字符串
    METADATA = None     #由output_metadata配置字符串生成的字典，格式为json
    UNIDATEFMT = '%Y-%m-%d %H:%M:%S'    #标准日期格式化字符串
    COLCNT = 0

#    SOURCE = ()
    
    DEFMUL_LIMITS = 1000
    
    TARGET_TYPE = 'C'  
    DATAPOS = None
    #输出数据在输入行中对应的位置下标，维度根据LINECNT而定
    TARGET = None
    #放数据的二维列表
    
#    TRANSCONFS = None
#    
#    RANSFORMERS = None       #存放转换器的字典
##    TRANSFORMER_OUT = []    #存放转换器输出结果的列表
#    COLMETA = None
#    TABMETA = None
#    CONFS = None         #用于存放配置信息的字典  
    
    def __init__(self,confdict):
        '''
        Constructor
        '''
        self.TRANSCONFS = []
    
        self.TRANSFORMERS = {}       #存放转换器的字典
#    TRANSFORMER_OUT = []    #存放转换器输出结果的列表
        self.COLMETA = {}
        self.TABMETA = {}
        self.CONFS = {}         #用于存放配置信息的字典  
        
        self.registerTransformers()
        
        self.CONFS = confdict
        transstrs = confdict.get('transformer','UNKNOWN')
        if transstrs != 'UNKNOWN':
            self.parseTrans(transstrs)
        mappos = confdict.get('mapping_positions','UNKNOWN')
        self.METAJSONSTR = confdict.get('output_metadata','UNKNOWN')
        self.INSEP = confdict.get('input_separator')
        
        if self.INSEP[0] == '\'':
            self.INSEP = self.INSEP[1:-1]
            
        self.parseMetadata(self.METAJSONSTR)
        self.DEALTYPE = self.METADATA.get('type')
        if self.DEALTYPE == 'sql':
            self.TARGET_TYPE = 'R'
        self.parsePos(mappos)
        
    def registerTransformers(self):
        memberdict = vars(self.__class__)
        for key,val in memberdict.iteritems():
            if key.startswith('_trans_'):
                self.TRANSFORMERS[key[7:].lower()] = val
    
    def parseTrans(self,transstrs):
        self.TRANSCONFS = []
        transstrlist = transstrs.split(';')
        for transstr in transstrlist:
            transconf = transstr.split(':')
            transconf[1] = transconf[1].split(',')
            self.TRANSCONFS.append(transconf)
    
    def parsePos(self,posstr):
        datapos = []
        posstrs = posstr.split(';')
        
        for posstrl in posstrs:
            datapos.append(tuple([x for x in posstrl.split(',')]))
        
        if self.TARGET_TYPE == 'R':
            self.DATAPOS = tuple(datapos)
        else:
            self.DATAPOS = zip(*tuple(datapos))
            
    def parseMetadata(self,mdatastr):
        self.METADATA = json.loads(mdatastr,encoding='utf8')
        self.COLMETA = self.METADATA.get('columns')
        self.COLCNT = len(self.COLMETA)

    def transform(self,source):
        transout = []
        for transconf in self.TRANSCONFS:
            transinput = [source[int(i)-1] for i in transconf[1]]
            transout.append(self.TRANSFORMERS[transconf[0]](self,*transinput))
        return transout
            
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
            return datetime.datetime.strptime('0001-01-01 00:00:00',self.UNIDATEFMT)
    
    def genTarget(self,source):
        try:
            transout = self.transform(source)
            if self.TARGET_TYPE == 'R':
                self.genTargetByRow(source,transout)
            else:
                self.genTargetByColumn(source,transout)
        except Exception,e:
            print e.message

    def genTargetByColumn(self,source,transout):
        for i,colidxes in enumerate(self.DATAPOS):
            col = []
            for idx in colidxes:
                if idx.find('.') < 0:
                    col.append(source[int(idx)-1])
                else:
                    idx = [int(x)-1 for x in idx.split('.')]
                    col.append(transout[idx[0]][idx[1]])
            self.TARGET[i].extend(col)

    def genTargetByRow(self,source,transout):
        for rpos in self.DATAPOS:
            row = []
            for idx in rpos:
                if idx.find('.') < 0:
                    row.append(source[int(idx)-1])
                else:
                    idx = [int(x)-1 for x in idx.split('.')]
                    row.append(transout[idx[0]][idx[1]])
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
    
    def getOutData(self):
        if self.TARGET_TYPE == 'R':
            return self.getOutDataByRow()
        else:
            return self.getOutDataByColumn()
            
    def getOutDataByColumn(self):
        tabdict = {}
        tabdict['mdata'] = copy.deepcopy(self.METADATA)
        tabdict['mdata']['style'] = 'C'
        tabdict['data'] = self.getOutCList()
        return tabdict

    def getOutDataByRow(self):
        tabdict = {}
        tabdict['mdata'] = copy.deepcopy(self.METADATA)
        tabdict['mdata']['style'] = 'R'
        tabdict['data'] = self.getOutRList()
        return tabdict
        
    def _trans_Decode(self,codestr):
        return CodecUtils.decode(codestr)
    
def test():
    pass

if __name__=='__main__':
    test()