# -*- coding:utf-8 -*-

'''
Created on 2012-11-16
my
@author: Caphael
'''

from DBOperator import DBOperator
import os,json
from TextFileParser.LineParser import LineParserBase
from ConfigParsers.ConfigParserEX import ConfigParserEX
import cx_Oracle as dbc

from string import Template
    
class OracleOperator(DBOperator):
    '''
    classdocs
    '''
    
    USERNAME = ''
    PASSWORD = ''
    IPADDR = ''
    DBNAME = ''
    PORT = 1521
    CONN = None
    DSN = None
    UNIDATEFMT = '%Y-%m-%d %H:%M:%S'    
    LINES_LIMITS = 50
    DEALLINES = 0

    
    def __init__(self,confs):
        '''
        Constructor
        '''
        self.CONNPOOL = []
        self.PROC_PARAMS = []
        self.DEALTYPE_MAPPING = {}
        self.CONFS = {}
        
        self.CONFS = confs
        jsonstr = confs.get('dealtype_mapping')
        self.DEALTYPE_MAPPING = json.loads(jsonstr,encoding='utf8')
        self.USERNAME = confs.get('username')
        self.PASSWORD = confs.get('password')
        self.IPADDR = confs.get('ipaddr')
        self.DBNAME = confs.get('dbname')
        self.PORT = confs.get('port')
        self.DSN = dbc.makedsn(self.IPADDR,self.PORT,self.DBNAME)
        self.CONN = self.createConn()
    
    def createConn(self):
        conn = None
        try:
            
            conn = dbc.connect(self.USERNAME, self.PASSWORD,self.DSN)
            if conn:
                print 'Connecting is Successful!!'
                self.CONNPOOL.append(conn)
            return conn
        except Exception,e:
            print e.message
            self.closeConn(conn)

    def applyFile2DB(self,infile,confs,conn = None):
        self.DEALLINES = 0
        lineparser = LineParserBase(confs)
        while lineparser.parseMultiLine(infile, self.LINES_LIMITS):
            self.applyDict2DB(lineparser.getOutData(), conn)
        return self.DEALLINES
            
    def applyDict2DB(self,tabdict,conn = None):
        self.PROC_PARAMS = []
        
        if not conn:
            conn = self.CONN
        
        deallines = 0
        mdata = tabdict.get('mdata')
        data = tabdict.get('data')
        command = mdata.get('command')
        
        try:
            cur = conn.cursor()
            
            colmdatas = mdata.get('columns');
            
            mtype = mdata.get('type')
            if mtype == 'procedure':
                self.PROC_PARAMS.append(str(mdata).replace("u'", "'"))
                self.PROC_PARAMS.extend(data)
                self.prepareArrayParams(cur,colmdatas)
                cur.callproc(self.DEALTYPE_MAPPING.get(command),self.PROC_PARAMS)
                deallines = len(data[0])
            if mtype == 'sql':
                cur.executemany(command,data)
                deallines = len(data)
            conn.commit()
            cur.close()
        except Exception,e:
            print e.message
            self.closeConn(conn)
        self.DEALLINES += deallines
            
    def prepareArrayParams(self,cur,colmdatas):
        for i,colmdata in enumerate(colmdatas):
            coltype = colmdata.get('type')
            if coltype.lower().startswith('date'):
                cur.arrayvar(dbc.DATETIME,self.PROC_PARAMS[i+1])
            elif coltype.lower() == 'number':
                cur.arrayvar(dbc.NUMBER,self.PROC_PARAMS[i+1])
            else:
                cur.arrayvar(dbc.STRING,self.PROC_PARAMS[i+1]);
                    
    def closeConn(self,conn):
        if conn:
            conn.close()

    def closeAll(self):
        for conn in self.CONNPOOL :
            self.closeConn(conn)
    
def test():
#    cfg = MySQLConfig()
#    opt = MySQLOperator(cfg)
#    conn = opt.createConn()
#    opt.closeAll()
    pass
    
if __name__=='__main__':
    test()