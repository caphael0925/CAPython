# -*- coding:utf-8 -*-

'''
Created on 2012-11-16
my
@author: Caphael
'''

from DBOperator import DBOperator
import os
from TextFileParser.LineParser import LineParserBase
from ConfigParsers.ConfigParserEX import ConfigParserEX
import cx_Oracle as dbc

from string import Template
    
class OracleOperator(DBOperator):
    '''
    classdocs
    '''
    CONFS = {}
    USERNAME = ''
    PASSWORD = ''
    IPADDR = ''
    DBNAME = ''
    CONNPOOL = []
    PORT = 1521
    CONN = None
    DSN = None
    UNIDATEFMT = '%Y-%m-%d %H:%M:%S'    
    STORED_STYLE = 'C'
    LINES_LIMITS = 100
    PROC_PARAMS = []
    
    def __init__(self,confdict):
        '''
        Constructor
        '''
        self.CONFS = confdict
        confs = self.CONFS
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
    
    def appendFile2DB(self,infile,cfgparser,conn = None):
        lineparser = LineParserBase(cfgparser,self.STORED_STYLE)
        while lineparser.parseMultiLine(infile, self.LINES_LIMITS):
            self.appendDict2DB(lineparser.getTableData(), conn)

    def appendDict2DB(self,tabdict,conn = None):
        self.PROC_PARAMS = []
        
        if not conn:
            conn = self.CONN
        
        deallines = 0
        mdata = tabdict.get('mdata')
        data = tabdict.get('data')
        
        if mdata.get('style') == 'R':
            data = zip(*data)
    
        self.PROC_PARAMS.append(str(mdata).replace("u'", "'"))
        self.PROC_PARAMS.extend(data)
        

        
        try:
            cur = conn.cursor()
            
            colmdatas = mdata.get('columns');
            
            self.prepareArrayParams(cur,colmdatas)

            cur.callproc('proc_timemark_append',self.PROC_PARAMS)
            deallines = len(data[0])
            conn.commit()
            cur.close()
        except Exception,e:
            print e.message
            self.closeConn(conn)
        print str(deallines) + ' lines has been inserted!!'
        
    def appendRDict2DB_SQL(self,tabdict,conn):

        deallines = 0
        colmeta = tabdict.get('columns')
        colcnt = len(colmeta)
        data = tabdict.get('data')
        
        tpl_dict = {}
        
        tpl_dict['tab'] = tabdict.get('table_name')
        tpl_dict['cols'] = ','.join([cd['column_name'] for cd in colmeta])
        tpl_dict['vals'] = ','.join([':'+str(i+1) for i in range(colcnt)])        
        
        tpl=Template('insert into ${tab}(${cols}) values(${vals})')

        sqlstr = tpl.substitute(tpl_dict) 
        
        try:
            cur = conn.cursor()
            cur.prepare(sqlstr)
            cur.executemany(None,data)
            lines = len(data)
            conn.commit()
        except Exception,e:
            print e.message
            self.closeConn(conn)
        print str(deallines) + ' lines has been inserted!!'
    
    def updateDict2DB_SQL(self,tabdict,conn = None):
        
        self.PROC_PARAMS = []
        
        if not conn:
            conn = self.CONN
            
        deallines = 0
        mdata = tabdict.get('mdata')
        data = tabdict.get('data')
        if mdata.get('style') == 'R':
            data = zip(*data)
            
        self.PROC_PARAMS.append(str(mdata).replace("u'", "'"))
        self.PROC_PARAMS.extend(data)
        
        try:
            cur = conn.cursor()
            
            colmdatas = mdata.get('columns');
            
            self.prepareArrayParams(cur,colmdatas)

            cur.callproc('proc_timemark_append',self.PROC_PARAMS)
            deallines = len(data[0])
            conn.commit()
            cur.close()
        except Exception,e:
            print e.message
            self.closeConn(conn)
        print str(deallines) + ' lines has been inserted!!'

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