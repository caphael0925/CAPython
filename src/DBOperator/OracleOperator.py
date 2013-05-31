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
    STORED_TYPE = 'C'
    
    def __init__(self,confparser):
        '''
        Constructor
        '''
        self.CONFS = confparser.CONFS
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
        lineparser = LineParserBase(cfgparser,self.STORED_TYPE)
        while lineparser.parseMultiLine(infile, 100):
            self.appendDict2DB(lineparser.getTableData(), conn)
    
    def appendDict2DB(self,tabdict,conn = None):
        if not conn:
            conn = self.CONN
        
        colmeta = tabdict.get('columns')
        colcnt = len(colmeta)
        data = tabdict.get('row_data')
        
        tpl_dict = {}
        
        tpl_dict['tab'] = tabdict.get('table_name')
        tpl_dict['cols'] = ','.join([cd['column_name'] for cd in colmeta])
        tpl_dict['vals'] = ','.join([':'+str(i+1) for i in range(colcnt)])        
        
        tpl=Template('insert into ${tab}(${cols}) values(${vals})')

        sqlstr = tpl.substitute(tpl_dict) 
        
        try:
            cur = conn.cursor()
            cur.prepare(sqlstr)
            deallines = cur.executemany(None,data)
            conn.commit()
            print str(deallines) + ' lines has been inserted!!'
        except Exception,e:
            print e.message
            self.closeConn(conn)

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