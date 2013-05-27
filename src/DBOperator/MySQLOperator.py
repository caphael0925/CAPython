# -*- coding:utf-8 -*-

'''
Created on 2012-11-16
my
@author: Caphael
'''

from DBOperator import DBOperator
import os
import MySQLdb as dbc
from ConfigParsers.ConfigParserEX import ConfigParserEX

from string import Template

class MySQLConfig():

    
    CONFS = None
    
    def __init__(self,type = 'mysql_statbsms'):
        os.environ['NLS_LANG']='AMERICAN_AMERICA.UTF8'
        self.CONFPARSER=ConfigParserEX(type,'/home/caphael/workspace/CAPython/conf/DataBaseConn.conf')
        self.CONFS = self.CONFPARSER.CONFS
#        self.CONFS = self.getConf()
#        
#    def getConf(self,filename = '/home/caphael/workspace/CAPython/conf/DataBaseConn.conf'):
#        self.CONFPARSER.loadConf(filename)
#        return self.CONFPARSER.CONFS
        
        
    
class MySQLOperator(DBOperator):
    '''
    classdocs
    '''
    CONFS = {}
    USERNAME = ''
    PASSWORD = ''
    IPADDR = ''
    DBNAME = ''
    CONNPOOL = []
    CONN = None
    
    
    def __init__(self,cfg):
        '''
        Constructor
        '''
        self.CONFS = cfg.CONFS
        confs = self.CONFS
        self.USERNAME = confs.get('username')
        self.PASSWORD = confs.get('password')
        self.IPADDR = confs.get('ipaddr')
        self.DBNAME = confs.get('dbname')
        
    def createConn(self):
        conn = None
        try:
            conn = dbc.connect(self.IPADDR, self.USERNAME,self.PASSWORD, self.DBNAME)
            if conn:
                print 'Connecting is Successful!!'
                self.CONNPOOL.append(conn)
            return conn
        except Exception,e:
            print e.message
            self.closeConn(conn)
    
    def appendDict2DB(self,dictlist,tabname,conn = None):
        if not conn:
            conn = self.CONN
        
        colcnt = len(dictlist)
        datalist = [cd['data'] for cd in dictlist]
        
        tpl_dict = {}
        
        tpl_dict['tab'] = tabname
        tpl_dict['cols'] = ','.join([cd['colname'] for cd in dictlist])
        tpl_dict['vals'] = ','.join(['%s' for i in range(colcnt)])        
        
        tpl=Template('insert into ${tab}(${cols}) values(${vals})')

        sqlstr = tpl.substitute(tpl_dict) 
        
        try:
            cur = conn.cursor()
            cur.executemany(sqlstr,datalist)
            conn.commit()
        except Exception,e:
            self.closeConn(conn)
            print e.message
    
    def closeConn(self,conn):
        if conn:
            conn.close()

    def closeAll(self):
        for conn in self.CONNPOOL :
            self.closeConn(conn)
    
def test():
    cfg = MySQLConfig()
    opt = MySQLOperator(cfg)
    conn = opt.createConn()
    opt.closeAll()
    
if __name__=='__main__':
    test()