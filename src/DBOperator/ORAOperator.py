'''
Created on 2012-11-16
my
@author: Caphael
'''
from DBOperator import DBOperator
from DBOperator import DBConfig

import cx_Oracle
import os
from string import Template

class ORAConfig(DBConfig):
    DB_USERNAME='test'
    DB_PASSWORD='test'
    DB_IPADDR='192.168.136.128'
    DB_PORT=1521
    DB_NAME='mytest'
    
    def get_dsn(self):
        return cx_Oracle.makedsn(self.DB_IPADDR,self.DB_PORT,self.DB_NAME)
    
    DB_DSN=property(get_dsn)
    
    def __init__(self):
        os.environ['NLS_LANG']='AMERICAN_AMERICA.UTF8'
    
    
class ORAOperator(DBOperator):
    '''
    classdocs
    '''
    
    def __init__(self,oracfg):
        '''
        Constructor
        '''
        self.DB_USERNAME=oracfg.DB_USERNAME
        self.DB_PASSWORD=oracfg.DB_PASSWORD
        self.DB_DSN=oracfg.DB_DSN

    def write_dict2oracle(self,tab,ar_list):
        dictlist=ar_list
#    Get the records list
        def get_rec_list(sub_dlist,count=10000):
            recs=[]
            remcount=count
            
            for d in sub_dlist:
                rec=[]
                for c in cols:
                    rec.append(str(d[c]))
                recs.append(rec)
                remcount=remcount-1
                if remcount<=0:
                    break
            return sub_dlist[count:],recs

#    Generate the SQL String:
        tpl_ins=Template('insert into ${tab}(${cols}) values(${vals})')
        tpl_dict={}
        
        cols=sorted(dictlist[0].keys())
        tpl_dict['tab']=tab
        tpl_dict['cols']=','.join(cols)
        tpl_dict['vals']=','.join([':'+ str(x) for x in range(1,len(cols)+1)])
        
        sqlstr=tpl_ins.substitute(tpl_dict)

        try:
            conn=cx_Oracle.connect(self.DB_USERNAME,self.DB_PASSWORD,self.DB_DSN)
            cur=conn.cursor()
            
            while dictlist:
                dictlist,rows=get_rec_list(dictlist)
                cur.executemany(sqlstr,rows)
            
        except Exception,e:
            print e.message
            conn.rollback()
        finally:
            conn.commit()
            cur.close()
            conn.close()
        