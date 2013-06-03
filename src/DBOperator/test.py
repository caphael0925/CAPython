'''
Created on 2013-5-30

@author: caphael
'''
#
#if __name__ == '__main__':
#    pass

import cx_Oracle,pprint

dsn = cx_Oracle.makedsn('192.168.200.33',1521,'my7g2')
conn = cx_Oracle.connect('statbsms','statbsms',dsn)
cur=conn.cursor()
res=cur.execute('select * from tab where rownum<10')
pprint.pprint(res.fetchall())
cur.close()
conn.close()



