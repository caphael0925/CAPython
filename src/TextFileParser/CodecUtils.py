'''
Created on 2013-6-4

@author: caphael
'''
import re

DIGITMAP = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
DEC = len(DIGITMAP)

MOBILE_PREFIX = ['134', '135', '136', '137', '138', '139', '150', '151','152', '158', '159', '182', '187', '147', '157', '188','130', '131', '132', '155', '156', '185', '186', '133','153', '180', '189', '181', '183', '184', '154', '145']
ERRMSG = {'fmt':'Coded string format error!','pref':'The MobileNumber has wrong prefix!!'}


def decode(codestr):
    if not re.match('^[0-9A-Z]{12}$', codestr):
        return [ERRMSG['fmt'] for i in (1,1)]
    
    jobid = codestr[:5]
    mobilenum = codestr[5:]
    
    return jobid,decodeMobileNumber(mobilenum)

def code2Decimal(codestr):
    digilist = list(codestr)
    for i,digi in enumerate(digilist):
        digilist[i] = DIGITMAP.find(digi)
    digilist.reverse()
    
    decnum = 0
    for i,digi in enumerate(digilist):
        decnum += digi * ( DEC ** i)
    
    return decnum

def decodeMobileNumber(codestr):
    prefix = codestr[0]
    prefidx = code2Decimal(prefix)
    if prefidx >= len(MOBILE_PREFIX) or prefidx<0:
        return ERRMSG['pref']
    
    postfix = codestr[1:]
    return MOBILE_PREFIX[prefidx] + str(code2Decimal(postfix))

if __name__ != '__main__':
    pass
#
#print decode('YH01EM14HVO0')