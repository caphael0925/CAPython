'''
Created on 2013-5-23

@author: caphael
'''

import ConfigParser,pprint
from CommonUtils.NotFound import NotFound

class ConfigParserEX():
    PARCONFSTR=''
    CONFFILE = ''
    CONFS = {}
    CPARSER = ConfigParser.ConfigParser()
    NOTFOUND = NotFound('NotFound')
    GETFUNCTIONS = []

    
    def __init__(self,*conffiles):
        self.GETFUNCTIONS = [self.getConfDict,self.getConfDict,self.getSectionDict,self.getOptionDict,self.getNotFound]
        for conffile in conffiles:
            self.loadConf(conffile)

    def loadConf(self,filename):
        self.CPARSER.read(filename)
        fclsstr = self.CPARSER.get('global','name')
        confdict = {}
        self.CONFS[fclsstr] = confdict
        
        sections = self.CPARSER.sections()
        
        for section in sections:
            secdict = {}
            confdict[section] = secdict
            options = self.CPARSER.options(section)
            for option in options:
                secdict[option] = self.CPARSER.get(section, option)

    def getConfDict(self,confname = 'root'):
        if confname == 'root':
            return self.CONFS
        return self.CONFS.get(confname,self.NOTFOUND)
    
    def getSectionDict(self,confname,secname):
    	return self.getConfDict(confname).get(secname,self.NOTFOUND)
    
    def getOptionDict(self,confname,secname,optname):
    	return self.getSectionDict(confname, secname).get(optname,self.NOTFOUND)
    
    def getNotFound(self,*p):
    	return 'NotFound'
    
    def getDictByPath(self,pathstr='root'):
    	nodes = pathstr.split('.')
    	nodenum = len(nodes)
    	if nodenum > 4:
    		nodenum = 4
    	return self.GETFUNCTIONS[nodenum](*nodes)

def test():
    cp = ConfigParserEX('/home/caphael/workspace/PyBSMSStat.my7g/conf/LineParsers.conf')
    pprint.pprint(cp.getDictByPath('lineparser.sendlog.order'))


if __name__=='__main__':
    test()