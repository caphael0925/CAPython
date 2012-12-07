'''
Created on 2012-12-7

@author: Caphael
'''

if __name__ == '__main__':
    pass

import sys

inum=input('Please input a number:')
factor=0
factors=[]
primelist=[2]
i=1
prod=inum

def get_factor(prod):
    global primlist
    if prod in primelist:
        return prod
    for p in primelist:
        if prod%p==0:
            return p
    while p<=prod:
        if prod%p==0:
            primelist.append(p)
            return p
        else:
            p=p+1
    return 0

while prod>1:
    factor=get_factor(prod)
    if factor==0 :
        break
    factors.append(factor)
    prod=prod/factor

print factors

#def get_next_factor(prod):
#    if prod in prime:
#        factor.append(prod)
#    else:
#        for i in prime:
#            if prod%i==0:
#                factor.append(i)
#                prod=prod/i
#                break
#
#        while i<prod:
#            i=i+1
#            if prod%i==0:            
#                factor.append(i)
#                prod=prod/i
#                prime.append(i)
#        get_next_factor()
        
