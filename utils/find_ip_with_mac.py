#!/usr/bin/env python
#coding=utf-8
import sys
import nmap 

mac = raw_input('Enter your words: ')
mac = mac.strip()
mac = mac.upper()
print mac   					
'''if len(mac)!=1:									
    print "Input errors""  
    sys.exit(0) '''

try:
    nm = nmap.PortScanner()    #创建端口扫描对象
    print ('nmap ready')
except nmap.PortScannerError:
    print('Nmap not found please yum -y install nmap  and pip install python-nmap==0.4.0', sys.exc_info()[0])
    sys.exit(0)
except:
    print("Unexpected error:", sys.exc_info()[0])
    sys.exit(0)

try:
    print('scan begin')
    nm.scan('10.239.47.1-254', arguments='-O -F')
    print('scan over')    
except Exception,e:
    print "Scan erro:"+str(e)
        
for h in nm.all_hosts():   
   # print('----------------------------------------------------')
    f = nm[h]['addresses'].has_key('mac')
    if f:
        if mac in nm[h]['addresses'].values():
            print(nm[h]['addresses'])
    else:
        print(nm[h]['addresses'])
print('run over')   
   

   
