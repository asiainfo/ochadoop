#!/usr/bin/env python
# -*-coding:utf-8 -*-

import getopt
import sys
sys.path.append('/home/ocdc/app/hive-0.11.0-bin/lib/py')
from thrift import Thrift
from hive_service import ThriftHive
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_service.ttypes import HiveServerException


def HiveExe(hql):
    try:
        transport = TSocket.TSocket('127.0.0.1',10000) 
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = ThriftHive.Client(protocol)
        transport.open()
        for sql in hql:
            print sql
            client.execute(sql)
            #client.fetchAll()
            print time.strftime( ISOTIMEFORMAT, time.localtime() )
            print "Successful implementation of this Sql!"
        transport.close()
    except Thrift.TException, tx:
        print '%s' % (tx.message)
        sys.exit(1)

def QueryExe(hql):
    try:
        transport = TSocket.TSocket('127.0.0.1',10000)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = ThriftHive.Client(protocol)
        transport.open()
        print hql
        client.execute(hql)
        query = client.fetchAll()
        return (query)
        print nowtimes
        print "Successful implementation of this Sql!"
        transport.close()
    except Thrift.TException, tx:
        print '%s' % (tx.message)
        sys.exit(1)
		
def Usage():
    print 'exechql.py usage:'
    print '\t-e,--sql: sql string.'
    print '\t-f, --file: sql file'
    print '\t-h, --help: print help message.'
	
if __name__=='__main__':
	
	sqlstr = ''
	opts, args = getopt.getopt(sys.argv[1:], "e:f:h", ["sql=", "file=",'help']) 
	for a,o in opts: 
		if a in ('-e', '--sql'): 
			sqlstr = o 
		elif a in ('-f', '--file'): 
			sqlfile = o 
		elif a in ('-h', '--help'): 
			Usage()
	if sqlstr != '':
		res = QueryExe(sqlstr)
		for item in res:
			print(item)
	else:
		Usage()