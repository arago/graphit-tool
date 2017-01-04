#!/usr/bin/env python2
"""graphit-tool

Usage:
  graphit-tool [options] mars list [PATTERN]...
  graphit-tool [options] mars putformalnode FILE...
  graphit-tool [options] mars getformalnode [--out=DIR] NODEID...
  graphit-tool [options] mars delnode NODEID...

Switches:
  -h, --help         print help and exit

Options:
  -d, --debug        print debug messages
  -o DIR, --out=DIR  save node to <node_id>.xml in given directory
"""
import sys;sys.path.append("/vagrant/includes.zip")
import codecs
from gevent import monkey; monkey.patch_all()
import gevent
from graphit import *
from itertools import islice, chain
from docopt import docopt

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

def enclose(string, start="", end=None):
	end = start if not end else end
	return enclose + string + enclose

def double_quote(string):
	return enclose(string, "\"")

def escape(string):
	for char in ":": string = string.replace(char, "\\" + char)
	return string

if __name__ == '__main__':
	args = docopt(__doc__, version='graphit-tool 0.1')

	if args['--debug']:
		import logging
		import httplib as http_client
		http_client.HTTPConnection.debuglevel = 1
		logging.basicConfig()
		logging.getLogger().setLevel(logging.DEBUG)
		requests_log = logging.getLogger("requests.packages.urllib3")
		requests_log.setLevel(logging.DEBUG)
		requests_log.propagate = True
	#print >>sys.stderr, args

	session = GraphitSession('https://hiro.ubs.dev:8443')
	session.auth = WSO2AuthClientCredentials(
		'https://hiro.ubs.dev:9443',
		client = (
			'FKOGAQRNFe6snU6RYLCYl7qI0woa',
			'BHHy447ZU1gMrEJcumsQ2vY6p5sa'
		),
		verify=False)
	session.verify=False

	if args['mars'] and args['list']:
		q = ESQuery({"+ogit/_type":["ogit/Automation/MARSNode"]})
		q.add({"+ogit/_id":args['PATTERN']})
		for r in session.query(q, fields=['ogit/_id']):
			print >>sys.stdout, r['ogit/_id']
		sys.exit(0)

	if args['mars'] and args['getformalnode']:
		factory = EngineDataFactory(
			'/vagrant/MODEL_default.xsd',
			'ogit/Automation/MARSNode',
			MARSNode, session)
		for node in factory.from_graphit(args['NODEID']):
			if args['--out']:
				node.write_xml_file(directory=args['--out'], pretty=True)
			else:
				print >>sys.stdout, node.to_xml(pretty=True)
		sys.exit(0)

	if args['mars'] and args['putformalnode'] and args['FILE']:
		factory = EngineDataFactory(
			'/vagrant/MODEL_default.xsd',
			'ogit/Automation/MARSNode',
			MARSNode, session)
		for chunk in chunks(factory.from_xml_files(
				args['FILE'], pretty=True), 10):
			jobs = [gevent.spawn(node.push) for node in chunk]
			gevent.joinall(jobs)
