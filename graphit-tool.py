#!/usr/bin/env python2
"""graphit-tool

Usage:
  graphit-tool [options] mars list [PATTERN]...
  graphit-tool [options] mars put FILE...
  graphit-tool [options] mars get [--out=DIR] NODEID...
  graphit-tool [options] mars del NODEID...
  graphit-tool [options] token (info|get)

Switches:
  -o DIR, --out=DIR  save node to <node_id>.xml in given directory
  -h, --help         print help and exit

Options:
  -d, --debug        print debug messages
"""
import sys
from gevent import monkey; monkey.patch_all()
import codecs
import gevent
from graphit import GraphitSession, WSO2Error, WSO2AuthClientCredentials, ESQuery, IDQuery, GraphitError, chunks, XMLValidator, GraphitNodeError, MARSNode, MARSNodeError
from docopt import docopt
from ConfigParser import ConfigParser
from requests.structures import CaseInsensitiveDict
import os

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

if __name__ == '__main__':
	args = docopt(__doc__, version='graphit-tool 0.1')

	config = ConfigParser(dict_type=CaseInsensitiveDict)
	config.read(['/etc/graphit-tool.conf', os.path.expanduser('~/.graphit-tool.conf')])

	session = GraphitSession(config.get('graphit', 'url'))
	try:
		wso2_verify = config.getboolean('wso2', 'verifycert')
	except ValueError:
		wso2_verify = config.get('wso2', 'verifycert')
	try:
		session.auth = WSO2AuthClientCredentials(
			config.get('wso2', 'url'),
			client = (
				config.get('wso2', 'clientid'),
				config.get('wso2', 'clientsecret')
			),
			verify=wso2_verify)
	except WSO2Error as e:
		print >>sys.stderr, e
		sys.exit(10)
	try:
		session.verify=config.getboolean('graphit', 'verifycert')
	except ValueError:
		session.verify=config.get('graphit', 'verifycert')

	if args['mars'] and args['list']:
		q = ESQuery({"+ogit/_type":["ogit/Automation/MARSNode"]})
		if args['PATTERN']: q.add({"+ogit/_id":args['PATTERN']})
		try:
			for r in session.query(q, fields=['ogit/_id']):
				print >>sys.stdout, r['ogit/_id']
			sys.exit(0)
		except GraphitError as e:
			print >>sys.stderr, "Cannot list nodes: {err}".format(err=e)
			sys.exit(5)

	if args['mars'] and args['get']:
		def resumeable(gen):
			while True:
				try: yield next(gen)
				except GraphitNodeError as e: print >>sys.stderr, e
				except StopIteration: raise
		q = IDQuery(args['NODEID'])
		try:
			for node in resumeable(session.query(q, fields=[
					'ogit/_id', 'ogit/_type', 'ogit/Automation/marsNodeFormalRepresentation'])):
				if args['--out']:
					with open("{directory}/{basename}.{ext}".format(
							directory=args['--out'],
							basename=node['ogit/_id'],
							ext='xml'), 'w', -1) as f:
						MARSNode(None, node).print_node(f)
				else:
					MARSNode(None, node).print_node(sys.stdout)
			sys.exit(0)
		except GraphitError as e:
			print >>sys.stderr, "Cannot get nodes: {err}".format(err=e)
			sys.exit(5)

	if args['mars'] and args['del']:
		def delete_node(node):
			try:
				MARSNode(session, {'ogit/_id':node,'ogit/_type':'ogit/Automation/MARSNode'}).delete()
				print >>sys.stderr, "Deleted {id}".format(id = node)
			except GraphitNodeError as e:
				print >>sys.stderr, e
		for chunk in chunks(args['NODEID']):
			jobs = [gevent.spawn(delete_node, n) for n in chunk]
			gevent.joinall(jobs)
		sys.exit(0)

	if args['mars'] and args['put'] and args['FILE']:
		mars_validator = XMLValidator(config.get('mars', 'schema'))
		def upload_file(filename):
			try:
				mars_node = MARSNode.from_xmlfile(session, filename, mars_validator)
				mars_node.push()
				print >>sys.stdout, mars_node.ogit_id + " successfully uploaded!"
			except MARSNodeError as e:
				print >>sys.stderr, e
		for chunk in chunks(args['FILE']):
			jobs = [gevent.spawn(upload_file, f) for f in chunk]
			gevent.joinall(jobs)
		sys.exit(0)

	if args['token'] and args['info']:
		print >>sys.stdout, session.auth

	if args['token'] and args['get']:
		print >>sys.stdout, session.auth.token
