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
		if args['PATTERN']: q.add({"+ogit/_id":args['PATTERN']})
		for r in session.query(q, fields=['ogit/_id']):
			print >>sys.stdout, r['ogit/_id']
		sys.exit(0)

	if args['mars'] and args['getformalnode']:
		q = IDQuery(args['NODEID'])
		for node in session.query(q, fields=[
				'ogit/_id', 'ogit/Automation/marsNodeFormalRepresentation', 'ogit/_type']):
			try:
				if args['--out']:
					with open("{directory}/{basename}.{ext}".format(
							directory=args['--out'], basename=node['ogit/_id'], ext='xml'
					), 'w', -1) as f:
							  print >>f, prettify_xml(node['ogit/Automation/marsNodeFormalRepresentation'])
				else:
					print >>sys.stdout, prettify_xml(node['ogit/Automation/marsNodeFormalRepresentation'])
			except KeyError as e:
				if 'error' in node:
					print >>sys.stderr, "ERROR: Node '{nd}' {err}".format(
						nd=node['error']['ogit/_id'], err=node['error']['message'])
				else:
					print >>sys.stderr, "ERROR: Node {nd} is missing 'ogit/Automation/marsNodeFormalRepresentation' attribute! Maybe it's not a MARS node?".format(nd=node['ogit/_id'])
		sys.exit(0)

	if args['mars'] and args['delnode']:
		def delete_node(node):
			try:
				session.delete('/' + node)
				print >>sys.stderr, "Deleted {id}".format(id = node)
			except GraphitError as e:
				if e.status == 404:
					print >>sys.stderr, "Cannot delete node {nd}: Not found!".format(nd=node)
				elif e.status == 409:
					print >>sys.stderr, "Cannot delete node {nd}: Already deleted!".format(nd=node)
				else:
					print >>sys.stderr, "Cannot delete node {nd}: {err}".format(nd=node, err=e)
		for chunk in chunks(args['NODEID']):
			jobs = [gevent.spawn(delete_node, n) for n in chunk]
			gevent.joinall(jobs)
		sys.exit(0)

	if args['mars'] and args['putformalnode'] and args['FILE']:
		mars_validator = XMLValidator('/vagrant/MODEL_default.xsd')
		def upload_file(filename):
			try:
				xml_doc = et.parse(filename).getroot()
				mars_validator.validate(xml_doc)
				ogit_id = xml_doc.attrib['ID']
				data = {
					'ogit/Automation/marsNodeFormalRepresentation':et.tostring(xml_doc),
					'ogit/_owner': xml_doc.attrib['CustomerID'],
					'ogit/_id': ogit_id,
					'ogit/_type':'ogit/Automation/MARSNode'
				}
				q = ESQuery({'ogit/_id':ogit_id})
				try:
					next(session.query(q))
				except StopIteration:
					session.create('ogit/Automation/MARSNode', data)
				else:
					session.replace('/' + ogit_id, data)
				print ogit_id + " successfully uploaded!"
			except XMLValidateError:
				print >>sys.stderr, "ERROR: {f} does not contain a valid MARS node".format(f=filename)
			except et.XMLSyntaxError:
				print >>sys.stderr, "ERROR: {f} does not contain valid XML".format(f=filename)
		mars_validator = XMLValidator('/vagrant/MODEL_default.xsd')
		for chunk in chunks(args['FILE']):
			jobs = [gevent.spawn(upload_file, f) for f in chunk]
			gevent.joinall(jobs)
