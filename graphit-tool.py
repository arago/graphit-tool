#!/usr/bin/env python2
"""graphit-tool

Usage:
  graphit-tool [options] mars list[--count] [PATTERN]...
  graphit-tool [options] mars put [--chunk-size=NUM] [--replace] FILE...
  graphit-tool [options] mars get [--out=DIR] NODEID...
  graphit-tool [options] mars del [--chunk-size=NUM] [--del-ci] NODEID...
  graphit-tool [options] mars sync NODEID...
  graphit-tool [options] mars sync (--count-unsynced|--list-unsynced)
  graphit-tool [options] token (info|get)
  graphit-tool [options] ci (count_orphans|cleanup_orphans)
  graphit-tool [options] ci create --attr=ATTR NODEID...
  graphit-tool [options] ci create --value=ATTR NODEID
  graphit-tool [options] issue getevent [--field=FIELD...] [--pretty] IID...
  graphit-tool [options] vertex get [--field=FIELD...] [--pretty] [--] OGITID...
  graphit-tool [options] vertex query [--count] [--list] [--field=FIELD...] [--pretty] [--] QUERY...
  graphit-tool [options] vertex del [--] OGITID...
  graphit-tool [options] vertex setattr --attr=ATTR --value=VALUE NODEID...
  graphit-tool [options] vertex delattr --attr=ATTR NODEID...

Switches:
  -o DIR, --out=DIR          save node to <node_id>.xml in given directory
  -f FIELD, --field=FIELD    Return only given fields
  -p, --pretty               Pretty print JSON data
  -c, --count                return the number of results, not the results themselves
  -C NUM, --chunk-size=NUM   Upload NUM MARS nodes in parallel
  -R, --replace              Replace existing nodes instead of updating them. Before 0.3.2, this
                             was the default behavior.
  -h, --help                 print help and exit

Options:
  -d, --debug                print debug messages
"""
import sys
from gevent import monkey; monkey.patch_all()
import codecs
import gevent, gevent.pool
from graphit import GraphitSession, WSO2Error, WSO2AuthClientCredentials, ESQuery, IDQuery, VerbQuery, GraphitError, chunks, XMLValidator, GraphitNodeError, MARSNode, GraphitNode, MARSNodeError
from docopt import docopt
from ConfigParser import ConfigParser
from requests.structures import CaseInsensitiveDict
import os
import re
import json

sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

if __name__ == '__main__':
	args = docopt(__doc__, version='graphit-tool 0.3.2')
	if not args['--chunk-size']: args['--chunk-size'] = 10

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
			if args['--count']:
				for r in session.query(q, fields=['ogit/_id'], count=args['--count']):
					print >>sys.stdout, r
				sys.exit(0)
			else:
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
				if args['--del-ci']:
					q2 = VerbQuery(node, "ogit/corresponds", ogit_types=['ogit/ConfigurationItem'])
					for item in session.query(q2, fields=['ogit/_id']):
						print >>sys.stderr, "Deleted corresponding ogit/ConfigurationItem {id}".format(id=item['ogit/_id'])
						GraphitNode(session, {'ogit/_id':item['ogit/_id'], 'ogit/_type':'ogit/ConfigurationItem'}).delete()
				MARSNode(session, {'ogit/_id':node,'ogit/_type':'ogit/Automation/MARSNode'}).delete()
				print >>sys.stderr, "Deleted {id}".format(id = node)
			except GraphitNodeError as e:
				print >>sys.stderr, e
			except GraphitError as e:
				print >>sys.stderr, "Failed to delete node {nodeid}: {e}".format(
					nodeid=node, e=e)
		try:
			size = int(args['--chunk-size'])
			if not size >= 1: raise IndexError("--chunk-size has to be >=1")
			if size > 9223372036854775808: raise IndexError("--chunk-size has to be <= 9223372036854775808")
		except ValueError:
			print >>sys.stderr, "--chunk-size has to be numeric"
			sys.exit(1)
		except IndexError as e:
			print >>sys.stderr, e
			sys.exit(1)
		pool = gevent.pool.Pool(size)
		for n in args['NODEID']:
			pool.spawn(delete_node, n)
		pool.join()
		sys.exit(0)

	if args['mars'] and args['put'] and args['FILE']:
		mars_validator = XMLValidator(config.get('mars', 'schema'))
		def upload_file(filename):
			try:
				mars_node = MARSNode.from_xmlfile(session, filename, mars_validator)
				mars_node.push(replace=args['--replace'])
				return (filename, mars_node.data["ogit/_id"])
			except MARSNodeError as e:
				print >>sys.stderr, e
			except GraphitError as e:
				print >>sys.stdout, e
		try:
			size = int(args['--chunk-size'])
			if not size >= 1: raise IndexError("--chunk-size has to be >=1")
			if size > 9223372036854775808: raise IndexError("--chunk-size has to be <= 9223372036854775808")
		except ValueError:
			print >>sys.stderr, "--chunk-size has to be numeric"
			sys.exit(1)
		except IndexError as e:
			print >>sys.stderr, e
			sys.exit(1)
		for filename, ogit_id in gevent.pool.Pool(size).imap_unordered(upload_file, args['FILE']):
			print >>sys.stdout, ogit_id + " (read from " + filename + ") successfully uploaded!"
		sys.exit(0)

	if args['mars'] and args['sync']:
		def sync_node(node):
			MARSNode(session, {
				'ogit/_id':node,
				'ogit/_type':'ogit/Automation/MARSNode',
				'ogit/Automation/deployStatus': None,
				'ogit/Automation/isDeployed': None }).update()
		try:
			size = int(args['--chunk-size'])
			if not size >= 1: raise IndexError("--chunk-size has to be >=1")
			if size > 9223372036854775808: raise IndexError("--chunk-size has to be <= 9223372036854775808")
		except ValueError:
			print >>sys.stderr, "--chunk-size has to be numeric"
			sys.exit(1)
		except IndexError as e:
			print >>sys.stderr, e
			sys.exit(1)
		try:
			if args['--count-unsynced']:
				q = ESQuery({"+ogit/_type":["ogit/Automation/MARSNode"], "+ogit/Automation/isDeployed":["false"]})
				for r in session.query(q, count=True):
					print r
				sys.exit(0)
			elif args['--list-unsynced']:
				q = ESQuery({"+ogit/_type":["ogit/Automation/MARSNode"], "+ogit/Automation/isDeployed":["false"]})
				for r in session.query(q, fields=['ogit/_id']):
					print >>sys.stdout, r['ogit/_id']
				sys.exit(0)
			else:
				pool=gevent.pool.Pool(size)
				for n in args['NODEID']:
					pool.spawn(sync_node, n)
				pool.join()
				sys.exit(0)
		except GraphitError as e:
			print >>sys.stderr, "Cannot trigger syncing of nodes: {err}".format(err=e)
			sys.exit(5)

	if args['token'] and args['info']:
		print >>sys.stdout, session.auth

	if args['token'] and args['get']:
		print >>sys.stdout, session.auth.token

	if args['ci'] and args['count_orphans']:
		q = ESQuery({"+ogit/_type":["ogit/ConfigurationItem"]})
		try:
			orphsum = 0
			def count_corresponds(node):
				q2 = VerbQuery(node['ogit/_id'], "ogit/corresponds")
				return len(list(session.query(q2, fields=['ogit/_id'])))
			for chunk in chunks(session.query(q, fields=['ogit/_id'])):
				jobs = [gevent.spawn(count_corresponds, n) for n in chunk]
				gevent.joinall(jobs)
				orphsum += sum([1 for job in jobs if job.value==0])
			print >>sys.stdout, orphsum
			sys.exit(0)
		except GraphitError as e:
			print >>sys.stderr, "Cannot list nodes: {err}".format(err=e)
			sys.exit(5)

	if args['ci'] and args['cleanup_orphans']:
		q = ESQuery({"+ogit/_type":["ogit/ConfigurationItem"]})
		try:
			def delete_if_orphan(node):
				q2 = VerbQuery(node['ogit/_id'], "ogit/corresponds")
				no_conn = len(list(session.query(q2, fields=['ogit/_id'])))
				if no_conn == 0:
					print >>sys.stdout, "{node} has {no} corresponding vertices, deleting ...".format(
						node=node['ogit/_id'], no=no_conn)
					GraphitNode(session, {'ogit/_id':node['ogit/_id'], 'ogit/_type':'ogit/ConfigurationItem'}).delete()
				else:
					print >>sys.stdout, "{node} has {no} corresponding vertices.".format(
						node=node['ogit/_id'], no=no_conn)
			pool=gevent.pool.Pool(10)
			for n in session.query(q, fields=['ogit/_id']):
				pool.spawn(delete_if_orphan, n)
			pool.join()
			sys.exit(0)
		except GraphitError as e:
			print >>sys.stderr, "Cannot list nodes: {err}".format(err=e)
			sys.exit(5)

	if args['ci'] and args['create'] and args['--attr'] and args['NODEID']:
		hostname_regex = re.compile('(?=^.{4,253}$)(^((?!-)[a-zA-Z0-9-]{0,62}[a-zA-Z0-9]\.)+[a-zA-Z]{2,63}$)')
		def create_missing_ci(mars_id):
			try:
				mars_node = MARSNode.from_graph(session, mars_id)
				#print mars_node.json(pretty_print=True)
			except GraphitError as e:
				if e.status == 404:
					print >>sys.stdout, "{id}: Failure, node does not exists.".format(id=mars_id)
				else:
					print >>sys.stdout, "{id}: Failure: {err}".format(id=mars_id, err=e)
				return
			if (args['--attr'] == '_SHORTNAME_'
				and mars_node.get_attr('/NodeType') != 'Machine'):
				print >>sys.stdout, mars_id + ": _SHORTNAME_ not applicable, works only with machine nodes."
				return
			elif (args['--attr'] == '_SHORTNAME_'
				  and mars_node.get_attr('/NodeType') == 'Machine'
				  and not hostname_regex.match(mars_node.get_attr('/NodeName'))):
				print >>sys.stdout, "{id}: _SHORTNAME_ not applicable, '{nn}' is not a valid FQDN'".format(
					id=mars_id,
					nn=mars_node.get_attr('/NodeName'))
				return
			elif (args['--attr'] == '_SHORTNAME_'
				  and mars_node.get_attr('/NodeType') == 'Machine'
				  and hostname_regex.match(mars_node.get_attr('/NodeName'))):
				new_id = mars_node.get_attr('/NodeName').split('.', 1)[0]
			elif mars_node.get_attr(args['--attr']):
				try:
					new_id = json.loads(mars_node.get_attr(args['--attr']))['items'][0][0]
				except ValueError:
					new_id = mars_node.get_attr(args['--attr'])
			else:
				print >>sys.stdout, "{id}: Failure, node doesn't have an attribute '{attr}'".format(
					id=mars_id, attr=args['--attr'])
				return
			q = ESQuery()
			q.add({'+ogit/id':[new_id]})
			q.add({'+ogit/_type':['ogit/ConfigurationItem']})
			if len(list(session.query(q, fields=['ogit/_id']))) > 0:
				print "{id}: ConfigurationItem with ogit/id '{c}' already exists".format(
					id=mars_id, c=new_id)
				return
			data =   {
				"ogit/_owner" : mars_node.get_attr('ogit/_owner'),
				"ogit/_type" : "ogit/ConfigurationItem",
				"ogit/ciType" : mars_node.get_attr('/NodeType'),
				"ogit/id" : new_id,
				"ogit/name" : new_id
			}
			try:
				ci_node = GraphitNode(session, data)
				#print(ci_node.json(pretty_print=True))
				ci_node.push()
				mars_node.connect('ogit/corresponds', ci_node)
				print >>sys.stdout, mars_id + ": ConfigurationItem created: " + ci_node.data['ogit/_id']
				return
			except GraphitError as e:
				print >>sys.stderr, e
		try:
			size = int(args['--chunk-size'])
			if not size >= 1: raise IndexError("--chunk-size has to be >=1")
			if size > 9223372036854775808: raise IndexError("--chunk-size has to be <= 9223372036854775808")
		except ValueError:
			print >>sys.stderr, "--chunk-size has to be numeric"
			sys.exit(1)
		except IndexError as e:
			print >>sys.stderr, e
			sys.exit(1)
		except GraphitError as e:
			print >>sys.stderr, e
			sys.exit(1)
		list(gevent.pool.Pool(size).imap_unordered(create_missing_ci, args['NODEID']))
		sys.exit(0)

	if args['ci'] and args['create'] and args['--value'] and args['NODEID']:
		def create_missing_ci(mars_id):
			try:
				mars_node = MARSNode.from_graph(session, mars_id)
			except GraphitError as e:
				if e.status == 404:
					print >>sys.stdout, "{id}: Failure, node does not exists.".format(id=mars_id)
				else:
					print >>sys.stdout, "{id}: Failure: {err}".format(id=mars_id, err=e)
				return
			new_id = args['--value']
			q = ESQuery()
			q.add({'+ogit/id':[new_id]})
			q.add({'+ogit/_type':['ogit/ConfigurationItem']})
			if len(list(session.query(q, fields=['ogit/_id']))) > 0:
				print "{id}: ConfigurationItem with ogit/id '{c}' already exists".format(
					id=mars_id, c=new_id)
				return
			data =   {
				"ogit/_owner" : mars_node.get_attr('ogit/_owner'),
				"ogit/_type" : "ogit/ConfigurationItem",
				"ogit/ciType" : mars_node.get_attr('/NodeType'),
				"ogit/id" : new_id,
				"ogit/name" : new_id
			}
			try:
				ci_node = GraphitNode(session, data)
				ci_node.push()
				mars_node.connect('ogit/corresponds', ci_node)
				print >>sys.stdout, mars_id + ": ConfigurationItem created: " + ci_node.data['ogit/_id']
				return
			except GraphitError as e:
				print >>sys.stderr, e
		try:
			size = int(args['--chunk-size'])
			if not size >= 1: raise IndexError("--chunk-size has to be >=1")
			if size > 9223372036854775808: raise IndexError("--chunk-size has to be <= 9223372036854775808")
		except ValueError:
			print >>sys.stderr, "--chunk-size has to be numeric"
			sys.exit(1)
		except IndexError as e:
			print >>sys.stderr, e
			sys.exit(1)
		except GraphitError as e:
			print >>sys.stderr, e
			sys.exit(1)
		create_missing_ci(args['NODEID'][0])
		sys.exit(0)


	if args['vertex'] and args['get'] and args['OGITID']:
		q = IDQuery(args['OGITID'])
		try:
			if args['--count']:
				for r in session.query(q, fields=['ogit/_id'], count=args['--count']):
					print >>sys.stdout, r
				sys.exit(0)
			else:
				if args['--list']:
					args['--field'] = ["ogit/_id"]
				for r in session.query(q, fields=args['--field']):
					print "holla"
					if args['--list']:
						print >>sys.stdout, GraphitNode(session,r).data['ogit/_id']
					else:
						print >>sys.stdout, GraphitNode(session,r).json(pretty_print=args['--pretty'])
				sys.exit(0)
		except GraphitError as e:
			print >>sys.stderr, "Cannot list nodes: {err}".format(err=e)
			sys.exit(5)
	if args['vertex'] and args['del']:
		def delete_node(node):
			try:
				GraphitNode(session, {'ogit/_id':node}).delete()
				print >>sys.stderr, "Deleted {id}".format(id = node)
			except GraphitNodeError as e:
				print >>sys.stderr, e
			except GraphitError as e:
				print >>sys.stderr, "Failed to delete node {nodeid}: {e}".format(
					nodeid=node, e=e)
		try:
			size = int(args['--chunk-size'])
			if not size >= 1: raise IndexError("--chunk-size has to be >=1")
			if size > 9223372036854775808: raise IndexError("--chunk-size has to be <= 9223372036854775808")
		except ValueError:
			print >>sys.stderr, "--chunk-size has to be numeric"
			sys.exit(1)
		except IndexError as e:
			print >>sys.stderr, e
			sys.exit(1)
		pool = gevent.pool.Pool(size)
		for n in args['OGITID']:
			pool.spawn(delete_node, n)
		pool.join()
		sys.exit(0)

	if args['vertex'] and args['query']:
		q = ESQuery()
		for cond in args['QUERY']:
			arr=cond.split(':', 1)
			q.add({arr[0]:arr[1]})
		try:
			if args['--count']:
				for r in session.query(q, fields=['ogit/_id'], count=args['--count']):
					print >>sys.stdout, r
				sys.exit(0)
			else:
				if args['--list']:
					args['--field'] = ["ogit/_id"]
				for r in session.query(q, fields=args['--field']):
					if args['--list']:
						print >>sys.stdout, GraphitNode(session,r).data['ogit/_id']
					else:
						print >>sys.stdout, GraphitNode(session,r).json(pretty_print=args['--pretty'])
				sys.exit(0)
		except GraphitError as e:
			print >>sys.stderr, "Cannot list nodes: {err}".format(err=e)
			sys.exit(5)

	if args['vertex'] and args['setattr'] and args['--attr'] and args['NODEID']:
		def set_vertex_attr(node):
			data = {
				'ogit/_id':node['ogit/_id'],
				'ogit/_type':node['ogit/_type'],
				args['--attr']:args['--value']
			}
			GraphitNode(session, data).update()
			print >>sys.stdout, "Attribute '{att}' of node '{id}' set to '{val}'".format(
				att=args['--attr'],
				id=node['ogit/_id'],
				val=args['--value'])
		try:
			size = int(args['--chunk-size'])
			if not size >= 1: raise IndexError("--chunk-size has to be >=1")
			if size > 9223372036854775808: raise IndexError("--chunk-size has to be <= 9223372036854775808")
		except ValueError:
			print >>sys.stderr, "--chunk-size has to be numeric"
			sys.exit(1)
		except IndexError as e:
			print >>sys.stderr, e
			sys.exit(1)
		pool = gevent.pool.Pool(size)
		q = IDQuery(args['NODEID'])
		for r in session.query(q, fields=['ogit/_id','ogit/_type']):
			pool.spawn(set_vertex_attr, r)
		pool.join()
		sys.exit(0)

	if args['vertex'] and args['delattr'] and args['--attr'] and args['NODEID']:
		def set_vertex_attr(node):
			data = {
				'ogit/_id':node['ogit/_id'],
				'ogit/_type':node['ogit/_type'],
				args['--attr']:None
			}
			GraphitNode(session, data).update()
			print >>sys.stdout, "Attribute '{att}' of node '{id}' deleted".format(
				att=args['--attr'],
				id=node['ogit/_id'])
		try:
			size = int(args['--chunk-size'])
			if not size >= 1: raise IndexError("--chunk-size has to be >=1")
			if size > 9223372036854775808: raise IndexError("--chunk-size has to be <= 9223372036854775808")
		except ValueError:
			print >>sys.stderr, "--chunk-size has to be numeric"
			sys.exit(1)
		except IndexError as e:
			print >>sys.stderr, e
			sys.exit(1)
		pool = gevent.pool.Pool(size)
		q = IDQuery(args['NODEID'])
		for r in session.query(q, fields=['ogit/_id','ogit/_type']):
			pool.spawn(set_vertex_attr, r)
		pool.join()
		sys.exit(0)

	if args['issue'] and args['getevent'] and args['IID']:
		q = IDQuery(args['IID'])
		try:
			def print_event(node):
				q2 = VerbQuery(node['ogit/_id'], "ogit/corresponds")
				for item in session.query(q2, fields=args['--field']):
					print >>sys.stdout, GraphitNode(session,item).json(pretty_print=args['--pretty'])
			for chunk in chunks(session.query(q, fields=['ogit/_id'])):
				jobs = [gevent.spawn(print_event, n) for n in chunk]
				gevent.joinall(jobs)
			sys.exit(0)
		except GraphitError as e:
			print >>sys.stderr, "Cannot list nodes: {err}".format(err=e)
			sys.exit(5)
