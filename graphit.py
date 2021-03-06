import gevent
import requests
import requests.auth
import time
import json
import re
#import hashlib
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from lxml import etree as et
from itertools import islice, chain
from urllib import quote_plus
import jsonschema

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def chunks(iterable, size=10):
	iterator = iter(iterable)
	for first in iterator: yield chain([first], islice(iterator, size - 1))

class GraphitSession(requests.Session):

	def __init__(self, baseurl, *args, **kwargs):
		self._baseurl=baseurl
		self._headers = {
			"User-Agent": "PyGraphIT/1.0",
			"charset": "UTF-8"
		}
		super(GraphitSession, self).__init__(*args, **kwargs)

	def request(self, method, url, params=None, data=None):
		try:
			headers = self._headers
			headers["Accept"] = "application/json"
			r = super(GraphitSession, self).request(
				method, self._baseurl + url, headers=self._headers,
				params=params, data=json.dumps(data))
			r.raise_for_status()
		except requests.exceptions.HTTPError as e:
			try:
				error_message = r.json()['error']['message']
			except ValueError:
				error_message = None
			except KeyError:
				error_message = None
			raise GraphitError(self, r.status_code, e, error_message)
		except requests.exceptions.ConnectionError as e:
			raise GraphitError(self, 0, e)
		return r.json()

	def get(self, resource):
		return self.request('GET', resource)
	def update(self, resource, data, params=None):
		return self.request('POST', resource, data=data, params=params)
	def replace(self, resource, data, params=None):
		return self.request('PUT', resource, data=data, params=params)
	def delete(self, resource):
		return self.request('DELETE', resource)
	def connect(self, ogit_type, in_id, out_id):
		return self.request(
			'POST', '/connect/' + quote_plus(ogit_type),
			data={'in':in_id, 'out':out_id})
	def create(self, ogit_type, data):
		return self.request(
			'POST', '/new/' + quote_plus(ogit_type), data=data)
	def query(self, query, limit=-1, offset=0, fields=None, count=False,
			  concurrent=10, chunksize=100):
		return QueryResult(self, query, count=count,
					 limit=limit, offset=offset, fields=fields,
					 concurrent=concurrent, chunksize=chunksize)

	def __str__(self):
		return 'GraphIT at {url}'.format(url=self._baseurl)

class WSO2AuthBase(requests.auth.AuthBase):
	def __init__(self, baseurl, verify=True):
		self._baseurl = baseurl
		self._verify = verify
		self.get_token()

	def get_token(self, auth=None, headers=None, post_data=None):
		try:
			r = requests.post(
				"{baseurl}/oauth2/token".format(baseurl=self._baseurl),
				auth=auth, data=post_data, headers=headers,
				verify=self._verify)
			r.raise_for_status()
		except requests.exceptions.HTTPError as e:
			if r.status_code == 401:
				raise WSO2Error("Could not get an access token from WSO2, check client credentials!")
			else:
				raise WSO2Error(e.message)
		except requests.exceptions.ConnectionError as e:
			raise WSO2Error("Could not connect to WSO2: " + str(e))
		self._token = Token(r.json())

	def renew_token(self, *args, **kwargs):
		self.get_token(*args, **kwargs)

	def __str__(self):
		str = "Token {token} expires in {exp} seconds."
		return str.format(
			token=self._token.access_token,
			exp=int(self._token.expires_in))

	@property
	def token(self):
		return self._token.access_token

	def __call__(self, r):
		#self.logger.debug("Inserting OAuth token into request header.")
		r.headers['_TOKEN'] = self._token.access_token
		return r

class WSO2AuthClientCredentials(WSO2AuthBase):
	def __init__(self, baseurl=None, client=None, verify=True):
		self._client_id, self._client_secret = client
		super(WSO2AuthClientCredentials, self).__init__(
			baseurl,
			verify=verify
		)

	def get_token(self):
		super(WSO2AuthClientCredentials, self).get_token(
			auth = requests.auth.HTTPBasicAuth(
				self._client_id, self._client_secret),
			headers = {
				"User-Agent": "PyGraphIT/1.0",
				"Content-Type": "application/x-www-form-urlencoded",
				"charset": "UTF-8"
			},
			post_data = {"grant_type": "client_credentials"},
		)

class Token(object):
	def __init__(self, t):
		self.access_token = t['access_token']
		self.expires_at = t['expires_in'] + time.time()
		if 'refresh_token' in t:
			self.refresh_token = t['refresh_token']

	def __str__(self):
		return self.access_token

	@property
	def expires_in(self):
		return int(self.expires_at - time.time())

class GraphitError(Exception):
	"""Error when talking to GraphIT"""
	def __init__(self, session, status, error, message=None):
		self.session=session
		self.status=status
		self.error = error
		self.message=message

	def __str__(self):
		if self.message:
			return "{sess} returned an error: {err}: {msg}".format(
				sess=self.session,
				err=self.error,
				msg=self.message)
		else:
			return "{sess} returned an error: {err}".format(
				sess=self.session,
				err=self.error)

class WSO2Error(Exception):
	"""Error when talking to GraphIT"""
	def __init__(self, message):
		self.message=message

	def __str__(self):
		return self.message

class EESQuery(object):
	def __init__(self, children=None, operation=''):
		self._op=operation
		self._ch=children if hasattr(children, '__iter__') else [children] if children else []

	@property
	def query_type(self): return "vertices"

	def append(self, child):
		if hasattr(child, '__iter__'):
			self._ch.extend(child)
		else:
			self._ch.append(child)

	def clear(self):
		self._ch = []

	def __str__(self):
		def escape_fieldname(string):
			string = [('\\' + c if c in "\\/+-~=\"<>!(){}[]^:&|*?"
					   else c)
					  for c in string]
			if string[0] in ['\\+','\\-']:
				string[0] = string[0][1:]
			return "".join(string)
		def escape_term(string):
			if string[0] == "/" and string[-1] == "/":
				return string[0] + escape_term(string[1:-1]) + string[-1]
			elif string[0] == "[" and string[-1] == "]":
				return string[0] + escape_term(string[1:-1]) + string[-1]
			elif string[0] == "{" and string[-1] == "}":
				return string[0] + escape_term(string[1:-1]) + string[-1]
			elif string[0] == "(" and string[-1] == ")":
				return string[0] + escape_term(string[1:-1]) + string[-1]
			elif string[0] in [">","<"]:
				return string[0] + escape_term(string[1:])
			else:
				return "".join([('\\' + c if c in "\\/+-~=<>!(){}[]^:&|"
						   else c)
						  for c in string])

		def escape_es(string):
			if string in ['AND', 'OR', 'NOT']:
				return string
			field, cond = tuple(string.split(':',1))
			return "{field}:{cond}".format(
				field=escape_fieldname(field),
				cond=escape_term(cond)
			)

		if self._op in ["AND", "OR", '']:
			term = " {op} ".format(op=self._op).join(
				[str(child) if isinstance(child, EESQuery) else escape_es(child) for child in self._ch]
			)
			return "( " + term + " )" if len(self._ch)>=2 else term
		elif self._op == "NOT" and len(self._ch) == 1:
			return "{op} {term}".format(op=self._op, term=str(self._ch[0]))
		else:
			raise NotImplementedError

class ESQuery(object):
	def __init__(self, conditions={}):
		self.conditions = {}
		self.add(conditions)

	@property
	def query_type(self): return "vertices"

	def add(self, conditions):
		for key, value in conditions.items():
			if type(value) is list:
				self.conditions.setdefault(key, []).extend(value)
			elif type(value) is str:
				self.conditions.setdefault(key, []).append(value)
			else: raise TypeError

	def __str__(self):
		def escape_fieldname(string):
			string = [('\\' + c if c in "\\/+-~=\"<>!(){}[]^:&|*?"
					   else c)
					  for c in string]
			if string[0] in ['\\+','\\-']:
				string[0] = string[0][1:]
			return "".join(string)
		def escape_term(string):
			if string[0] == "/" and string[-1] == "/":
				return "".join([('\\' + c if c in "-~=<>!:&"
						   else c)
						  for c in string])
			elif string[0] in ["[","{"] and string[-1] in ["]","}"]:
				return "".join([('\\' + c if c in "\\/+~=<>!()^:&|"
						   else c)
						  for c in string])
			elif string[0] in [">","<"]:
				return "".join([('\\' + c if c in "\\/+-~!(){}[]^:&|"
						   else c)
						  for c in string])
			else:
				return "".join([('\\' + c if c in "\\/+-~=<>!(){}[]^:&|"
						   else c)
						  for c in string])

		def join_set(lst):
			return "(" + " AND ".join([escape_term(it) for it in lst]) + ")"
		return " ".join(
			["{key}:{val}".format(
				key=escape_fieldname(key),
				val=join_set(val)
			)
			 for key, val
			 in self.conditions.items()]
		)

class IDQuery(object):
	def __init__(self, node_ids):
		self.node_ids=[]
		self.add(node_ids)

	@property
	def query_type(self): return "ids"

	def add(self, node_ids):
		if type(node_ids) is list:
			self.node_ids.extend(node_ids)
		elif type(node_ids) is str:
			self.node_ids.append(node_ids)
		else: raise TypeError

	def __str__(self):
		return ",".join(self.node_ids)



class VerbQuery(object):
	def __init__(self, node_id, verb, ogit_types=[]):
		self.node_id = node_id
		self.verb = verb
		self.ogit_types=ogit_types

	def __str__(self):
		return ""

class IDNotFoundError(Exception):
	"""Error when retrieving results"""
	def __init__(self, ID):
		self.message="Node {ID} not found!".format(ID=ID)
		self.ID = ID

	def __str__(self):
		return self.message

def QueryResult(graph, query, count=False, limit=-1, offset=0, fields=None, concurrent=10, chunksize=10):
	def get_values(ogit_ids):
		data = {"query":",".join(ogit_ids)}
		if fields: data['fields'] = ', '.join(fields)
		return graph.request(
			'POST',
			'/query/' + 'ids',
			data=data
		)['items']

	if type(query) is IDQuery:
		if count:
			yield len(query.node_ids)
			raise StopIteration()
		result_ids = query.node_ids
	elif type(query) is ESQuery or type(query) is EESQuery:
		if count:
			yield int(graph.request(
			'POST', '/query/' + query.query_type,
			data={
				"query":str(query),
				"fields":'ogit/_id',
				"limit":limit,
				"count":True,
				"offset":offset
			})['items'][0])
			raise StopIteration()
		result_ids = (i['ogit/_id'] for i in graph.request(
			'POST', '/query/' + query.query_type,
			data={
				"query":str(query),
				"fields":'ogit/_id',
				"limit":limit,
				"offset":offset
			})['items'] if 'ogit/_id' in i)
	elif type(query) is VerbQuery:
		test = graph.get('/' + query.node_id + '/' + quote_plus(query.verb) + '/')['items']
		if query.ogit_types:
			result_ids = (i['ogit/_id'] for i in test if i['ogit/_type'] in query.ogit_types)
		else:
			result_ids = (i['ogit/_id'] for i in test)
	else: raise NotImplementedError

	if fields == ['ogit/_id']:
		for res in result_ids:
			yield {'ogit/_id':res}

	for curr_slice in chunks(result_ids, chunksize*concurrent):
		jobs = [gevent.spawn(get_values, list(items)) for items in chunks(curr_slice, chunksize)]
		gevent.joinall(jobs)
		for job in jobs:
			for item in job.value:
				if 'error' in item and item['error']['code'] == 404:
					raise GraphitNodeError(
						"Node '{nd}' not found!".format(
							nd=item['error']['ogit/_id']))
				yield item

class XMLValidateError(Exception):
	"""Error when retrieving results"""
	def __init__(self):
		self.message="XML invalid!"

	def __str__(self):
		return self.message

class XMLValidator(object):
	def __init__(self, xsdfile):
		xml_schema_doc = et.parse(xsdfile)
		self.xml_schema = et.XMLSchema(xml_schema_doc)
	def validate(self, xml_doc):
		if self.xml_schema.validate(xml_doc):
			return True
		raise XMLValidateError()

class JSONValidateError(Exception):
	def __init__(self, msg=None):
		self.message=msg if msg else "JSON invalid!"

	def __str__(self):
		return self.message

class JSONValidator(object):
	def __init__(self, schemafile):
		try:
			self.json_schema = json.load(schemafile)
		except Exception as e:
			raise
	def validate(self, doc):
		try:
			jsonschema.validate(doc, self.json_schema)
			return True
		except jsonschema.ValidationError as e:
			raise JSONValidateError(e)

def prettify_xml(string):
	p = et.XMLParser(remove_blank_text=True)
	return et.tostring(et.fromstring(string, p), pretty_print=True)

class GraphitNodeError(Exception):
	"""Error when retrieving results"""
	def __init__(self, message):
		self.message=message

	def __str__(self):
		return self.message

class GraphitNode(object):
	def __init__(self, session, data):
		self.data = data
		self.session=session
		self.owner_error_regex = re.compile(r'owner ([a-z0-9\-._]+ )?does not exist')

	@classmethod
	def from_graph(cls, session, ogit_id):
		data = session.get('/' + quote_plus(ogit_id))
		return cls(session, data)

	def create_owner(self, owner=None):
		if not owner: owner=self.data['ogit/_owner']
		GraphitNode(self.session, {
					"ogit/_custom-id" : self.data['ogit/_owner'],
					"ogit/_id" : self.data['ogit/_owner'],
					"ogit/_owner" : self.data['ogit/_owner'],
					"ogit/_type" : "ogit/Organization",
					"ogit/description" : "created by MARS upload",
					"ogit/name" : self.data['ogit/_owner']
				}).push()

	def get_attr(self, attr):
		if attr in self.data:
			return self.data[attr]

	def set_attr(self, attr, val):
		self.data[attr]=val
		self.update()

	def rem_attr(self, attr):
		self.set_attr(attr, None)

	def connect(self, ogit_type, node):
		self.session.connect(ogit_type, self.get_attr('ogit/_id'), node.get_attr('ogit/_id'))

	def create(self):
		try:
			self.data = self.session.create(self.data['ogit/_type'], self.data)
		except GraphitError as e:
			if e.status == 400 and self.owner_error_regex.match(e.message):
				self.create_owner()
				self.create()
			else: raise

	def update(self):
		self.session.update('/' + quote_plus(self.data["ogit/_id"]), self.data)

	def pull(self):
		self.data = self.session.get('/' + quote_plus(self.data['ogit/_id']))

	def push(self, replace=True):
		if 'ogit/_id' in self.data:
			try:
				self.session.update(
					'/' + quote_plus(self.data["ogit/_id"]),
					self.data,
					params={
						'createIfNotExists':'true',
						'ogit/_type':self.data['ogit/_type']
					}
				)
			except GraphitError as e:
				if e.status == 400 and self.owner_error_regex.match(e.message):
					self.create_owner()
					self.push()
				else: raise
		else:
			self.create()

	def delete(self):
		try:
			self.session.delete('/' + self.data["ogit/_id"])
		except GraphitError as e:
			if e.status == 404:
				raise GraphitNodeError("Cannot delete node '{nd}': Not found!".format(nd=self.data["ogit/_id"]))
			elif e.status == 409:
				raise GraphitNodeError("Cannot delete node '{nd}': Already deleted!".format(nd=self.data["ogit/_id"]))
			else:
				raise GraphitNodeError("Cannot delete node '{nd}': {err}".format(nd=self.data["ogit/_id"], err=e))

	def json(self, pretty_print=False):
		if pretty_print:
			return json.dumps(self.data,sort_keys=True, indent=4, separators=(',', ': '))
		else:
			return json.dumps(self.data)

class MARSNodeError(Exception):
	"""Error when retrieving results"""
	def __init__(self, message):
		self.message=message

	def __str__(self):
		return self.message

class MARSNode(GraphitNode):
	@classmethod
	def from_xmlfile(cls, session, filename, validator=None):
		try:
			xml_doc = et.parse(filename).getroot()
			if validator:
				validator.validate(xml_doc)
			ogit_id = xml_doc.attrib['ID']
			ogit_name = xml_doc.attrib['NodeName']
			ogit_automation_marsnodetype = xml_doc.attrib['NodeType']
			#ogitid = hashlib.md5(ogit_id).hexdigest()
			data = {
				'ogit/Automation/marsNodeFormalRepresentation':et.tostring(xml_doc),
				'ogit/_owner': xml_doc.attrib['CustomerID'],
				'ogit/_id': ogit_id,
				'ogit/_type':'ogit/Automation/MARSNode',
				'ogit/name':ogit_name,
				'ogit/Automation/marsNodeType': ogit_automation_marsnodetype,
				'ogit/id':ogit_name
			}
		except XMLValidateError:
			raise MARSNodeError("ERROR: {f} does not contain a valid MARS node".format(f=filename))
		except et.XMLSyntaxError:
			raise MARSNodeError("ERROR: {f} does not contain valid XML".format(f=filename))
		return cls(session, data)

	@classmethod
	def from_jsonfile(cls, session, filename, mars_validator=None, json_validator=None):
		try:
			with open(filename) as jsonfile:
				try:
					doc=json.load(jsonfile)
				except ValueError:
					raise MARSNodeError("ERROR: {f} does not contain valid JSON".format(f=filename))
			if json_validator:
				json_validator.validate(doc)
			xml_doc = et.fromstring(doc['ogit/Automation/marsNodeFormalRepresentation'])
			if mars_validator:
				mars_validator.validate(xml_doc)
			ogit_id = xml_doc.attrib['ID']
			ogit_name = xml_doc.attrib['NodeName']
			ogit_automation_marsnodetype = xml_doc.attrib['NodeType']
			data = {
				'ogit/Automation/marsNodeFormalRepresentation':et.tostring(xml_doc),
				'ogit/_owner': xml_doc.attrib['CustomerID'],
				'ogit/_id': ogit_id,
				'ogit/_type':'ogit/Automation/MARSNode',
				'ogit/name':doc['ogit/name'] if 'ogit/name' in doc else ogit_name,
				'ogit/Automation/marsNodeType': ogit_automation_marsnodetype,
				'ogit/id':doc['ogit/id'] if 'ogit/id' in doc else ogit_name
			}
		except XMLValidateError:
			raise MARSNodeError("ERROR: 'ogit/Automation/marsNodeFormalRepresentation' in {f} does not contain a valid MARS node".format(f=filename))
		except JSONValidateError:
			raise MARSNodeError("ERROR: {f} does not contain valid ogit/Automation/MARSNode JSON".format(f=filename))
		return cls(session, data)

	def print_node(self, stream):
		try:
			print >>stream, prettify_xml(self.data['ogit/Automation/marsNodeFormalRepresentation'])
		except KeyError as e:
			if 'error' in self.data:
				raise MARSNodeError("ERROR: Node '{nd}' {err}".format(
					nd=self.data['error']['ogit/_id'], err=self.data['error']['message']))
			else:
				raise MARSNodeError("ERROR: Node {nd} is missing 'ogit/Automation/marsNodeFormalRepresentation' attribute! Maybe it's not a MARS node?".format(nd=self.data['ogit/_id']))
