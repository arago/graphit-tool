#import gevent  # NOQA
import requests
import requests.auth
import time
import json
import re
import os, sys
from requests.packages.urllib3.exceptions import InsecureRequestWarning
#from lxml import etree as et
from itertools import islice, chain
try:
	from urllib import quote_plus
except ImportError:
	from urllib.parse import quote_plus
#import jsonschema
from datetime import datetime  # NOQA
try:
	from urllib3.packages.ordered_dict import OrderedDict
except ImportError:
	from collections import OrderedDict
import ijson

# import yaml
# try:
# 	from yaml import CLoader as yLoader, CDumper as yDumper  # NOQA
# except ImportError:
# 	from yaml import Loader as yLoader, Dumper as yDumper  # NOQA

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

__version__ = (0, 6, 0, "rc1")


def chunks(iterable, size=10):
	iterator = iter(iterable)
	for first in iterator:
		yield chain([first], islice(iterator, size - 1))

def DEBUG():
	return os.getenv('GT_DEBUG')

class IterStreamer(object):
	"""
	File-like streaming iterator.
	"""
	def __init__(self, generator):
		self.generator = generator
		self.iterator = iter(generator)
		self.leftover = u''

	def __len__(self):
		return self.generator.__len__()

	def __iter__(self):
		return self.iterator

	def next(self):
		return next(self.iterator)

	def read(self, size):
		count = len(self.leftover)
		data = self.leftover
		if count < size:  # ----- does not have enough in leftover
			try:
				while count < size:
					chunk = self.next()
					data += chunk
					count += len(chunk)
			except StopIteration:
				pass
		self.leftover = data[size:]
		return data[:size]


class GraphitSession(requests.Session):

	def __init__(self, baseurl, auth=None, verify=True, *args, **kwargs):
		self._baseurl = baseurl
		self._headers = {
			"User-Agent": "PyGraphIT/2.0",
			"charset": "UTF-8"
		}
		super(GraphitSession, self).__init__(*args, **kwargs)
		self.auth= auth
		self.verify = verify

	def request(self, method, url, params=None, data=None, raw=False):
		try:
			headers = self._headers
			headers["Accept"] = "application/json"
			data = data if raw else json.dumps(data)
			r = super(GraphitSession, self).request(
				method, self._baseurl + url, headers=self._headers,
				params=params, data=data, stream=True)
			if DEBUG():
				print("======================= REQUEST =======================", file=sys.stderr)
				print("{op} {url}".format(op=method, url=url), file=sys.stderr)
				print("Headers:", file=sys.stderr)
				for key, value in r.request.headers.items():
					print("  {k}: {v}".format(k=key, v=value), file=sys.stderr)
				if method in ['POST', 'PUT']:
					print("Body:", file=sys.stderr)
					print(json.dumps(data, sort_keys=True, indent=2, separators=(',', ': ')), file=sys.stderr)
				print("======================= RESPONSE ======================", file=sys.stderr)
				print("Headers:", file=sys.stderr)
				for key, value in r.headers.items():
					print("  {k}: {v}".format(k=key, v=value), file=sys.stderr)
				print("=======================================================", file=sys.stderr)
			r.raise_for_status()
		except requests.exceptions.HTTPError as e:
			try:
				error_message = r.json()['error']['message']
				raise GraphitError(self, r.status_code, error_message)
			except (ValueError, KeyError):
				raise e
		return r

	def get(self, resource, params=None):
		return self.request('GET', resource, params=params).json()

	def update(self, resource, data, params=None):
		return self.request('POST', resource, data={k:v for k,v in data.items() if k != "ogit/_id"}, params=params).json()

	def replace(self, resource, data, params=None):
		return self.request('PUT', resource, data={k:v for k,v in data.items() if k != "ogit/_id"}, params=params).json()

	def delete(self, resource):
		return self.request('DELETE', resource).json()

	def connect(self, ogit_type, in_id, out_id):
		return self.request(
			'POST', '/connect/' + quote_plus(ogit_type),
			data={'in': in_id, 'out': out_id}).json()

	def create(self, ogit_type, data):
		return self.request(
			'POST', '/new/' + quote_plus(ogit_type), data=data).json()

	def query(self, query, query_type=None, limit=-1, offset=0, fields=None, count=False):
		# === ElasticSearch and Multi-ID Queries ===
		if query_type:
			query_type = query_type
		elif not query_type and hasattr(query, "query_type"):
			query_type= query.query_type
		elif not query_type and not hasattr(query, "query_type"):
			query_type = "vertices"
		if query_type in ['vertices', 'ids']:
			data = {
				"query": str(query),
				"limit": limit,
				"offset": offset
			}
			if count:
				data['count'] = True
			if fields:
				data['fields'] = ', '.join(fields)
			response = self.request(
				'POST', '/query/{qt}/'.format(qt=query_type),
				data=data)
			stream = IterStreamer(response.iter_content(
				chunk_size=1024, decode_unicode=True))
			gen = (item for item in ijson.items(stream, 'items.item'))
			return gen
		# === Simple Edge Queries ===
		elif query_type == 'verb':
			params = {"direction": query.direction, "limit": limit, "offset": offset}
			if query.ogit_types:
				params['types'] = ', '.join(query.ogit_types)
			if fields:
				params['fields'] = ','.join(fields)
			response = self.request('GET', '/{node}/{verb}'.format(
				node=query.node_id, verb=quote_plus(query.verb)), params=params)
			stream = IterStreamer(response.iter_content(chunk_size=1024, decode_unicode=True))
			return (item for item in ijson.items(stream, 'items.item'))
		# === Gremlin Queries ===
		elif query_type == 'gremlin':
			query_string = str(query)
			data = {
				"query": query_string,
				"root": query.root
			}
			if fields:
				data['fields'] = ', '.join(fields)
			response = self.request('POST', '/query/' + query_type + '/', data=data)
			stream = IterStreamer(response.iter_content(chunk_size=1024, decode_unicode=True))
			gen = (item for item in ijson.items(stream, 'items.item'))
			return gen

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

	def __str__(self):
		str = "Token {token} expires in {exp} seconds."
		return str.format(
			token=self._token.access_token,
			exp=int(self._token.expires_in))

	@property
	def token(self):
		return self._token.access_token

	def __call__(self, r):
		if self._token.expires_in < 60:
			self.renew_token()
		r.headers['_TOKEN'] = self._token.access_token
		return r


class WSO2AuthClientCredentials(WSO2AuthBase):
	def __init__(self, baseurl, client, verify=True):
		self._client_id, self._client_secret = client
		super(WSO2AuthClientCredentials, self).__init__(
			baseurl,
			verify=verify
		)

	def get_token(self):
		super(WSO2AuthClientCredentials, self).get_token(
			headers={
				"User-Agent": "PyGraphIT/1.0",
				"Content-Type": "application/x-www-form-urlencoded",
				"charset": "UTF-8"
			},
			post_data={
				"grant_type": "client_credentials",
				"scope": "batchjob",
				"client_id": self._client_id,
				"client_secret": self._client_secret
			}
		)

	def renew_token(self):
		self.get_token()

class WSO2AuthUserCredentials(WSO2AuthBase):
	def __init__(self, baseurl, client, user_creds, verify=True):
		self._client_d , self._client_secret = client
		self._username, self._password = user_creds
		super(WSO2AuthClientCredentials, self).__init__(
			baseurl,
			verify=verify
		)

	def get_token(self):
		super(WSO2AuthClientCredentials, self).get_token(
			headers={
				"User-Agent": "PyGraphIT/1.0",
				"Content-Type": "application/x-www-form-urlencoded",
				"charset": "UTF-8"
			},
			post_data={
				"grant_type": "password",
				"scope": "individual,department,company",
				"client_id": self._client_id,
				"client_secret": self._client_secret,
				"username": self._username,
				"password": self._password
			}
		)

	def renew_token(self):
		super().get_token(
			headers={
				"User-Agent": "PyGraphIT/1.0",
				"Content-Type": "application/x-www-form-urlencoded",
				"charset": "UTF-8"
			},
			post_data={
				"grant_type": "refresh_token",
				"client_id": self._client_id,
				"client_secret": self._client_secret,
				"refresh_token": self._token.refresh_token
			}
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
	def __init__(self, session, status_code, message=None):
		self.session = session
		self.status_code = status_code
		self.message = message

	def __str__(self):
		if self.message:
			return "{sess} returned an error: {msg}".format(
				sess=self.session,
				msg=self.message)
		else:
			return "{sess} returned an error: {code}".format(
				sess=self.session,
				code=self.status_code)


class WSO2Error(Exception):
	"""Error when talking to GraphIT"""
	def __init__(self, message):
		self.message = message

	def __str__(self):
		return self.message


class GremlinQuery(object):
	def __init__(self, root, query=None):
		self.root = root
		self.query = query
		self.query_type = "gremlin"

	def __str__(self):
		return self.query


class EESQuery(object):
	def __init__(self, *clauses, operation='AND'):
		self._op = operation
		self._ch = list(clauses)
		#print(self._ch)
		self.query_type = "vertices"

	def append(self, *clauses):
		self._ch.extend(clauses)

	def clear(self):
		self._ch = []

	def __str__(self):
		def escape_fieldname(string):
			string = [('\\' + c if c in "\\/+-~=\"<>!(){}[]^:&|*?"
					   else c)
					  for c in string]
			if string[0] in ['\\+', '\\-']:
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
			elif string[0] in [">", "<"]:
				return string[0] + escape_term(string[1:])
			else:
				return "".join([('\\' + c if c in "\\/+-~=<>!(){}[]^:&|"
						   else c)
						  for c in string])

		def escape_es(string):
			if string in ['AND', 'OR', 'NOT']:
				return string
			field, cond = tuple(string.split(':', 1))
			return "{field}:{cond}".format(
				field=escape_fieldname(field),
				cond=escape_term(cond)
			)

		if self._op in ["AND", "OR", '']:
			term = " {op} ".format(op=self._op).join(
				[str(child) if isinstance(child, EESQuery) else escape_es(child) for child in self._ch]
			)
			return "( " + term + " )" if len(self._ch) >= 2 else term
		elif self._op == "NOT" and len(self._ch) == 1:
			return "{op} {term}".format(op=self._op, term=str(self._ch[0]))
		else:
			raise NotImplementedError


class IDQuery(object):
	def __init__(self, node_ids):
		self.node_ids = []
		self.add(node_ids)
		self.query_type = 'ids'

	def add(self, node_ids):
		if type(node_ids) is list:
			self.node_ids.extend(node_ids)
		elif type(node_ids) is str:
			self.node_ids.append(node_ids)
		else:
			raise TypeError

	def __str__(self):
		return ",".join(self.node_ids)



class VerbQuery(object):
	def __init__(self, node_id, verb, ogit_types=None, fields=None, direction='both'):
		self.node_id = node_id
		self.verb = verb
		self.ogit_types = ogit_types
		self.direction = direction
		self.fields = fields
		self.query_type = 'verb'

	def __str__(self):
		return ""


class IDNotFoundError(Exception):
	"""Error when retrieving results"""
	def __init__(self, ID):
		self.message = "Node {ID} not found!".format(ID=ID)
		self.ID = ID

	def __str__(self):
		return self.message


# class XMLValidateError(Exception):
# 	"""Error when retrieving results"""
# 	def __init__(self):
# 		self.message = "XML invalid!"

# 	def __str__(self):
# 		return self.message


# class XMLValidator(object):
# 	def __init__(self, xsdfile):
# 		xml_schema_doc = et.parse(xsdfile)
# 		self.xml_schema = et.XMLSchema(xml_schema_doc)

# 	def validate(self, xml_doc):
# 		if self.xml_schema.validate(xml_doc):
# 			return True
# 		raise XMLValidateError()


# class JSONValidateError(Exception):
# 	def __init__(self, msg=None):
# 		self.message = msg if msg else "JSON invalid!"

# 	def __str__(self):
# 		return self.message


# class JSONValidator(object):
# 	def __init__(self, schemafile):
# 		try:
# 			self.json_schema = json.load(schemafile)
# 		except Exception:
# 			raise

# 	def validate(self, doc):
# 		try:
# 			jsonschema.validate(doc, self.json_schema)
# 			return True
# 		except jsonschema.ValidationError as e:
# 			raise JSONValidateError(e)


# def prettify_xml(string):
# 	p = et.XMLParser(remove_blank_text=True)
# 	return et.tostring(et.fromstring(string, p), pretty_print=True)


# def xml_to_yaml(string):
# 	try:
# 		issue = {'Issue': OrderedDict()}
# 		string = string.replace(
# 				'xmlns="https://graphit.co/schemas/v2/IssueSchema"',
# 				'xmlnamespace="https://graphit.co/schemas/v2/IssueSchema"')
# 		xml = et.fromstring(string)
# 		for k, v in xml.items():
# 			if k == 'xmlnamespace':
# 				issue['Issue']['xmlns'] = str(v)
# 			else:
# 				issue['Issue'][str(k)] = str(v)
# 		for k in list(xml):
# 			c = k.find('Content')
# 			if c is not None:
# 				issue['Issue'][k.tag] = OrderedDict()
# 				issue['Issue'][k.tag][c.tag] = dict(c.attrib)
# 			else:
# 				issue['Issue'][k.tag] = {}
# 		return issue
# 	except Exception as e:
# 		print(e)


class GraphitNodeError(Exception):
	"""Error when retrieving results"""
	def __init__(self, message):
		self.message = message

	def __str__(self):
		return self.message


class GraphitObject(object):
	def __init__(self, session, data):
		self.data = data
		self.session = session
		self.owner_error_regex = re.compile(r'owner ([a-z0-9\-._]+ )?does not exist')

	def json(self, pretty_print=False):
		if pretty_print:
			return json.dumps(self.data, sort_keys=True, indent=4, separators=(',', ': '))
		else:
			return json.dumps(self.data)

class GraphitNode(GraphitObject):

	@classmethod
	def from_graph(cls, session, ogit_id):
		data = session.get('/' + quote_plus(ogit_id))
		return cls(session, data)

	def create_owner(self, owner=None):
		if not owner:
			owner = self.data['ogit/_owner']
		GraphitNode(self.session, {
					"ogit/_custom-id": self.data['ogit/_owner'],
					"ogit/_id": self.data['ogit/_owner'],
					"ogit/_owner": self.data['ogit/_owner'],
					"ogit/_type": "ogit/Organization",
					"ogit/description": "created by MARS upload",
					"ogit/name": self.data['ogit/_owner']
				}).push()

	def get_attr(self, attr):
		if attr in self.data:
			return self.data[attr]

	def set_attr(self, attr, value):
		self.data[attr] = value
		self.update()
		if value is None:
			del self.data[attr]

	def del_attr(self, attr):
		self.set_attr(attr, None)

	# backwards compatibility
	def rem_attr(self, attr):
		return self.del_attr(attr)

	def connect(self, ogit_type, node, reverse=False):
		in_id = node.get_attr('ogit/_id') if reverse else self.get_attr('ogit/_id')
		out_id = self.get_attr('ogit/_id') if reverse else node.get_attr('ogit/_id')
		self.session.connect(ogit_type, in_id, out_id)

	def create(self, owner=None):
		try:
			self.data = self.session.create(self.data['ogit/_type'], self.data)
		except GraphitError as e:
			if e.status == 400 and self.owner_error_regex.match(e.message):
				self.create_owner(owner)
				self.create()
			else:
				raise

	def update(self):
		
		self.session.update('/' + quote_plus(self.data["ogit/_id"]), self.data)

	def pull(self):
		self.data = self.session.get('/' + quote_plus(self.data['ogit/_id']))

	def push(self, replace=True, owner=None):
		if 'ogit/_id' in self.data:
			try:
				self.session.update(
					'/' + quote_plus(self.data["ogit/_id"]),
					{k:v for k,v in self.data.items() if k != "ogit/_id"},
					params={
						'createIfNotExists': 'true',
						'ogit/_type': self.data['ogit/_type']
					}
				)
			except GraphitError as e:
				if e.status == 400 and self.owner_error_regex.match(e.message):
					self.create_owner(owner)
					self.push()
				else:
					raise
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
				raise


# class MARSNodeError(Exception):
# 	"""Error when retrieving results"""
# 	def __init__(self, message):
# 		self.message = message

# 	def __str__(self):
# 		return self.message


# class MARSNode(GraphitNode):
# 	@classmethod
# 	def from_xmlfile(cls, session, filename, validator=None):
# 		try:
# 			xml_doc = et.parse(filename).getroot()
# 			if validator:
# 				validator.validate(xml_doc)
# 			ogit_id = xml_doc.attrib['ID']
# 			ogit_name = xml_doc.attrib['NodeName']
# 			ogit_automation_marsnodetype = xml_doc.attrib['NodeType']
# 			data = {
# 				'ogit/Automation/marsNodeFormalRepresentation': et.tostring(xml_doc),
# 				'ogit/_owner': xml_doc.attrib['CustomerID'],
# 				'ogit/_id': ogit_id,
# 				'ogit/_type': 'ogit/Automation/MARSNode',
# 				'ogit/name': ogit_name,
# 				'ogit/Automation/marsNodeType': ogit_automation_marsnodetype,
# 				'ogit/id': ogit_name
# 			}
# 		except XMLValidateError:
# 			raise MARSNodeError("ERROR: {f} does not contain a valid MARS node".format(f=filename))
# 		except et.XMLSyntaxError:
# 			raise MARSNodeError("ERROR: {f} does not contain valid XML".format(f=filename))
# 		return cls(session, data)

# 	@classmethod
# 	def from_jsonfile(cls, session, filename, mars_validator=None, json_validator=None):
# 		try:
# 			with open(filename) as jsonfile:
# 				try:
# 					doc = json.load(jsonfile)
# 				except ValueError:
# 					raise MARSNodeError("ERROR: {f} does not contain valid JSON".format(f=filename))
# 			if json_validator:
# 				json_validator.validate(doc)
# 			xml_doc = et.fromstring(doc['ogit/Automation/marsNodeFormalRepresentation'])
# 			if mars_validator:
# 				mars_validator.validate(xml_doc)
# 			ogit_id = xml_doc.attrib['ID']
# 			ogit_name = xml_doc.attrib['NodeName']
# 			ogit_automation_marsnodetype = xml_doc.attrib['NodeType']
# 			data = {
# 				'ogit/Automation/marsNodeFormalRepresentation': et.tostring(xml_doc),
# 				'ogit/_owner': xml_doc.attrib['CustomerID'],
# 				'ogit/_id': ogit_id,
# 				'ogit/_type': 'ogit/Automation/MARSNode',
# 				'ogit/name': doc['ogit/name'] if 'ogit/name' in doc else ogit_name,
# 				'ogit/Automation/marsNodeType': ogit_automation_marsnodetype,
# 				'ogit/id': doc['ogit/id'] if 'ogit/id' in doc else ogit_name
# 			}
# 		except XMLValidateError:
# 			raise MARSNodeError("ERROR: 'ogit/Automation/marsNodeFormalRepresentation' in {f} "
# 								"does not contain a valid MARS node".format(f=filename))
# 		except JSONValidateError:
# 			raise MARSNodeError("ERROR: {f} does not contain valid ogit/Automation/MARSNode JSON".format(f=filename))
# 		return cls(session, data)

# 	def print_node(self, stream):
# 		try:
# 			print(prettify_xml(self.data['ogit/Automation/marsNodeFormalRepresentation']), file=stream)
# 		except KeyError:
# 			if 'error' in self.data:
# 				raise MARSNodeError("ERROR: MARS Node '{nd}' {err}".format(
# 					nd=self.data['error']['ogit/_id'], err=self.data['error']['message']))
# 			else:
# 				raise MARSNodeError("ERROR: MARS Node {nd} is missing "
# 									"'ogit/Automation/marsNodeFormalRepresentation' "
# 									"attribute! Maybe it's not a MARS node?".format(nd=self.data['ogit/_id']))

# 	def push(self, **kwargs):
# 		self.data['ogit/Automation/isDeployed'] = None
# 		self.data['ogit/Automation/deployStatus'] = None
# 		super(MARSNode, self).push(**kwargs)


# class KnowledgeItemError(Exception):
# 	"""Error when retrieving results"""
# 	def __init__(self, message):
# 		self.message = message

# 	def __str__(self):
# 		return self.message


# class KnowledgeItem(GraphitNode):
# 	@classmethod
# 	def from_xmlfile(cls, session, filename, deploy, validator=None):
# 		try:
# 			xml_doc = et.parse(filename).getroot()
# 			if validator:
# 				validator.validate(xml_doc)
# 			ogit_id = xml_doc.attrib['ID']
# 			ogit_name = xml_doc.find('{https://graphit.co/schemas/v2/KiSchema}Title').text
# 			data = {
# 				'ogit/Automation/knowledgeItemFormalRepresentation': et.tostring(xml_doc),
# 				'ogit/_owner': "arago.co",
# 				"ogit/Automation/deployToEngine": "true" if deploy else "false",
# 				'ogit/_id': ogit_id,
# 				'ogit/_type': 'ogit/Automation/KnowledgeItem',
# 				'ogit/name': ogit_name
# 			}
# 		except XMLValidateError:
# 			raise KnowledgeItemError("ERROR: {f} does not contain a valid Knowledge Item".format(f=filename))
# 		except et.XMLSyntaxError:
# 			raise KnowledgeItemError("ERROR: {f} does not contain valid XML".format(f=filename))
# 		return cls(session, data)

# 	@classmethod
# 	def from_jsonfile(cls, session, filename, ki_validator=None, json_validator=None):
# 		try:
# 			with open(filename) as jsonfile:
# 				try:
# 					doc = json.load(jsonfile)
# 				except ValueError:
# 					raise MARSNodeError("ERROR: {f} does not contain valid JSON".format(f=filename))
# 			if json_validator:
# 				json_validator.validate(doc)
# 			xml_doc = et.fromstring(doc['ogit/Automation/knowledgeItemFormalRepresentation'])
# 			if ki_validator:
# 				ki_validator.validate(xml_doc)
# 			ogit_id = xml_doc.attrib['ID']
# 			ogit_name = xml_doc.attrib['NodeName']  # title
# 			data = {
# 				'ogit/Automation/marsNodeFormalRepresentation': et.tostring(xml_doc),
# 				'ogit/_owner': xml_doc.attrib['CustomerID'],
# 				'ogit/_id': ogit_id,
# 				'ogit/_type': 'ogit/Automation/KnowledgeItem',
# 				'ogit/name': doc['ogit/name'] if 'ogit/name' in doc else ogit_name,
# 				'ogit/id': doc['ogit/id'] if 'ogit/id' in doc else ogit_name
# 			}
# 		except XMLValidateError:
# 			raise KnowledgeItemError("ERROR: 'ogit/Automation/knowledgeItemFormalRepresentation' in {f} "
# 									 "does not contain a valid Knowledge Item".format(f=filename))
# 		except JSONValidateError:
# 			raise MARSNodeError("ERROR: {f} does not contain valid ogit/Automation/KnowledgeItem "
# 								"JSON".format(f=filename))
# 		return cls(session, data)

# 	def print_node(self, stream):
# 		try:
# 			print(prettify_xml(self.data['ogit/Automation/knowledgeItemFormalRepresentation']), file=stream)
# 		except KeyError:
# 			if 'error' in self.data:
# 				raise KnowledgeItemError("ERROR: Knowledge Item '{nd}' {err}".format(
# 					nd=self.data['error']['ogit/_id'], err=self.data['error']['message']))
# 			else:
# 				raise KnowledgeItemError("ERROR: Knowledge Item {nd} is missing "
# 										 "'ogit/Automation/knowledgeItemFormalRepresentation' "
# 										 "attribute! Maybe it's not a KnowledgeItem?".format(
# 											 nd=self.data['ogit/_id']))

# 	def push(self, **kwargs):
# 		self.data['ogit/Automation/isDeployed'] = None
# 		self.data['ogit/Automation/deployStatus'] = None
# 		super(KnowledgeItem, self).push(**kwargs)

# 	def deploy(self):
# 		self.data['ogit/Automation/deployToEngine'] = "true"
# 		self.push()

# 	def undeploy(self):
# 		self.data['ogit/Automation/deployToEngine'] = "false"
# 		self.push()


# class AutomationIssueError(Exception):
# 	"""Error when retrieving results"""
# 	def __init__(self, message):
# 		self.message = message

# 	def __str__(self):
# 		return self.message


# class AutomationIssue(GraphitNode):
# 	@classmethod
# 	def from_xmlfile(cls, session, filename, validator=None):
# 		try:
# 			xml_doc = et.parse(filename).getroot()
# 			if validator:
# 				validator.validate(xml_doc)
# 			ogit_id = xml_doc.attrib['ID']
# 			ogit_name = xml_doc.attrib['NodeName']
# 			ogit_automation_marsnodetype = xml_doc.attrib['NodeType']
# 			data = {
# 				'ogit/Automation/marsNodeFormalRepresentation': et.tostring(xml_doc),
# 				'ogit/_owner': xml_doc.attrib['CustomerID'],
# 				'ogit/_id': ogit_id,
# 				'ogit/_type': 'ogit/Automation/MARSNode',
# 				'ogit/name': ogit_name,
# 				'ogit/Automation/marsNodeType': ogit_automation_marsnodetype,
# 				'ogit/id': ogit_name
# 			}
# 		except XMLValidateError:
# 			raise MARSNodeError("ERROR: {f} does not contain a valid MARS node".format(f=filename))
# 		except et.XMLSyntaxError:
# 			raise MARSNodeError("ERROR: {f} does not contain valid XML".format(f=filename))
# 		return cls(session, data)

# 	@classmethod
# 	def from_yamlfile(cls, session, filename, mars_validator=None, json_validator=None):
# 		try:
# 			with open(filename) as yamlfile:
# 				try:
# 					doc = yaml.load(yamlfile)
# 				except ValueError:
# 					raise MARSNodeError("ERROR: {f} does not contain valid JSON".format(f=filename))
# 			if json_validator:
# 				json_validator.validate(doc)
# 			xml_doc = et.fromstring(doc['ogit/Automation/marsNodeFormalRepresentation'])
# 			if mars_validator:
# 				mars_validator.validate(xml_doc)
# 			ogit_id = xml_doc.attrib['ID']
# 			ogit_name = xml_doc.attrib['NodeName']
# 			ogit_automation_marsnodetype = xml_doc.attrib['NodeType']
# 			data = {
# 				'ogit/Automation/marsNodeFormalRepresentation': et.tostring(xml_doc),
# 				'ogit/_owner': xml_doc.attrib['CustomerID'],
# 				'ogit/_id': ogit_id,
# 				'ogit/_type': 'ogit/Automation/MARSNode',
# 				'ogit/name': doc['ogit/name'] if 'ogit/name' in doc else ogit_name,
# 				'ogit/Automation/marsNodeType': ogit_automation_marsnodetype,
# 				'ogit/id': doc['ogit/id'] if 'ogit/id' in doc else ogit_name
# 			}
# 		except XMLValidateError:
# 			raise MARSNodeError("ERROR: 'ogit/Automation/marsNodeFormalRepresentation' in {f} does not "
# 								"contain a valid MARS node".format(f=filename))
# 		except JSONValidateError:
# 			raise MARSNodeError("ERROR: {f} does not contain valid ogit/Automation/MARSNode "
# 								"JSON".format(f=filename))
# 		return cls(session, data)

# 	def print_node(self, stream, format='yaml'):
# 		def blockseq_rep(dumper, data):
# 			return dumper.represent_mapping(u'tag:yaml.org,2002:map', data.items(), flow_style=False)
# 		yaml.add_representer(OrderedDict, blockseq_rep)
# 		try:
# 			issue = xml_to_yaml(self.data['ogit/Automation/issueFormalRepresentation'])
# 			q = "outE('ogit/generates').inV().has('ogit/_type', 'ogit/Automation/History')"
# 			fields = [
# 				'ogit/message',
# 				'ogit/Automation/command',
# 				'ogit/Automation/affectedNodeId',
# 				'ogit/Automation/knowledgeItemId',
# 				'ogit/Automation/logLevel',
# 				'ogit/timestamp'
# 			]
# 			q = GremlinQuery(root=self.data['ogit/_id'], query=q)
# 			history = self.session.query(q, fields=fields)
# 			history = sorted(history, key=lambda k: k['ogit/timestamp'])
# 			history = [
# 				{
# 					"ElementMessage": str(item['ogit/message']),
# 					"ElementName":str(item['ogit/Automation/command']),
# 					"KIID":str(item['ogit/Automation/knowledgeItemId']),
# 					"LogLevel":str(item['ogit/Automation/logLevel']),
# 					"ModelNodeID":str(item['ogit/Automation/affectedNodeId']),
# 					"Timestamp":str(item['ogit/timestamp'])
# 				} for item in history
# 			]
# 			kiids = list(set([item['KIID'] for item in history]))
# 			if kiids:
# 				kinames = list(self.session.query(IDQuery(kiids), fields=['ogit/_id', 'ogit/name']))
# 			else:
# 				kinames = []
# 			kinames = dict(((item['ogit/_id'], item['ogit/name']) for item in kinames if 'ogit/name' in item))
# 			for item in history:
# 				if item['KIID'] in kinames:
# 					item['KIName'] = str(kinames[item['KIID']])
# 			#issue['Issue']['History'] = {"HistoryEntry": history}
# 			yaml.dump(issue, stream, default_flow_style=False)
# 		except KeyError:
# 			if 'error' in self.data:
# 				raise AutomationIssueError("ERROR: Automation Issue '{nd}' {err}".format(
# 					nd=self.data['error']['ogit/_id'], err=self.data['error']['message']))
# 			else:
# 				raise AutomationIssueError("ERROR: Vertex {nd} is missing "
# 										   "'ogit/Automation/marsNodeFormalRepresentation' "
# 										   "attribute! Maybe it's not an AutomationIssue?".format(
# 											   nd=self.data['ogit/_id']))

# 	def push(self, **kwargs):
# 		self.data['ogit/Automation/isDeployed'] = None
# 		self.data['ogit/Automation/deployStatus'] = None
# 		super(AutomationIssue, self).push(**kwargs)

# 	def get_formalrep(self):
# 		xml = self.get_attr('ogit/Automation/issueFormalRepresentation')
# 		#print prettify_xml(xml)
# 		frep = et.fromstring(xml)
# 		return frep

# 	def get_var(self, var, key=None, node=None):
# 		result = []
# 		frep = self.get_formalrep()
# 		for r in frep.findall(".//" + "{https://graphit.co/schemas/v2/IssueSchema}" + var):
# 			for c in r.findall(".//" + "{https://graphit.co/schemas/v2/IssueSchema}Content"):
# 				if key and node and 'Key' in c.attrib and 'CreatedOn' in c.attrib and c.attrib['Key']==key and c.attrib['CreatedOn']==node:
# 					result.append(c.attrib['Value'])
# 				elif key and not node and 'Key' in c.attrib and c.attrib['Key']==key:
# 					result.append(c.attrib['Value'])
# 				elif not key and node and 'CreatedOn' in c.attrib and c.attrib['CreatedOn']==node:
# 					result.append(c.attrib['Value'])
# 				elif not key and not node:
# 					result.append(c.attrib['Value'])
# 		#print(prettify_xml(et.tostring(frep)))
# 		return result

# 	def add_var(self, var, value, key=None, node=None):
# 		frep = self.get_formalrep()
# 		for k,v in frep.attrib.iteritems():
# 			if k not in ['xmlns', 'IID']:
# 				del frep.attrib[k]
# 		for v in list(frep):
# 			frep.remove(v)
# 		nv = et.Element("{https://graphit.co/schemas/v2/IssueSchema}" + var)
# 		if key and node:
# 			n = et.Element("Content", Key=key, CreatedOn=node, Value=value)
# 		elif key:
# 			n = et.Element("Content", Key=key, Value=value)
# 		elif node:
# 			n = et.Element("Content", CreatedOn=node, Value=value)
# 		else:
# 			n = et.Element("Content", Value=value)
# 		nv.append(n)
# 		frep.append(nv)
# 		self.set_attr("ogit/Automation/issueFormalRepresentation", et.tostring(frep))
# 		#print prettify_xml(et.tostring(frep))


def resumeable(gen, stream):
	while True:
		try:
			yield next(gen)
		except GraphitNodeError as e:
			print(e, file=stream)
		except AutomationIssueError as e:
			print(e, file=stream)
		except StopIteration:
			raise

class Attachment(GraphitNode):
	def get_content(self):
		response = self.session.request(
			'GET',
			('/' + quote_plus(self.data['ogit/_id']) + '/content')
		)
		return response.content

	def put_content(self, binary):
		self.session.request(
			'POST',
			('/' + quote_plus(self.data['ogit/_id']) + '/content'),
			data=binary, raw=True
		)

class Timeseries(GraphitNode):
	def get_values(self, start=None, end=None):
		params = {}
		if start:
			params['from']=start
		if end:
			params['to']=end
		response = self.session.request(
			'GET',
			('/' + quote_plus(self.data['ogit/_id']) + '/values'),
			params=params
		)
		stream = IterStreamer(response.iter_content(chunk_size=1024, decode_unicode=True))
		return (item for item in ijson.items(stream, 'items.item'))

	def put_values(self, values, size=None):
		if size:
			for chunk in chunks(values, size=size):
				self.session.request(
					'POST',
					('/' + quote_plus(self.data['ogit/_id']) + '/values'),
					data={"items": list(chunk)})
		else:
			self.session.request(
				'POST',
				('/' + quote_plus(self.data['ogit/_id']) + '/values'),
				data={"items":list(values)})
