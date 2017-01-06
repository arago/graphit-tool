import gevent
import requests
import requests.auth
import time
import json
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from lxml import etree as et
from StringIO import StringIO
import sys
from itertools import islice, chain
from urllib import quote_plus

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
		#print >>sys.stderr, "Request to " + url
		try:
			headers = self._headers
			headers["Accept"] = "application/json"
			r = super(GraphitSession, self).request(
				method, self._baseurl + url, headers=self._headers,
				params=params, data=json.dumps(data))
			r.raise_for_status()
		except requests.exceptions.HTTPError as e:
			raise GraphitError(self, r.status_code, e)
		return r.json()

	def get(self, resource):
		return self.request('GET', resource)
	def update(self, resource, data):
		return self.request('POST', resource, data=data)
	def replace(self, resource, data):
		return self.request('PUT', resource, data=data)
	def delete(self, resource):
		return self.request('DELETE', resource)
	def create(self, ogit_type, data):
		return self.request(
			'POST', '/new/' + quote_plus(ogit_type), data=data)
	def query(self, query, limit=-1,
			   offset=0, fields=None, concurrent=10, chunksize=10):
		return QueryResult(self, query,
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
			raise WSO2Error(e)
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
	def __init__(self, session, status, error):
		self.status=status
		self.message="{sess} returned an error: {err}".format(
			sess=session,
			err=error)

	def __str__(self):
		return self.message

class WSO2Error(Exception):
	"""Error when talking to GraphIT"""
	def __init__(self, message):
		self.message=message

	def __str__(self):
		return self.message

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
			return "".join([('\\' + c if c in "\\/+-~=\"<>!(){}[]^:&|"
					   else c)
					  for c in string])
		def join_set(lst):
			return "(" + " OR ".join([escape_term(it) for it in lst]) + ")"
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

class IDNotFoundError(Exception):
	"""Error when retrieving results"""
	def __init__(self, ID):
		self.message="Node {ID} not found!".format(ID=ID)
		self.ID = ID

	def __str__(self):
		return self.message

class QueryResult(object):
	def __init__(self, graph, query, limit=-1,
				 offset=0, fields=None, concurrent=10, chunksize=10):
		self.graph = graph;
		self.fields = fields
		result=self.graph.request(
			'POST', '/query/' + query.query_type,
			data={
				"query":str(query),
				"fields":'ogit/_id',
				"limit":limit,
				"offset":offset
			})
		if type(query) is IDQuery:
			self.result_ids = query.node_ids
		elif type(query) is ESQuery:
			self.result_ids = [i['ogit/_id'] for i in result['items'] if 'ogit/_id' in i]
		else: raise NotImplementedError
		self.concurrent=concurrent
		self.chunksize = chunksize
		self.slices = chunks(self.result_ids, concurrent*chunksize)
		self._cache=[]

	def __iter__(self): return self

	def get_values(self, ogit_ids):
		data = {"query":",".join(ogit_ids)}
		if self.fields: data['fields'] = ', '.join(self.fields)
		return self.graph.request(
			'POST',
			'/query/' + 'ids',
			data=data
		)['items']

	def next(self):
		if self.fields==['ogit/_id']:
			try:
				return {'ogit/_id':self.result_ids.pop()}
			except IndexError:
				raise StopIteration
		if self._cache:
			return self._cache.pop()
			if 'error' in item and item['error']['code'] == 404:
				raise IDNotFoundError(item['error']['ogit/_id'])
			return item
		else:
			c = [c for c in chunks(next(self.slices), self.chunksize)]
			jobs = [gevent.spawn(self.get_values, items) for items in c]
			gevent.joinall(jobs)
			self._cache = [i for l in [j.value for j in jobs] for i in l]
			return self._cache.pop()

class XMLValidateError(Exception):
	"""Error when retrieving results"""
	def __init__(self):
		self.message="XML invalid!"

	def __str__(self):
		return self.message

class XMLValidator(object):
	def __init__(self, xsd):
		xml_schema_doc = et.parse(xsd)
		self.xml_schema = et.XMLSchema(xml_schema_doc)
	def validate(self, xml_doc):
		if self.xml_schema.validate(xml_doc):
			return True
		raise XMLValidateError()


def prettify_xml(string):
	p = et.XMLParser(remove_blank_text=True)
	return et.tostring(et.fromstring(string, p), pretty_print=True)
