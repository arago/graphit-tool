#import gevent  # NOQA
import requests
import requests.auth
import time
import json
import re
import os, sys
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from itertools import islice, chain
try:
	from urllib import quote_plus
except ImportError:
	from urllib.parse import quote_plus
from datetime import datetime  # NOQA
import ijson

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

__version__ = (0, 6, 0, "rc1")


def chunks(iterable, size=10):
	iterator = iter(iterable)
	for first in iterator:
		yield chain([first], islice(iterator, size - 1))

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


def get_debug_level():
	try:
		return int(os.getenv('GT_DEBUG'))
	except (ValueError, TypeError):
		return 1 if os.getenv('GT_DEBUG') else 0


def debug_request(response, *args, **kwargs):
	if get_debug_level() == 0:
		return
	print("======================= REQUEST =======================", file=sys.stderr)
	print("{op} {url}".format(op=response.request.method, url=response.request.url), file=sys.stderr)
	if get_debug_level() >= 3:
		print("Headers:", file=sys.stderr)
		for key, value in response.request.headers.items():
			print("  {k}: {v}".format(k=key, v=value), file=sys.stderr)
	if response.request.body:
		print("Body:", file=sys.stderr)
		print(response.request.body, file=sys.stderr)


def debug_response(response, *args, **kwargs):
	if get_debug_level() == 0:
		return
	print("======================= RESPONSE ======================", file=sys.stderr)
	if get_debug_level() >= 3:
		print("Headers:", file=sys.stderr)
		for key, value in response.headers.items():
			print("  {k}: {v}".format(k=key, v=value), file=sys.stderr)
	if response.text and get_debug_level() >= 2:
		print("Body:", file=sys.stderr)
		print(response.text, file=sys.stderr)

class RestSession(requests.Session):

	def __init__(self, baseurl, auth=None, verify=True, *args, **kwargs):
		self._baseurl = baseurl
		self._headers = {
			"User-Agent": "PyGraphIT/2.0",
			"charset": "UTF-8",
			"Content-type": "application/json"
		}
		super().__init__(*args, **kwargs)
		self.auth= auth
		self.verify = verify
		self.hooks['response'].extend([debug_request, debug_response])

	def __repr__(self):
		cls = type(self)
		myurl = self._baseurl
		return f"<{cls.__name__} '{myurl}'>"

	def request(self, method, url, params=None, data=None, raw=False, *args, **kwargs):
		try:
			if data and not raw:
				data = json.dumps(data)
			r = super().request(
				method, self._baseurl + url, headers=self._headers,
				params=params, data=data, stream=True, *args, **kwargs)
			r.raise_for_status()
		except requests.exceptions.HTTPError as e:
			try:
				error_message = r.json()['error']['message']
				raise GraphitError(self, r.status_code, error_message)
			except (ValueError, KeyError, TypeError):
				raise e
		return r


class GraphitSession(RestSession):

	def request(self, method, url, params=None, data=None, raw=False, *args, **kwargs):
		headers = self._headers
		headers["Accept"] = "application/json"
		if params:
			params['listMeta'] = "true"
		else:
			params = {"listMeta": "true"}
		return super().request(method, url, params, data, raw, *args, **kwargs)

	def get(self, resource, data=None, params=None, *args, **kwargs):
		return self.request('GET', resource, data=data, params=params, *args, **kwargs).json()

	def update(self, resource, data, params=None, *args, **kwargs):
		return self.request('POST', resource, data={k:v for k,v in data.items() if k not in ["ogit/_id", "ogit/_type", "ogit/_xid"]}, params=params, *args, **kwargs).json()

	def replace(self, resource, data, params=None, *args, **kwargs):
		return self.request('PUT', resource, data={k:v for k,v in data.items() if k not in ["ogit/_id", "ogit/_type", "ogit/_xid"]}, params=params, *args, **kwargs).json()

	def delete(self, resource, *args, **kwargs):
		return self.request('DELETE', resource, *args, **kwargs).json()

	def connect(self, ogit_type, in_id, out_id, *args, **kwargs):
		return self.request(
			'POST', '/connect/' + quote_plus(ogit_type),
			data={'in': in_id, 'out': out_id}, *args, **kwargs).json()

	def create(self, ogit_type, data, *args, **kwargs):
		return self.request(
			'POST', '/new/' + quote_plus(ogit_type), data=data, *args, **kwargs).json()

	def query(self, query, query_type=None, limit=-1, offset=0, fields=None, count=False, order=None, *args, **kwargs):
		# === ElasticSearch and Multi-ID Queries ===
		if query_type:
			query_type = query_type
		elif not query_type and hasattr(query, "query_type"):
			query_type = query.query_type
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
			if order:
				data['order'] = order
			response = self.request(
				'POST', '/query/{qt}/'.format(qt=query_type),
				data=data, *args, **kwargs)
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
				node=query.node_id, verb=quote_plus(query.verb)), params=params, *args, **kwargs)
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
			response = self.request('POST', '/query/' + query_type + '/', data=data, *args, **kwargs)
			stream = IterStreamer(response.iter_content(chunk_size=1024, decode_unicode=True))
			gen = (item for item in ijson.items(stream, 'items.item'))
			return gen

	def __str__(self):
		return 'GraphIT at {url}'.format(url=self._baseurl)

class GraphitToken(object):
	def __init__(self, t):
		self.access_token = t['_TOKEN']
		self.application = t['_APPLICATION']
		self.identity = t['_IDENTITY']
		self.identity_id = t['_IDENTITY_ID']
		self.expires_at = t['expires-at']

	def __str__(self):
		return self.access_token

	@property
	def expires_in(self):
		return int((self.expires_at - (time.time() * 1000)) // 1000)

class GraphitAuthBase(requests.auth.AuthBase):
	def __init__(self, baseurl, verify=True):
		self._baseurl = baseurl
		self._verify = verify
		self._token = self._get_token()

	@property
	def token(self):
		return self._token.access_token

	def _renew_token(self):
		self._token = self._get_token()

	def handle_response(self, response, retry=0, *args, **kwargs):
		if response.status_code == 401 and retry < 10:
			#print(f"Renewing token and repeating request: Try {retry}")
			if retry > 0:
				timeout = min(5, 0.01 * 2 ** retry)
				#print(f"Throttling a little bit ({timeout})")
				time.sleep(timeout)
			self._renew_token()
			response.request.headers['_TOKEN'] = self.token
			return self.handle_response(response.connection.send(response.request, *args, **kwargs), retry=retry+1)
		return response

	def __call__(self, r):
		r.headers['_TOKEN'] = self.token
		r.register_hook('response', self.handle_response)
		return r

class GraphitAppAuth(GraphitAuthBase):
	def __init__(self, baseurl, client_id, client_secret, username, password, verify=True):
		self._client_id = client_id
		self._client_secret = client_secret
		self._username = username
		self._password = password
		super().__init__(baseurl=baseurl, verify=verify)

	def _get_token(self):
		try:
			r = requests.post(
				"{baseurl}/api/6/auth/app".format(baseurl=self._baseurl),
				headers = {
					"User-Agent": "python-graphit/2.0",
					"Content-type": "application/json",
					"Charset": "UTF-8"
				},
				json = {
					"client_id": self._client_id,
					"client_secret": self._client_secret,
					"username": self._username,
					"password": self._password
				},
				verify = self._verify
			)
			r.raise_for_status()
		except requests.exceptions.HTTPError as e:
			try:
				error_message = r.json()['error']['message']
				raise GraphitError(self, r.status_code, error_message)
			except (ValueError, KeyError):
				raise e
		return GraphitToken(r.json())


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
			if r.status_code in [400, 401]:
				raise WSO2Error("Could not get an access token from WSO2, check client credentials!")
			else:
				raise
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
		self._client_id, self._client_secret = client
		self._username, self._password = user_creds
		super().__init__(
			baseurl,
			verify=verify
		)

	def get_token(self):
		super().get_token(
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

	@classmethod
	def from_xid(cls, session, xid):
		try:
			data = session.get('/xid/' + quote_plus(xid))['items'][0]
		except (KeyError, IndexError):
			raise GraphitError(self, 404, "Not found")
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
		#self.update()
		self.session.update('/' + quote_plus(self.data["ogit/_id"]), {attr: value})
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
			if e.status_code == 400 and self.owner_error_regex.match(e.message):
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
				if e.status_code == 400 and self.owner_error_regex.match(e.message):
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
			if e.status_code == 404:
				raise GraphitNodeError("Cannot delete node '{nd}': Not found!".format(nd=self.data["ogit/_id"]))
			elif e.status_code == 409:
				raise GraphitNodeError("Cannot delete node '{nd}': Already deleted!".format(nd=self.data["ogit/_id"]))
			else:
				raise

class EngineVariable(object):
	def __init__(self, data):
		self._data = data

	@staticmethod
	def _filter(data, key, value):
		if key:
			return [item for item in data if key in item and item[key] == value]
		elif key == False:
			return [item for item in data if key not in item]

	@staticmethod
	def _values(data, attr):
		return [item[attr] for item in data if attr in item]

	def _by_key(self, key, flatten=True):
		result = self._filter(self._data, "key",  key)
		if flatten:
			result = self._values(result, "value")
		return result

	def by_created_on(self, ogit_id, flatten=True):
		result = self._filter(self._data, "created_on", ogit_id)
		if flatten:
			result = self._values(result, "value")
		return result


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

class GraphitIAMItem(object):
	_basepath = '/api/6/iam'

	def __init__(self, session, auth_type, data=None):
		self._session = session
		self._data = data if data else {}
		self._auth_type = auth_type

	@classmethod
	def from_graph(cls, session, auth_type, ogit_id):
		data = session.get("{path}/{auth_type}/{ogit_id}".format(path=GraphitIAMItem._basepath, auth_type=auth_type, ogit_id=ogit_id))
		return cls(session, data)

	def create(self, data):
		r = self._session.request('POST', "{path}/{auth_type}".format(path=GraphitIAMItem._basepath, auth_type=self._auth_type), data =data)
		self._data = r.json()

	def pull(self):
		self._data = self._session.get("{path}/{auth_type}/{ogit_id}".format(path=self._basepath,))

	def push(self):
		pass

	def activate(self, auth_type):
		return self._session.request('PATCH', "{path}/{auth_type}/{ogit_id}".format(path=self._basepath, auth_type=auth_type, ogit_id=self._data['ogit/_id']), data={}).json()

class GraphitAccount(GraphitIAMItem):
	def __init__(self, session, data=None):
		super().__init__(session=session, auth_type='accounts', data=data)

	@classmethod
	def from_graph(cls, session, ogit_id):
		return super().from_graph(session=session, auth_type='accounts', ogit_id=ogit_id)

	def activate(self):
		return super().activate('accounts')

	def create(self, name, password = None):
		data = {
			'ogit/name': name,
		}
		if password:
			data['password'] = password
		super().create(auth_type='accounts', data=data)

class GraphitRole(GraphitIAMItem):
	def __init__(self, session, data=None):
		super().__init__(session=session, auth_type='roles', data=data)

class GraphitIAM(object):
	def __init__(self, session):
		self._session = session
		self._basepath = '/api/6/iam'

	def _retrieve(self, auth_type, search=None):
		search = search if search else {}
		response = self._session.request('GET', "{path}/{auth_type}".format(path=self._basepath, auth_type=auth_type), data=search)
		stream = IterStreamer(response.iter_content(chunk_size=1024, decode_unicode=True))
		return (item for item in ijson.items(stream, 'item'))

	def accounts(self, search=None):
		return self._retrieve('accounts', search=search)

	def roles(self, search=None):
		return self._retrieve('roles', search=search)
