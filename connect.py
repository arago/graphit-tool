import yaml
from graphit import RestSession
try:
	import ujson as json
except ImportError:
	import json

class ConnectITSession(RestSession):

	# Fixme: Fetches all views, even when a specific one is
	# requested
	@property
	def views(self):
		return {item['id']:ConnectITView(self, item) for item in self.request('GET', '/views/').json()}

class ConnectITView(object):
	def __init__(self, session, data):
		self.session = session
		self.data = data

	def __getattr__(self, name):
		try:
			return self.data[name]
		except KeyError:
			raise AttributeError

	def __repr__(self):
		cls = type(self)
		myid = self.data['id']
		return f"<{cls.__name__} '{myid}'>"

	@classmethod
	def from_graph(cls, session, view_id):
		data = session.request('GET', f"/views/{view_id}/").json()
		return cls(session, data)

	@classmethod
	def from_yaml(cls, session, yaml_data):
		data = yaml.load(yaml_data, Loader=yaml.CLoader)
		return cls(session, data)

	@classmethod
	def from_params(cls, session, view_id, sdf_definition, kafka_topic_prefix=None, xid_prefix=None):
		if not kafka_topic_prefix:
			kafka_topic_prefix = f"{view_id}-topic---"
		if not xid_prefix:
			xid_prefix = f"{view_id}~~~"
		data = {
			"id": view_id,
			"kafkaTopicPrefix": kafka_topic_prefix,
			"xidPrefix": xid_prefix,
			"sdfDefinition": sdf_definition
		}
		return cls(session, data)

	@property
	def id(self):
		return self.data['id']

	@property
	def json(self):
		return json.dumps(self.data, sort_keys=True, indent=4, separators=(',', ': '))

	@property
	def yaml(self):
		def blockseq_rep(dumper, data):
			return dumper.represent_mapping(u'tag:yaml.org,2002:map', data.items(), flow_style=False)
		yaml.add_representer(dict, blockseq_rep)
		return yaml.dump(self.data, default_flow_style=False)

	def put(self, xid, data):
		return ConnectITTransaction(self, data=self.session.request('PUT', f"/views/{self.id}/objects/{xid}", data=data).json())

	# Fixme: Fetches all transactions, even when a specific one is
	# requested
	@property
	def transactions(self):
		return {item['link'].split("/")[-1]:ConnectITTransaction(self, item) for item in self.session.request('GET', f"/views/{self.id}/transactions/").json()}

	def create(self):
		self.session.request('POST', '/views/', data=self.data)

	def delete(self):
		self.session.request('DELETE', f"/views/{self.data['id']}")

class ConnectITTransaction(object):
	def __init__(self, view, data):
		self._static_attr = ['link', 'object', 'requestedAt']
		self.view = view
		self.data = data

	@property
	def id(self):
		return self.data['link'].split("/")[-1]

	def refresh(self):
		self.data = self.view.session.request('GET', self.data['link']).json()

	# Fixme: Refreshes even when a static attribute is requested
	def __getattr__(self, name):
		if name not in self._static_attr:
			self.refresh()
		try:
			return self.data[name]
		except KeyError:
			raise AttributeError

	def __repr__(self):
		cls = type(self)
		myid = self.id
		return f"<{cls.__name__} '{myid}'>"

	@property
	def json(self):
		return json.dumps(self.data, sort_keys=True, indent=4, separators=(',', ': '))

	@property
	def yaml(self):
		def blockseq_rep(dumper, data):
			return dumper.represent_mapping(u'tag:yaml.org,2002:map', data.items(), flow_style=False)
		yaml.add_representer(dict, blockseq_rep)
		return yaml.dump(self.data, default_flow_style=False)
