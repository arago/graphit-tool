class t(object):
	def __init__(self, name):
		self.name = name
	def __repr__(self):
		return self.name

TYPE = t('TYPE')
ATTR = t('ATTR')
SUBT = t('SUBT')

ALLT = ['root', 'mandatory', 'optional', 'many']
ROOT = ['root']
MISC = ['mandatory', 'optional', 'many']

def get_key_type(key, attributes, other_entites):
	if key == '$type':
		return TYPE
	elif key in attributes:
		return ATTR
	elif key in other_entites:
		return SUBT
	else:
		raise NotImplementedError

def find_key(mydict, key):
    return list(mydict.keys())[list(mydict.values()).index(key)]

def get_attributes(entity_definition):
	return entity_definition['attributes'].values() if 'attributes' in entity_definition else []

def get_entity_names(sdf_definition, types=ALLT):
	return [ent for ent, defi in sdf_definition['entities'].items() if defi['presence'] in types]

def get_entity_definition(name, sdf_definition):
	return sdf_definition['entities'][name]

def get_sdf_definition(sdftype, sdf):
	return [item['sdf'] for item in sdf['sdfDefinition'] if item['sdf']['id'] == sdftype][0]

def get_message_type(message):
	if '$type' in message:
		return message['$type']
	else:
		raise NotImplementedError


class DomainObject(object):
	def __init__(self, xid, data, sdf, sdf_definition, entity_name):
		self._data = data
		self._sdf = sdf
		self._xid = f"{self._sdf['xidPrefix']}{xid}"
		self._sdf_definition = sdf_definition
		self._entity_name = entity_name
		self._entity_definition = get_entity_definition(self._entity_name, self._sdf_definition)

	@property
	def xid(self):
		return self._xid

	@classmethod
	def from_root(cls, xid, data, sdf):
		sdftype = get_message_type(data)
		sdf_definition = get_sdf_definition(sdftype, sdf)
		entity_name = get_entity_names(sdf_definition, types=ROOT)[0]
		return cls(xid, data, sdf, sdf_definition, entity_name)

	@property
	def type(self):
		return {'ogit/_type': self._sdf_definition['entities'][self._entity_name]['type']}

	@property
	def attributes(self):
		def entity_def(name):
			return find_key(self._entity_definition['attributes'], name)
		return {entity_def(name):self._data[name] for name in get_attributes(self._entity_definition) if name in self._data}

	@property
	def constants(self):
		if 'constants' in self._sdf_definition['entities'][self._entity_name]:
			return self._sdf_definition['entities'][self._entity_name]['constants']
		else:
			return {}

	@property
	def subitems(self):
		def entity_def(name):
			if self._sdf_definition['entities'][name]['type'].startswith('$'):
				entity_type = self._sdf_definition['entities'][name]['type'][1:]
				return get_sdf_definition(entity_type, self._sdf)
			else:
				return self._sdf_definition
		other_entities = get_entity_names(self._sdf_definition, types=MISC)
		attributes = get_attributes(self._entity_definition)
		subitems = [key for key in self._data.keys() if get_key_type(key, attributes, other_entities) == SUBT]
		r = {}
		for subitem in subitems:
			for name, data in self._data[subitem].items():
				subitem_sdf_def = entity_def(subitem)
				if subitem in subitem_sdf_def['entities']:
					subitem_ent_name = subitem
				elif subitem in self._sdf_definition['entities']:
					subitem_ent_name = get_entity_names(subitem_sdf_def, types=ROOT)[0]
				#print("SUBTYPE:", subitem, "==>", subitem_sdf_def['id'], "/", subitem_ent_name)
				r[name] = DomainObject(name, data, self._sdf, subitem_sdf_def, subitem_ent_name)
		return r

	def vertices(self):
		yield self._xid, {**self.type, **self.attributes, **self.constants, **{'ogit/_xid': self._xid}}
		for subxid, subitem in self.subitems.items():
			for xid, v in subitem.vertices():
				yield xid, v

	def edges(self):
		for ent in get_entity_names(self._sdf_definition, types=MISC):
			if ent in self._data:
				for xid, item in self._data[ent].items():
					for verb in self._sdf_definition['verbs']:
						if verb['to'][1:] == ent:
							yield verb['id'], self.subitems[xid], self
							for edge in self.subitems[xid].edges():
								yield edge
						elif verb['from'][1:] == ent:
							yield verb['id'], self, self.subitems[xid]
							for edge in self.subitems[xid].edges():
								yield edge

	def __str__(self):
		return self._xid

	def __repr__(self):
		return f"<DOBJ {self._entity_name} {self._xid}>"
