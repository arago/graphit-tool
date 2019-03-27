import graphit, connect, requests
from configparser import ConfigParser
import errno
import os


class MyHTTPAdapter(requests.adapters.HTTPAdapter):
	def __init__(self, timeout=None, *args, **kwargs):
		self.timeout = timeout
		super(MyHTTPAdapter, self).__init__(*args, **kwargs)

	def send(self, *args, **kwargs):
		kwargs['timeout'] = self.timeout
		return super(MyHTTPAdapter, self).send(*args, **kwargs)


def get_session(config, timeout=60, cls=graphit.GraphitSession):
	session = cls(
		config['graph']['api_endpoint'],
		auth=graphit.GraphitAppAuth(
			config['graph']['api_endpoint'],
			client_id=config['graph']['auth_client_id'],
			client_secret=config['graph']['auth_client_secret'],
			username=config['graph']['auth_username'],
			password=config['graph']['auth_password'],
			verify=False),
		verify=False)
	session.mount("https://", MyHTTPAdapter(timeout=(30, timeout)))
	return session


def get_config(path):
	config = ConfigParser()
	r = config.read(path)
	if len(r) == 0:
		raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
	return config

def set_debug_level(lvl):
	os.environ['GT_DEBUG'] = str(lvl)
