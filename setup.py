#!/usr/bin/env python2
import os
if os.environ.get('USER','') == 'vagrant':
    del os.link

import distutils.core

name = 'graphit-tool'

distutils.core.setup(name=name,
    version='0.1.1',
    author="Marcus Klemm",
    author_email="mklemm@arago.de",
    url="https://arago.co",
    description="Tool to manipulate GraphIT",
    long_description="Tool to manipulate GraphIT",

    scripts=['graphit-tool.py'],
    py_modules=['graphit'],
    data_files=[('/etc', ['conf/graphit-tool.conf']), ('/usr/share/graphit-tool', ['data/MODEL_default.xsd'])]
)
