from flask import Flask
from werkzeug.contrib.cache import MemcachedCache
from hbase_model import HBase

app = Flask(__name__)
app.config.from_object('amazonbies.default_settings')
app.config.from_pyfile('application.cfg', silent=True)

cache = MemcachedCache(["127.0.0.1:11211"])
hbase = HBase(app)

import amazonbies.views

