from flask import Flask

app = Flask(__name__)
app.config.from_object('amazonbies.default_settings')
app.config.from_pyfile('application.cfg', silent=True)

import amazonbies.views

