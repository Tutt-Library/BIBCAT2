
__author__ = "Jeremy Nelson"

from flask import Flask
from catalog import catalog

app = Flask(__name__, instance_relative_config=True)
app.config.from_pyfile('config.py')
app.register_blueprint(catalog)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

