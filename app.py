#!/usr/bin/env python

from flask import Flask
from flask_restfull import REscource, Api

app = Flask(__name__)
api = Api(app)

class Dummy(Resources):
    def get(self):
        return {'message': 'This is only a dummy setup'}


api.add_resource(Dummy, '/')


app.run()