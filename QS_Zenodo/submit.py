#!/usr/bin/python
# -*- coding: utf-8 -*-
import zenodorequest

from zenodorequest import *
from bottle import route, run, request, response

""" enable cross domain ajax requests when using json of another domain"""
def enable_cors(fn):
    def _enable_cors(*args, **kwargs):
        # set CORS headers
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'

        if request.method != 'OPTIONS':
            # actual request; reply with the actual response
            return fn(*args, **kwargs)

    return _enable_cors

@route('/submit', method='GET')
@enable_cors
def readjson():
    """ read the json file from the url"""
    uuid = request.query.get('uuid','')
    if(uuid == ""):
        result = { "code":"fail", "message":"empty uuid"}
        return result
    else:
        zenodo = ZenodoRequest(uuid)
        return {'data':zenodo.saveInDatabase()}

""" to launch local server"""    
"""run(host='localhost', port=8084, debug=True)"""
