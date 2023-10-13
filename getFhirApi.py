# -*- coding: utf-8 -*-
import requests
# stdlib
import bunch
from json import dumps,loads
# Zato
from zato.server.service import Service

class GetFhirApi(Service):
	name = "alkuip.api.get.fhir.api"
	class SimpleIO:
		input_optional = ('resourceType','fields')
	# Retrieve data from FHIR server
	def handle_GET(self):
		inputParams = {'resourceType': 'metadata' }
		# make request
		req = self.outgoing.plain_http.get('HAPI FHIR R4')	
		if self.request.input.resourceType is not None:
			inputParams = {'resourceType': self.request.input.resourceType }
			response = req.conn.get(self.cid,params=inputParams)
			self.response.payload= bunch.bunchify(response.text)
		else:
			response = req.conn.get(self.cid,params=inputParams)
			self.response.payload= bunch.bunchify(response.text)
	# POST data to FHIR Server
	def handle_POST(self):
		inputParams = {'resourceType': '' }
		# make request
		preq = self.outgoing.plain_http.get('DHIS2 API POST')		
		if self.request.input.resourceType is not None:
			inputParams = {'resourceType': self.request.input.resourceType }
			presponse = preq.conn.get(self.cid,params=inputParams)
			self.response.payload = bunch.bunchify(presponse.text)
		else:
			pdata = self.request.payload
			presponse = preq.conn.post(self.cid,data=pdata,params=inputParams)
			# print the result		
			self.response.payload = bunch.bunchify(presponse.text)


