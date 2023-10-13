# -*- coding: utf-8 -*-
import requests
# stdlib
import bunch
from json import dumps,loads
# Zato
from zato.server.service import Service

class GetMetadataApi(Service):
	name = "alkuip.api.get.metadata"
	class SimpleIO:
		input_optional = ('type','fields')
	def handle_GET(self):
		inputParams = {'resource': 'dataValueSets' }
		# make request
		req = self.outgoing.plain_http.get('DHIS2 API')	
		if self.request.input.type == 'terminology':
			if self.request.input.fields == '':
				inputParams = {'resource': 'dataElements', 'fields': 'id,name,code,valueType,formName' }
			else:
				inputParams = {'resource': 'dataElements', 'fields': self.request.input.fields }
			response = req.conn.get(self.cid,params=inputParams)
			self.response.payload = self.mapDhis2ToFhir(data=loads(response.text),type="dataElements",req=req)
		elif self.request.input.type == 'location':
			if self.request.input.fields == '':
				inputParams = {'resource': 'organisationUnits', 'fields': 'id,name,code,openingDate,closingDate,created,lastUpdated' }
			else:
				inputParams = {'resource': 'organisationUnits', 'fields': self.request.input.fields }
			response = req.conn.get(self.cid,params=inputParams)
			self.response.payload = self.mapDhis2ToFhir(data=loads(response.text),type="organisationUnits",req=req)

		else:
			response = req.conn.get(self.cid,params=inputParams)
			self.response.payload= bunch.bunchify(response.text)
    # Map DHIS2 to FHIR standard
	def mapDhis2ToFhir(self,data=None,type="organisationUnits",req=None):
		apiConfig = req['config']
		mappings = {}
		mappedData  = []
		if data is None:
			pass
		else:
			if type in data:
				for d in data[type]:
					identifier = []
					for k,v in d.items():
						if k == "id":
							identifier.append({
								"value" : v,
								"property" : k,
								"system" : f"{apiConfig['host']}/api/{type}/{v}",
								"type" : "dhis2"
							})
						elif k == "name" and "id" in d:
							identifier.append({
								"value" : v,
								"property" : k,
								"system" : f"{apiConfig['host']}/api/{type}/{ d['id']}?fields=id,name&filter=name:eq:{v}",
								"type" : "dhis2"
							})
						elif k == "code" and "id" in d:
							identifier.append({
								"value" : v,
								"property" : k,
								"system" : f"{apiConfig['host']}/api/{type}/{ d['id']}?fields=id,code&filter=code:eq:{v}",
								"type" : "dhis2"
							})
						else:
							pass
					mappedData.append({ 
						"identifier": list(identifier)
					})
			mappings["data"] = mappedData
		return mappings


	# Re-Map data to DHIS2 format
	# Switch this to Pandas
	def remap(self,data):
		if hasattr(data,'locationId'):
			data['orgUnit'] = data['locationId']
			del data['locationId']
		elif hasattr(data,'attribution'):
			data['attributeOptionCombo'] = data['attribution']
			del data['attribution']
		elif hasattr(data,'id'):
			data['dataElement'] = data['id']
			del data['id']
		elif hasattr(data,'disaggregation'):
			data['categoryOptionCombo'] = data['disaggregation']
			del data['disaggregation']
		else:
			pass
		return data
	# POST data to Data Warehouse
	def handle_POST(self):
		inputParams = {'resource': 'dataValueSets' }
		# make request
		preq = self.outgoing.plain_http.get('DHIS2 API POST')		
		if self.request.input.type == 'terminology':
			inputParams = {'resource': 'dataElements'}
			presponse = preq.conn.get(self.cid,params=inputParams)
			self.response.payload = bunch.bunchify(presponse.text)
		else:
			pdata = self.request.payload
			if 'dataValues' in pdata:
				pdata['dataValues'] = list(map(self.remap,pdata['dataValues']))
			else:
				pass
			presponse = preq.conn.post(self.cid,data=pdata,params=inputParams)
			# print the result		
			self.response.payload = bunch.bunchify(presponse.text)


