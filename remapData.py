# -*- coding: utf-8 -*-
import requests
# stdlib
import json,bunch
# Zato
from zato.server.service import Service

class RemapData(Service):
	class SimpleIO:
		input_optional = ('type')
	def handle_GET(self):
		inputParams = {'resource': 'organisationUnits','fields':'id,name,path','paging':'false' }
		inputDsParams = {'resource': 'dataStore/ugx_elmis/data' }
		# make request
		req = self.outgoing.plain_http.get('ELMIS CONN')		
		response = req.conn.get(self.cid,params=inputParams)
		responseDs = req.conn.get(self.cid,params=inputDsParams)
		result = bunch.bunchify(response.text)
		resultDs = bunch.bunchify(responseDs.text)
		finalData = self.remap(resultDs,result)
		self.response.payload= finalData
    # Re-Map data to DHIS2 format
	# Switch this to Pandas
	def remap(self,data,ous):
		newMap = []
		for d in json.loads(data):
			newMap = d
			if hasattr(d,'location'):
				for ou in json.loads(ous):
					if ou['id'] == d['location']['id']:
						d['location']['path'] = ou['path']
						#newMap.append(d)
					else:
						pass
			else:
				pass
		return newMap