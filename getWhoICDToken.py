# -*- coding: utf-8 -*-
import requests
# stdlib
import json,bunch
# Zato
from zato.server.service import Service

class GetICD11Token(Service):
	class SimpleIO:
		input_required = ('code')
	def handle_GET(self):
		#Get Auth token
		#token_endpoint = 'https://icdaccessmanagement.who.int/connect/token'
		client_id = ''
		client_secret = ''
		scope = 'icdapi_access'
		grant_type = 'client_credentials'


		# get the OAUTH2 token

		# set data to post
		payload = {'client_id': client_id,
			   	   'client_secret': client_secret,
		           'scope': scope,
		           'grant_type': grant_type}

		# make request
		tokReq = self.outgoing.plain_http.get('WHO ICD API Token')
		tokRes =  tokReq.conn.post(self.cid, data=payload)
		
		tokenStr = json.loads(tokRes.text)
		token=tokenStr['access_token']


		# access ICD API
		#uri = 'https://id.who.int/icd/release/11/2019-04/mms/search'
		req = self.outgoing.plain_http.get('WHO ICD API')

		# HTTP header fields to set
		headers = {'Authorization':  'Bearer '+token,
		           'Accept': 'application/json',
		           'Accept-Language': 'en','API-Version':'v2'}

		# make request
		searchQuery = self.request.input.code+'%'
		r = req.conn.get(self.cid, headers=headers, params={'q':searchQuery,'propertiesToBeSearched':'Title,Code,Synonym,indexTerm','useFlexisearch':'false','flatResults':'true'})

		# print the result

		self.response.payload = r.text
