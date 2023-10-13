# -*- coding: utf-8 -*-
# Facility Registry Service
# Creates facility registry from systems delegated as Master based on openHIE architecture
#

from __future__ import absolute_import, division, print_function, unicode_literals
# stdlib
import json,bunch,uuid
import riak,pandas as pd, jellyfish as jf

from urllib.parse import urlparse,parse_qs
# Custom services
#import database
# Zato
from zato.server.service import Service

class CreateFacilityRegistry(Service):
    """ Creates Master Facility Registry endpoint.
    """
    class SimpleIO:
        output_optional = ('organisationUnits')
        input_required = ('dataType','serviceType')

    def handle(self):

        # Python dict representing the payload we want to send across
        payload = {}
        # Create/update configuration
        config = {"supportedSystems":[{"name":"DHIS2","platform":"DHIS2","description":"Use for HMIS"},{"name":"openMRS","platform":"openMRS","description":"Use for EMR"},{"name":"openClinic","platform":"openClinic","description":"Use for EMR"},{"name":"SIDAInfo","platform":"SIDAInfo","description":"Use for HIV tracking"}],
        "availableSystems":[{"id":1,"url":"","name":"HMIS","platform":"DHIS2","description":"Use for HMIS"}],"services":[{"name":"FR","master":"HMIS"},{"name":"TS","master":"HMIS"}],"dhis2ids":{'targets':[{'dataSets':'AitXBHsC7RA'},{'dataSets':'BuRoS9i851o'},{'dataSets':'jEzgpBt5Icf'},{'dataSets':'Ir58H3zBKEC'},{'dataSets':'O8hSwgCbepv'},{'dataSets':'pTuDWXzkAkJ'},{'dataSets':'c7Gwzm5w9DE'},{'dataSets':'YWZrOj5KS1c'},{'dataSets':'bqiB5G6qgzn'},{'dataSets':'AvmGbcurn4K'}],'results':[{'dataSets':'uTvHPA1zqzi'},{'dataSets':'O3VMNi8EZSV'},{'dataSets':'asptuHohmHt'},{'dataSets':'Ir58H3zBKEC'},{'dataSets':'kuV7OezWYUj'},{'dataSets':'f78MvV1k0rA'},{'dataSets':'jTRF4LdklYA'},{'dataSets':'bQTvQq3pDEK'},{'dataSets':'tFBgL95CRtN'},{'dataSets':'zNMzQjXhX7w'}]}}

        # Python dict with all the query parameters, including path and query string
        params = {}
        # Metadata mapping
        tsMappings ={
            "types":[{"id":1,"name":"disaggregation"},{"id":2,"name":"indicator"},{"id":3,"name":"attribute"}],
            "groups":[{"id":1,"group":"ART","type":"indicator","category":"HIV","system":1},{"id":2,"group":"HTS_TST","type":"indicator","category":"HIV","system":1}
          ],
            "mappings":[]
        }

        # Headers the endpoint expects
        headers = {'X-Service': 'Facility Registry', 'X-Version':'Production'}

        # Obtains a DHIS2 connection object
        frsConnection = self.outgoing.plain_http.get('FacilityRegistryService')
        datConnection = self.outgoing.plain_http.get('DATIMPublicCodeList')

        ## Get connection to Riak Database
        riak_frs = self.connect('dhis2.jsi.com',8087,'integration')
        #riak_frs = self.connect('192.168.106.128',8087,'integration')
        # save configuration to database
        self.saveItem(riak_frs,'config',config)
        riak_configs = self.getItems(riak_frs,'config')
        self.saveItem(riak_frs,'tsMappings',tsMappings)
        # Invoke the resystem providing all the information on input
        # response = frsConnection.conn.post(self.cid, payload, params, headers=headers)
        qs =parse_qs(self.wsgi_environ['QUERY_STRING'])
        serviceType = qs['serviceType']
        dataType = qs['dataType']

        qsx =parse_qs(self.wsgi_environ['QUERY_STRING'])
        serviceTypex = qsx['serviceType']
        if serviceTypex[0] == 'frs':
            params = {'apiVersion':'29','resourceName': 'organisationUnits','fields':'id,code,name,openingDate,closedDate,coordinates,ancestors[level,id,code,name],organisationUnitGroups[id,code,name,groupSets[id,code,name]]'}
            response = frsConnection.conn.get(self.cid,params=params)
            organisationUnits = self.createOrgUnitList(json.loads(response.text))
            self.saveItem(riak_frs,'organisationUnits',organisationUnits)
            riak_data = self.getItems(riak_frs,'organisationUnits')
            self.response.payload =  bunch.bunchify(riak_data)
        elif serviceTypex[0] == 'ts':
            params = {'apiVersion':'29','resourceName': 'dataElements','fields':'id,code,name,dataElementGroups[id,name]'}
            response = frsConnection.conn.get(self.cid,params=params)
            tsNewMappings = bunch.bunchify(response.text)
            riak_mappings = self.getItems(riak_frs,'tsMappings')
            if len(riak_mappings['mappings']) == 0:
                tsMappings['mappings'] = tsNewMappings
                self.saveItem(riak_frs,'tsMappings',tsMappings)
            else:
                # Check if an element exist
                nonExistingElements = []
                for el in tsNewMappings['dataElements']:
                    elemExist = self.elementExists(el,tsNewMappings['dataElements'])
                    if not elemExist:
                        nonExistingElements.append(el)
                riak_mappings['mappings'].extend(nonExistingElements)
                ### manipulate data elements for Terminology Service
                tsParams = riak_configs['dhis2ids']
                datimPublicElements = getDATIMDataElements(tsParams['results'])
                riak_mappings['mappings'].extend(datimPublicElements)
                self.saveItem(riak_frs,'terminology',riak_mappings)
            riak_data = self.getItems(riak_frs,'tsMappings')
            self.response.payload =  bunch.bunchify(riak_data)
        else:
            #pass
            #self.logger.info(organisationUnits)
            response = frsConnection.conn.get(self.cid,params=params)
            self.response.payload = []
        self.logger.info(self.response.payload)
        self.response.content_type='application/json'


    def createOrgUnitList(self,orgunits):
        ouList = {}
        orgUnitList = []
        ouList['pager'] = orgunits['pager']
        if len(orgunits['organisationUnits']) > 0:
            for ou in orgunits['organisationUnits']:
                orgUnit = {}
                orgUnit['systemid'] = str(uuid.uuid4())
                orgUnit['identifier'] = [{"value":"","type":"code","system":"DHIS2"},{"value":ou['name'],"type":"name","system":"DHIS2"},{"value":ou['id'],"type":"id","system":"DHIS2"}]
                orgUnit['administrativeUnits'] = []
                orgUnit['endpoint'] = []
                orgUnit['authority'] = [{"value":"","type":"ownership"}]
                orgUnit['status'] = [{"value":"","type":"regulatory"},{"value":"","type":"operating"},{"value":"","type":"validity"},{"value":"","type":"activity"}]
                orgUnit['location'] = {"current":[{"value":"","type":"latitude","system":"nbs"},{"value":"","type":"longitude","system":"nbs"},{"value":"","type":"address","system":"nbs"},{"value":"","type":"telecom","system":"nbs"}],"historic":[{"value":"","type":"latitude","system":"nbs"},{"value":"","type":"longitude","system":"nbs"},{"value":"","type":"address","system":"nbs"},{"value":"","type":"telecom","system":"nbs"}]}
                orgUnit['population'] = [{"value":"","type":"catchment","system":"nbs","period":"2004"}]
                orgUnit['tracking'] = [{"value":"","type":"openingDate"},{"value":"","type":"closingDate"},{"value":"","type":"maintenanceDate"},{"value":"","type":"nextMaintenanceDate"}]
                orgUnitGroupInSets = []
                for key,val in ou.items():
                    if key == 'ancestors' and len(ou['ancestors']) > 0:
                        for ancestor in ou['ancestors']:
                            admin = {}
                            admin = {"type":"level","value":ancestor['level'],"identifier":[]}
                            for k,v in ancestor.items():
                                adminUnit = {}
                                if k is not 'level':
                                    adminUnit={}
                                    adminUnit['type']=k
                                    adminUnit['value']=ancestor[k]
                                    adminUnit['system'] ="DHIS2"
                                    admin['identifier'].append(adminUnit)
                            orgUnit['administrativeUnits'].append(admin)
                    elif key == 'organisationUnitGroups' and len(ou['organisationUnitGroups']) > 0:
                        for ouGroup in ou['organisationUnitGroups']:
                            if key == 'groupSets' and len(ou['groupSets']) > 0:
                                for ouGroupSet in ou['groupSets']:
                                    for ogk,ogv in ouGroupSet.items():
                                        ogGroupUnit = {}
                                        ogGroupUnit['type'] = ogk
                                        ogGroupUnit['value'] = ogv
                                        ogGroupUnit['system']='DHIS2'
                                        orgUnit['authority'].append(ogGroupUnit)
                                    orgUnit[ouGroupSet['name']] = ouGroup['name']
                                    orgUnitGroupInSets.append(ouGroup['name'])
                            else:
                                #orgUnit[ouGroup['name']] = 'Yes'
                                ougGroupUnit = {"identifier":[]}
                                for ougk, ougv in ouGroup.items():
                                    if ougk == 'name':
                                        ougGroupUnit['type'] = ougv
                                        ougGroupUnit['value'] = 'Yes'
                                    elif ougk == 'id':
                                        ougGroupUnit['identifier'].append({'type':ougk,'value': ougv,'system':'DHIS2'})
                                    elif ougk == 'code':
                                        ougGroupUnit['identifier'].append({'type':ougk,'value': ougv,'system':'DHIS2'})
                                    else:
                                        pass
                                ougGroupUnit['system']='DHIS2'
                                orgUnit['authority'].append(ougGroupUnit)
                    else:
                        orgUnit[key] = val
                orgUnitList.append(orgUnit)
        ouList['organisationUnits'] = sorted(orgUnitList)
        return ouList
    #Riak Routines
    def connect(self,host, port, bucket):
        self.logger.info("Connecting to Riak Database")
        client = riak.RiakClient(host=host, pb_port=port, protocol='pbc')
        current_bucket = client.bucket(bucket)
        self.logger.info("Connected to the bucket "+ bucket)
        return current_bucket
    def saveItem(self,client,item,items):
        newItem = client.new(item, data=items)
        newItem.store()
        return "Saved"
    def getItems(self,client,items):
        newItem = client.get(items)
        retrievedItem = newItem.data
        return retrievedItem
    def updateItems(self,client,items,itemKey,updateItem):
        newItem = client.get(items)
        newItem.data[itemKey] = updateItem
        newItem.store()

    def sendToRabbitMQ(self,message,sender,reciever):
        print("Sending messages to RabbitMQ")

    def recieveFromRabbitMQ(self,message,sender,reciever):
        print("Recieving messages from RabiitMQ")
    def checkJaroDistance(self,left,right):
        match = (jf.jaro_distance(left,right) * 100)
        fullMatch = False
        if match == 100:
            fullMatch = True
        matchedObject = {"matchValue":match,"matched":fullMatch,"left":left,"right":right}
        return matchedObject

    def createMappings(self,data,refdata,type):
        df = self.createDataFrame(data,type)
        valueMatch = {}
        for index,d in df.iterrows():
            if len(refdata['mappings']) > 0:
                for refd in refdata['mappings']:
                    #valueMatch = self.checkJaroDistance(d['dataelement'],refd['name'])
                    #if valueMatch['matched'] == "true":
                    #    d['matched'] = valueMatch['matched']
                    #    refdata.append(d['map'])
                    #else:
                    #    d['matched'] = valueMatch['matched']
                    refd['name'] = str(d['dataelement'])
                    refd['shortname'] = str(d['shortname'])
                    refd['type'] = 1
                    refd['system'] =1
                    refd['group']= 1
                    refdata['mappings'].append(refd)
                    #refdata.append(d['map'])
            else:
                refd = {}
                #valueMatch = self.checkJaroDistance(d['dataelement'],refd['name'])
                #if valueMatch['matched'] == "true":
                #    d['matched'] = valueMatch['matched']
                #    refdata.append(d['map'])
                #else:
                #    d['matched'] = valueMatch['matched']
                refd['name'] = str(d['dataelement'])
                refd['shortname'] = str(d['shortname'])
                refd['type'] = 1
                refd['system'] =1
                refd['group']= 1
                refdata['mappings'].append(refd)
                #refdata.append(d['map'])
        return refdata
    # Read in Panda file
    def getPdFile(self,fileName,type):
        df = []
        if type == 'csv':
            df = pd.read_csv(fileName,encoding='utf-8')
        elif type == 'json':
            df= pd.read_json(fileName,orient=table)
        else:
            pass
        return df
    # Get DATIM data elements from public repository
    def getDATIMDataElements(self,datasets):
        codes = []
        for das in datasets:
            ds = {'var':'dataSets:'+ das['dataSets']}
            tsResponse = datConnection.conn.get(self.cid,params=ds)
            codes.extend(self.createMappings(json.loads(tsResponse.text),riak_mappings,'None'))
        return codes
    # Check if an element exists
    def elementExists(self,id,values):
        if len(values) > 0:
            for elem in values:
                if elem == id:
                    return True
            return False
        return False

    # create Panda Data Frame from event data
    def createDataFrame(self,events,type):
        cols = self.createColumns(events['headers'],type)
        dataFrame = pd.DataFrame.from_records(events['rows'],columns=cols)
        return dataFrame
    def createColumns(self,data,type):
        cols =[]
        if type == 'AGGREGATE':
            pass
        else:
            for dt in data:
                cols.append(dt['name'])
        return cols
