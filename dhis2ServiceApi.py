# -*- coding: utf-8 -*-
# stdlib
from __future__ import absolute_import

import sys
import time
import bunch, json
import pandas as pd
#from pyspark.sql import SparkSession as ss
#from pyspark.conf import SparkConf
#from pyspark import SparkContext, SQLContext
#import pyspark.pandas as ps
#from pyspark.mllib.classification import NaiveBayes
#from pyspark.mllib.regression import LabeledPoint
#from pyspark.ml.feature import CountVectorizer
#from pyspark.ml.classification import RandomForestClassifier
#from pyspark.ml.feature import VectorAssembler
#from pyspark.ml.linalg import Vectors
#from pyspark.sql.functions import col
#from pyspark.sql import functions as F
#from pyspark.ml import Pipeline
#from pyspark.ml.classification import LogisticRegression
#from pyspark.ml.feature import HashingTF, Tokenizer
# Zato
from zato.server.service import Service
from kubernetes import client, config
import yaml
import json

from abc import ABCMeta, abstractproperty, abstractmethod


import base64
import cloudpickle
import os
import re
import requests
import threading
import traceback
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor
from future.moves.urllib.parse import ParseResult, urlparse
from io import open, StringIO
from requests_kerberos import HTTPKerberosAuth, REQUIRED
#from livy.job_handle import JobHandle
from concurrent.futures import Future
from threading import Timer

# Possible job states.
PENDING = 'PENDING'
RUNNING = 'RUNNING'
CANCELLED = 'CANCELLED'
FINISHED = 'FINISHED'
SENT = 'SENT'
QUEUED = 'QUEUED'


class Dhis2ServiceApi(Service):
	name='alkip.api.dhis2.service'
	sparkPipeline='alkip.api.apache.spark.service'
	class SimpleIO:
		input_required = ('system')        
		input_optional = ('type','fields','store')
	def handle_GET(self):
		inputParams = {'resource': 'dataValueSets'}
		# make request
		req = self.outgoing.plain_http.get('DHIS2 API')		
		if self.request.input.type == 'terminology':
			req['config']['address_host'] = self.request.input.system
			if self.request.input.fields == '':
				inputParams = {'resource': 'dataElements', 'fields': 'id,name,code,valueType,formName' }
			else:
				inputParams = {'resource': 'dataElements', 'fields': self.request.input.fields }
			response = req.conn.get(self.cid,params=inputParams)
			self.response.payload = bunch.bunchify(response.text)

		else:
			inputParams = { 'resource': 'dataValueSets','dataSet': self.request.input.dataSet,'orgUnit': self.request.input.orgUnit,'startDate': self.request.input.startDate,'endDate': self.request.input.endDate}
			response = req.conn.get(self.cid,params=inputParams)
			if self.request.input.store == 'true':
				dhisData = response.data
				etlData = {'data': json.loads(self.getDwData(data=dhisData['dataValues']))}
				responsex = self.invoke_async(self.sparkPipeline,etlData)
				self.logger.info('I also received1 %r',responsex)
				self.response.payload= etlData
			else:
				self.response.payload=  bunch.bunchify(response.text)
    # Re-Map data to DHIS2 format
	# Switch this to Pandas
	def remap(self,data):
		if hasattr(data,'location'):
			data['orgUnit'] = data['location']
			del data['location']
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
    # Re-Map data to DataWarehouse format
	# Switch this to Pandas
	def getDwData(self,data,source=None):
		df = pd.DataFrame(data)
		df.rename(inplace=True,columns={'orgUnit':'location','attributeOptionCombo':'attribution','categoryOptionCombo':'disaggregation'})
		return df.to_json(orient='records')
	# POST data to DHIS2
	def handle_POST(self):
		inputParams = {'resource': 'dataValueSets' }
		# make request
		preq = self.outgoing.plain_http.get('DHIS2 POST')		
		if self.request.input.type == 'terminology':
			preq['config']['host'] = self.request.input.system            
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
class Dhis2ServiceApiAsyncCallback(Service):
	def handle(self):
		self.logger.info('I also received %r', self.request.payload)
# Spark Cluster
class SparkServiceApi(Service):
        name = 'alkip.api.apache.spark.service'
        def simple_spark_job(self,context):
            elements =[10,20,30,40]
            return context.sc.parrallize(elements,2).count()
        def handle(self):   
            scReq = self.out.rest['SPARKLIVY']
            # Create Spark Session
            scRes = scReq.conn.post(self.cid,data=json.dumps({ 'kind': 'pyspark' }),params={ 'path': 'sessions'})
            # Check Spark session status
            self.logger.info('Waiting for session state to idle')
            #while state != 'idle':
            r = scReq.conn.get(self.cid,params={'path': scRes.headers['location'].lstrip('/')})
            self.logger.info('Session state:::::::::::::',r.data)
            state = r.data['state']
            time.sleep(1)
            self.logger.info('\rSession State is Ready:::::::',r.data)
            # Testing code
            r1 = scReq.conn.post(self.cid, data=json.dumps({'code': '1 + 1'}), params={ 'path': "{}/{}".format(scRes.headers['location'].lstrip('/'),'statements')})
            self.response.payload = r1.data
            time.sleep(100)
            print('=' * 80)
            self.logger.info('Starting Request:',r1)

            output = None
            #while output == None:
            r2 = scReq.conn.get(self.cid, params={ 'path': "{}/{}".format(scRes.headers['location'].lstrip('/'),'statements')})
            self.logger.info("Output:",r2)
            ret = r2.data
            if ret['output'] == None:
                time.sleep(1)
            if 'output' in ret and 'data' in ret['output']:
                output = ret['output']['data']['text/plain']

            print('-' * 80)
            self.logger.info("output:",output)
            # End Spark session
            print('=' * 80)
            r = scReq.conn.delete(self.cid,params={'path': scRes.headers['location']})
            print('{0} {1}'.format(r.data))
            # Prepare training documents from a list of (id, text, label) tuples.
            #training = spark.createDataFrame([
            #	(0, "1", 1.0),
            #	(1,"2", 0.0),
            #	(2, "4", 1.0),
            #	(3, "7", 0.0)
            #], ["id", "text", "label"])
            #training.show()
            # Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
            #tokenizer = Tokenizer(inputCol="text", outputCol="words")
            #hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
            #lr = LogisticRegression(maxIter=10, regParam=0.001)
            #pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

            # Fit the pipeline to training documents.
            #model = pipeline.fit(training)

            # Prepare test documents, which are unlabeled (id, text) tuples.
            #test = spark.createDataFrame([
            #	(4, "spark"),
            #	(5, "l m n"),
            #	(6, "spark hadoop spark"),
            #	(7, "apache hadoop")
            #], ["id", "text"])

            # Make predictions on test documents and print columns of interest.
            #prediction = model.transform(test)
            #selected = prediction.select("id", "text", "probability", "prediction")
            #dfjson= selected.pandas_api()
            self.response.payload = json.loads(output)
            #spark1.stop()


class Dhis2CreateUser(Service):
	name='alkip.api.dhis2.create.user.service'
	def handle_POST(self):
		userParams = { 'resource':'users'}
		userReq = self.outgoing.plain_http.get('DHIS2 API').conn
		userData = self.request.payload
		userDataPayload = {
			"firstName": userData['firstName'],
			"surname": userData['surname'],
			"email": userData['email'],
            "disabled": True,
			"userCredentials": {
				"username": userData['email'],
                "password": userData['password'],
                "disabled": True,
				"userRoles": [
					{
						"id": "CFrwLDsuDKb"
					},
					{
						"id": "yrB6vc5Ip3r"
					},
					{
						"id": "T2F7WDpdNeu"
					}
				]
			},
			"organisationUnits": [
				{
					"id": "akV6429SUqu"
				}
			],
			"userGroups": [
				{
					"id": "KiPQpbaCnhG"
				},
				{
					"id": "CcMROycPbw9"
				}
			]
		}
		userResponse = userReq.post(self.cid,data=userDataPayload,params=userParams)
		self.response.payload = userResponse.data
	def handle_OPTIONS(self):
		allow_from_name = "Access-Control-Allow-Origin"
		allow_from_value= "*"
		self.response.headers[allow_from_name] = allow_from_value
class Dhis2InviteUser(Service):
	name='alkip.api.dhis2.invite.user.service'
	def handle_POST(self):
		userParams = { 'resource':'users/invite'}
		userReq = self.outgoing.plain_http.get('DHIS2 API').conn
		userData = self.request.payload
		userDataPayload = {
			"firstName": userData['firstName'],
			"surname": userData['surname'],
			"email": userData['email'],
			"userCredentials": {
				"username": userData['email'],
				"userRoles": [
					{
						"id": "CFrwLDsuDKb"
					},
					{
						"id": "yrB6vc5Ip3r"
					},
					{
						"id": "T2F7WDpdNeu"
					}
				]
			},
			"organisationUnits": [
				{
					"id": "akV6429SUqu"
				}
			],
			"userGroups": [
				{
					"id": "KiPQpbaCnhG"
				},
				{
					"id": "CcMROycPbw9"
				}
			]
		}
		userResponse = userReq.post(self.cid,data=userDataPayload,params=userParams)
		self.response.payload = userResponse.data
	def handle_OPTIONS(self):
		allow_from_name = "Access-Control-Allow-Origin"
		allow_from_value= "*"
		self.response.headers[allow_from_name] = allow_from_value


# Create Http Client for LIVY                
class HttpClient(object):
    """A http based client for submitting Spark-based jobs to a Livy backend.

    Parameters
    ----------
    url_str : string
        Livy server url to create a new session or the url of an existing
        session
    load_defaults : boolean, optional
        This parameter decides if the default config needs to be loaded
        Default is True
    conf_dict : dict, optional
        The key-value pairs in the conf_dict will be loaded to the config
        Default is None

    Examples
    --------
    Imports needed to create an instance of HttpClient
    >>> from livy.client import HttpClient

    1) Creates a client that is loaded with default config
       as 'load_defaults' is True by default
    >>> client = HttpClient("http://example:8998/")

    2) Creates a client that does not load default config, but loads
       config that are passed in 'config_dict'
    >>> config_dict = {'spark.app.name', 'Test App'}
    >>> client = HttpClient("http://example:8998/", load_defaults=False,
    >>>    config_dict=config_dict)

    """

    _CONFIG_SECTION = 'env'
    _LIVY_CLIENT_CONF_DIR = "LIVY_CLIENT_CONF_DIR"

    def __init__(self, url, load_defaults=True, conf_dict=None):
        uri = urlparse(url)
        self._config = ConfigParser()
        self._load_config(load_defaults, conf_dict)
        self._job_type = 'pyspark'
        match = re.match(r'(.*)/sessions/([0-9]+)', uri.path)
        if match:
            base = ParseResult(scheme=uri.scheme, netloc=uri.netloc,
                path=match.group(1), params=uri.params, query=uri.query,
                fragment=uri.fragment)
            self._set_uri(base)
            self._conn = _LivyConnection(base, self._config)
            self._session_id = int(match.group(2))
            self._reconnect_to_existing_session()
        else:
            self._set_uri(uri)
            session_conf_dict = dict(self._config.items(self._CONFIG_SECTION))
            self.logger.info("CCC:",session_conf_dict,"ddffff:",self._config.items(self._CONFIG_SECTION))
            self._conn = _LivyConnection(uri, self._config)
            self._session_id = self._create_new_session(
                session_conf_dict).json()['id']
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._stopped = False
        self.lock = threading.Lock()

    def submit(self, job):
        """
        Submits a job for execution to the spark cluster.

        Parameters
        ----------
        job : function
            The function must accept a single parameter, which is an instance
            of JobContext.

        Returns
        -------
        job_handle : an instance of the class JobHandle
            A handle that can be used to monitor the job

        Examples
        -------
        >>> def simple_spark_job(context):
        >>>     elements = [10, 20, 30, 40, 50]
        >>>     return context.sc.parallelize(elements, 2).count()

        >>> client.submit(simple_spark_job)

        """
        return self._send_job('submit-job', job)

    def run(self, job):
        """
        Asks the remote context to run a job immediately.

        Normally, the remote context will queue jobs and execute them based on
        how many worker threads have been configured. This method will run
        the submitted job in the same thread processing the RPC message,
        so that queueing does not apply.

        It's recommended that this method only be used to run code that
        finishes quickly. This avoids interfering with the normal operation
        of the context.

        Parameters
        ----------
        job : function
            The function must accept a single parameter, which is an instance
            of JobContext. Spark jobs can be created with the help of
            JobContext, which exposes the Spark libraries.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> def simple_job(context):
        >>>     return "hello"

        >>> client.run(simple_job)
        """
        return self._send_job("run-job", job)

    def add_file(self, file_uri):
        """
        Adds a file to the running remote context.

        Note that the URL should be reachable by the Spark driver process. If
        running the driver in cluster mode, it may reside on a different
        host, meaning "file:" URLs have to exist on that node (and not on
        the client machine).

        Parameters
        ----------
        file_uri : string
            String representation of the uri that points to the location
            of the file

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.add_file("file:/test_add.txt")

        >>> # Example job using the file added using add_file function
        >>> def add_file_job(context):
        >>>    from pyspark import SparkFiles
        >>>    def func(iterator):
        >>>        with open(SparkFiles.get("test_add.txt")) as testFile:
        >>>        fileVal = int(testFile.readline())
        >>>        return [x * fileVal for x in iterator]
        >>>    return context.sc.parallelize([1, 2, 3, 4])
        >>>        .mapPartitions(func).collect()

        >>> client.submit(add_file_job)
        """
        return self._add_file_or_pyfile_job("add-file", file_uri)

    def add_jar(self, file_uri):
        """
        Adds a jar file to the running remote context.

        Note that the URL should be reachable by the Spark driver process. If
        running the driver  in cluster mode, it may reside on a different host,
        meaning "file:" URLs have to exist on that node (and not on the
        client machine).

        Parameters
        ----------
        file_uri : string
            String representation of the uri that points to the location
            of the file

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.add_jar("file:/test_package.jar")

        """
        return self._add_file_or_pyfile_job("add-jar", file_uri)

    def add_pyfile(self, file_uri):
        """
        Adds a .py or .zip to the running remote context.

        Note that the URL should be reachable by the Spark driver process. If
        running the driver  in cluster mode, it may reside on a different host,
        meaning "file:" URLs have to exist on that node (and not on the
        client machine).

        Parameters
        ----------
        file_uri : string
            String representation of the uri that points to the location
            of the file

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.add_pyfile("file:/test_package.egg")

        >>> # Example job using the file added using add_pyfile function
        >>> def add_pyfile_job(context):
        >>>    # Importing module from test_package.egg
        >>>    from test.pyfile_test import TestClass
        >>>    test_class = TestClass()
        >>>    return test_class.say_hello()

        >>> client.submit(add_pyfile_job)
        """
        return self._add_file_or_pyfile_job("add-pyfile", file_uri)

    def upload_file(self, file_path):
        """
        Upload a file to be passed to the Spark application.

        Parameters
        ----------
        file_path : string
            File path of the local file to be uploaded.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.upload_file("/test_upload.txt")

        >>> # Example job using the file uploaded using upload_file function
        >>> def upload_file_job(context):
        >>>    from pyspark import SparkFiles
        >>>    def func(iterator):
        >>>        with open(SparkFiles.get("test_upload.txt")) as testFile:
        >>>        fileVal = int(testFile.readline())
        >>>        return [x * fileVal for x in iterator]
        >>>    return context.sc.parallelize([1, 2, 3, 4])
        >>>        .mapPartitions(func).collect()

        >>> client.submit(add_file_job)
        """
        return self._upload_file_or_pyfile("upload-file",
            open(file_path, 'rb'))

    def upload_pyfile(self, file_path):
        """
        Upload a .py or .zip dependency to be passed to the Spark application.

        Parameters
        ----------
        file_path : string
            File path of the local file to be uploaded.

        Returns
        -------
        future : concurrent.futures.Future
            A future to monitor the status of the job

        Examples
        -------
        >>> client.upload_pyfile("/test_package.egg")

        >>> # Example job using the file uploaded using upload_pyfile function
        >>> def upload_pyfile_job(context):
        >>>    # Importing module from test_package.egg
        >>>    from test.pyfile_test import TestClass
        >>>    test_class = TestClass()
        >>>    return test_class.say_hello()

        >>> client.submit(upload_pyfile_job)
        """
        return self._upload_file_or_pyfile("upload-pyfile",
            open(file_path, 'rb'))

    def stop(self, shutdown_context):
        """
        Stops the remote context.
        The function will return immediately and will not wait for the pending
        jobs to get completed

        Parameters
        ----------
        shutdown_context : Boolean
            Whether to shutdown the underlying Spark context. If false, the
            context will keep running and it's still possible to send commands
            to it, if the backend being used supports it.
        """
        with self.lock:
            if not self._stopped:
                self._executor.shutdown(wait=False)
                try:
                    if shutdown_context:
                        session_uri = "/" + str(self._session_id)
                        headers = {'X-Requested-By': 'livy'}
                        self._conn.send_request("DELETE", session_uri,
                            headers=headers)
                except Exception:
                    raise Exception(traceback.format_exc())
                self._stopped = True

    def _set_uri(self, uri):
        if uri is not None and uri.scheme in ('http', 'https'):
            self._config.set(self._CONFIG_SECTION, 'livy.uri', uri.geturl())
        else:
            url_exception = uri.geturl if uri is not None else None
            raise ValueError('Cannot create client - Uri not supported - ',
                url_exception)

    def _set_conf(self, key, value):
        if value is not None:
            self._config.set(self._CONFIG_SECTION, key, value)
        else:
            self._delete_conf(key)

    def _delete_conf(self, key):
        self._config.remove_option(self._CONFIG_SECTION, key)

    def _set_multiple_conf(self, conf_dict):
        for key, value in conf_dict.items():
            self._set_conf(key, value)

    def _load_config(self, load_defaults, conf_dict):
        self._config.add_section(self._CONFIG_SECTION)
        if load_defaults:
            self._load_default_config()
        if conf_dict is not None and len(conf_dict) > 0:
            self._set_multiple_conf(conf_dict)

    def _load_default_config(self):
        config_dir = os.environ.get(self._LIVY_CLIENT_CONF_DIR)
        if config_dir is not None:
            config_files = os.listdir(config_dir)
            default_conf_files = ['spark-defaults.conf', 'livy-client.conf']
            for default_conf_file in default_conf_files:
                if default_conf_file in config_files:
                    self._load_config_from_file(config_dir, default_conf_file)

    def _load_config_from_file(self, config_dir, config_file):
        path = os.path.join(config_dir, config_file)
        data = "[" + self._CONFIG_SECTION + "]\n" + \
            open(path, encoding='utf-8').read()
        self._config.readfp(StringIO(data))

    def _create_new_session(self, session_conf_dict):
        data = {'kind': 'pyspark', 'conf': session_conf_dict}
        response = self._conn.send_request('POST', "/",
            headers=self._conn._JSON_HEADERS, data=data)
        return response

    def _reconnect_to_existing_session(self):
        reconnect_uri = "/" + str(self._session_id) + "/connect"
        self._conn.send_request('POST', reconnect_uri,
            headers=self._conn._JSON_HEADERS)

    def _send_job(self, command, job):
        pickled_job = cloudpickle.dumps(job)
        base64_pickled_job = base64.b64encode(pickled_job).decode('utf-8')
        base64_pickled_job_data = \
            {'job': base64_pickled_job, 'jobType': self._job_type}
        handle = JobHandle(self._conn, self._session_id,
            self._executor)
        handle._start(command, base64_pickled_job_data)
        return handle

    def _add_file_or_pyfile_job(self, command, file_uri):
        data = {'uri': file_uri}
        suffix_url = "/" + str(self._session_id) + "/" + command
        return self._executor.submit(self._add_or_upload_resource, suffix_url,
            data=data, headers=self._conn._JSON_HEADERS)

    def _upload_file_or_pyfile(self, command, open_file):
        files = {'file': open_file}
        suffix_url = "/" + str(self._session_id) + "/" + command
        return self._executor.submit(self._add_or_upload_resource, suffix_url,
            files=files)

    def _add_or_upload_resource(
        self,
        suffix_url,
        files=None,
        data=None,
        headers=None
    ):
        return self._conn.send_request('POST', suffix_url, files=files,
            data=data, headers=headers).content

# Create Apache Livy Connection
class _LivyConnection(object):

    _SESSIONS_URI = '/sessions'
    # Timeout in seconds
    _TIMEOUT = 10
    _JSON_HEADERS = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    _SPNEGO_ENABLED_CONF = 'livy.client.http.spnego.enable'

    def __init__(self, uri, config):
        self._server_url_prefix = uri.geturl() + self._SESSIONS_URI
        self._requests = requests
        self.lock = threading.Lock()
        self._spnego_enabled = \
            config.getboolean('env', self._SPNEGO_ENABLED_CONF) \
            if config.has_option('env', self._SPNEGO_ENABLED_CONF) else False

    def _spnego_auth(self):
        if self._spnego_enabled:
            return HTTPKerberosAuth(mutual_authentication=REQUIRED,
                                    sanitize_mutual_error_response=False)
        else:
            return None

    def send_request(
        self,
        method,
        suffix_url,
        headers=None,
        files=None,
        data=None
    ):
        """
        Makes a HTTP request to the server for the given REST method and
        endpoint.
        This method takes care of closing the handles of the files that
        are to be sent as part of the http request

        Parameters
        ----------
        method : string
            REST verb
        suffix_url : string
            valid API endpoint
        headers : dict, optional
            Http headers for the request
            Default is None
        files : dict, optional
            Files to be sent with the http request
            Default is None
        data : dict, optional
            The payload to be sent with the http request
            Default is None

        Returns
        -------
        future : concurrent.futures.Future
           A future to monitor the status of the job

        """
        try:
            with self.lock:
                local_headers = {'X-Requested-By': 'livy'}
                if headers:
                    local_headers.update(headers)
                request_url = self._server_url_prefix + suffix_url
                return self._requests.request(method, request_url,
                    timeout=self._TIMEOUT, headers=local_headers, files=files,
                    json=data, auth=self._spnego_auth())
        finally:
            if files is not None:
                files.clear()
                
# Create SparkContext
class JobContext:
    """
    An abstract class that holds runtime information about the job execution
    context.

    An instance of this class is kept on the node hosting a remote Spark
    context and is made available to jobs being executed via
    RemoteSparkContext#submit().

    """

    __metaclass__ = ABCMeta

    @abstractproperty
    def sc(self):
        """
        The shared SparkContext instance.

        Returns
        -------
        sc : pyspark.context.SparkContext
            A SparkContext instance

        Examples
         -------

        >>> def simple_spark_job(context):
        >>>     elements = [10, 20, 30]
        >>>     sc = context.sc
        >>>     return sc.parallelize(elements, 2).count()
        """
        pass

    @abstractproperty
    def sql_ctx(self):
        """
        The shared SQLContext instance.

        Returns
        -------
        sql_ctx : pyspark.sql.SQLContext
            A SQLContext instance

        Examples
         -------

        >>> def simple_spark_sql_job(context):
        >>>     sql_ctx = context.sql_ctx
        >>>     df1 = sql_ctx.read.json("/sample.json")
        >>>     return df1.dTypes()
        """
        pass

    @abstractproperty
    def hive_ctx(self):
        """
        The shared HiveContext instance.

        Returns
        -------
        hive_ctx : pyspark.sql.HiveContext
            A HiveContext instance

        Examples
         -------

        >>> def simple_spark_hive_job(context):
        >>>     hive_ctx = context.hive_ctx
        >>>     df1 = hive_ctx.read.json("/sample.json")
        >>>     return df1.dTypes()
        """
        pass

    @abstractproperty
    def streaming_ctx(self):
        """
        The shared SparkStreamingContext instance that has already been created

        Returns
        -------
        streaming_ctx : pyspark.streaming.StreamingContext
            A StreamingContext instance

        Raises
        -------
        ValueError
            If the streaming_ctx is not already created using the function
            create_streaming_ctx(batch_duration)

        Examples
         -------

        >>> def simple_spark_streaming_job(context):
        >>>     context.create_streaming_ctx(30)
        >>>     streaming_ctx = context.streaming_ctx
        >>>     lines = streaming_ctx.socketTextStream('localhost', '8080')
        >>>     filtered_lines = lines.filter(lambda line: 'spark' in line)
        >>>     filtered_lines.pprint()
        """
        pass

    @abstractmethod
    def create_streaming_ctx(self, batch_duration):
        """
        Creates the SparkStreaming context.

        Raises
        -------
        ValueError
            If the streaming_ctx has already been initialized

        Examples
        -------
        See usage in JobContext.streaming_ctx

        """
        pass

    @abstractmethod
    def stop_streaming_ctx(self):
        """
        Stops the SparkStreaming context.
        """
        pass

    @abstractproperty
    def local_tmp_dir_path(self):
        """"
        Returns
        -------
        local_tmp_dir_path : string
            Returns a local tmp dir path specific to the context
        """
        pass

    @abstractproperty
    def spark_session(self):
        """
        The shared SparkSession instance.

        Returns
        -------
        sc : pyspark.sql.SparkSession
            A SparkSession instance

        Examples
         -------

        >>> def simple_spark_job(context):
        >>>     session = context.spark_session
        >>>     df1 = session.read.json('/sample.json')
        >>>     return df1.dTypes()
        """
        pass

# Create Job Handle
class JobHandle(Future):

    """A child class of concurrent.futures.Future. Allows for monitoring and
        controlling of the running remote job

    """
    # Poll intervals in seconds
    _JOB_INITIAL_POLL_INTERVAL = 0.1
    _JOB_MAX_POLL_INTERVAL = 5

    def __init__(self, conn, session_id, executor):
        Future.__init__(self)
        self._conn = conn
        self._session_id = session_id
        self._executor = executor
        self._cancelled = False
        self._job_id = -1
        self._done = False
        self._job_handle_condition = threading.Condition()
        self._running_callbacks = []
        self._queued_callbacks = []

    def _start(self, command, serialized_job):
        self._executor.submit(self._send_job_task, command, serialized_job)

    def _send_job_task(self, command, job):
        suffix_url = "/" + str(self._session_id) + "/" + command
        job_status = self._conn.send_request('POST', suffix_url,
            headers=self._conn._JSON_HEADERS, data=job)
        self._job_id = job_status.json()['id']
        self._poll_result()

    def queued(self):
        """
        Returns
        -------
        True if the job is currently queued.
        """
        with self._job_handle_condition:
            return self._state == QUEUED

    def _invoke_queued_callbacks(self):
        for callback in self._queued_callbacks:
            try:
                callback(self)
            except Exception:
                traceback.print_exc()
            self._queued_callbacks.remove(callback)

    def _invoke_running_callbacks(self):
        for callback in self._running_callbacks:
            try:
                callback(self)
            except Exception:
                traceback.print_exc()
            self._running_callbacks.remove(callback)

    def add_queued_callback(self, fn):
        """
        Attaches a callable that will be called when the job is queued.

        Parameters
        ----------
        fn : Function
            A callable that will be called with this future as its only
            argument when the job is queued.
        """
        with self._job_handle_condition:
            if self._state == PENDING:
                self._queued_callbacks.append(fn)
            elif self._state == QUEUED:
                fn(self)

    def add_running_callback(self, fn):
        """
        Attaches a callable that will be called when the job is running.

        Parameters
        ----------
        fn : Function
            A callable that will be called with this future as its only
            argument when the job is running.
        """
        with self._job_handle_condition:
            if self._state in [PENDING, QUEUED]:
                self._running_callbacks.append(fn)
            elif self._state == RUNNING:
                fn(self)

    def _update_state(self, state):
        with self._job_handle_condition:
            if state == 'STARTED':
                self._state = RUNNING
                self._invoke_running_callbacks()
            elif state == 'QUEUED':
                self._state = QUEUED
                self._invoke_queued_callbacks()
            elif state == 'CANCELLED':
                self._state = CANCELLED
                self._invoke_callbacks()
            else:
                raise RuntimeError('Future in unexpected state::', self._state)
            self._job_handle_condition.notifyAll()

    def cancel(self):
        """
        Cancel the job if possible.

        Returns
        -------
        True if the job was cancelled, False if the job has already finished.
        """
        with self._job_handle_condition:
            if self._state == FINISHED:
                return False
            if self._state == CANCELLED:
                return True
            if self._job_id > -1:
                self._executor.submit(self._send_cancel_request)
            self._job_handle_condition.notify_all()
        return True

    def _send_cancel_request(self):
        try:
            end_point = "/" + str(self._session_id) + "/jobs/" + \
                str(self._job_id) + "/cancel"
            self._conn.send_json(None, end_point)
        except Exception as err:
            self.set_job_exception(err, traceback.format_exc())
            traceback.print_exc()

    def _poll_result(self):
        def do_poll_result():
            try:
                suffix_url = "/" + str(self._session_id) + "/jobs/" + \
                    str(self._job_id)
                job_status = self._conn.send_request('GET', suffix_url,
                    headers={'Accept': 'application/json'}).json()
                job_state = job_status['state']
                job_result = None
                has_finished = False
                job_error = None
                if job_state == 'SUCCEEDED':
                    job_result = job_status['result']
                    has_finished = True
                elif job_state == 'FAILED':
                    job_error = job_status['error']
                    has_finished = True
                elif job_state == 'CANCELLED':
                    repeated_timer.stop()
                else:
                    pass
                if has_finished:
                    if job_result is not None:
                        b64_decoded = base64.b64decode(job_result)
                        b64_decoded_decoded = base64.b64decode(b64_decoded)
                        deserialized_object = cloudpickle.loads(
                            b64_decoded_decoded)
                        super(JobHandle, self).set_result(deserialized_object)
                    if job_error is not None:
                        self.set_job_exception(Exception(job_error))
                    repeated_timer.stop()
                else:
                    self._update_state(job_state)
            except Exception as err:
                repeated_timer.stop()
                traceback.print_exc()
                self.set_job_exception(err, traceback.format_exc())

        repeated_timer = self._RepeatedTimer(self._JOB_INITIAL_POLL_INTERVAL,
            do_poll_result, self._executor)
        repeated_timer.start()

    def set_running_or_notify_cancel(self):
        raise NotImplementedError("This operation is not supported.")

    def set_result(self, result):
        raise NotImplementedError("This operation is not supported.")

    def set_exception_info(self, exception, traceback):
        raise NotImplementedError("This operation is not supported.")

    def set_exception(self, exception):
        raise NotImplementedError("This operation is not supported.")

    def set_job_exception(self, exception, error_msg=None):
        if sys.version >= '3':
            super(JobHandle, self).set_exception(exception)
        else:
            super(JobHandle, self).set_exception_info(exception, error_msg)

    class _RepeatedTimer(object):
        def __init__(self, interval, polling_job, executor):
            self._timer = None
            self.polling_job = polling_job
            self.interval = interval
            self.is_running = False
            self.stop_called = False
            self.executor = executor

        def _run(self):
            self.is_running = False
            self.executor.submit(self.polling_job)
            self.start()

        def start(self):
            if not self.is_running and not self.stop_called:
                self._timer = Timer(self.interval, self._run)
                self._timer.start()
                self.interval = min(self.interval * 2,
                    JobHandle._JOB_MAX_POLL_INTERVAL)
                self.is_running = True

        def stop(self):
            self._timer.cancel()
            self.stop_called = True
            self.is_running = False

class RunAnalyticsAndAI(Service):
	name='alkip.api.spark.analytics.ai.service'
	def handle_POST(self):
          kubeResponse = createSparkApp()
          self.response.payload = kubeResponse

def createSparkApp(name='pyspark-pi',namespace='spark'):
    config.load_kube_config()
    yamlFile = {
        'apiVersion': 'sparkoperator.k8s.io/v1beta2',
        'kind': 'SparkApplication',
        'metadata':{
            'name': name,
            'namespace': namespace
        },
        'spec':{
            'type': 'Python',
            'pythonVersion': '3',
            'mode': 'cluster',
            'image': 'gcr.io/spark-operator/spark-py:v3.1.1',
            'imagePullPolicy': 'Always',
            'mainApplicationFile': 'local:///opt/spark/examples/src/main/python/pi.py',
            'sparkVersion': '3.4.1',
            'restartPolicy':{
                'type': 'OnFailure',
                'onFailureRetries': 3,
                'onFailureRetryInterval': 10,
                'onSubmissionFailureRetries': 5,
                'onSubmissionFailureRetryInterval': 20
            },
            'driver':{
                'cores': 1,
                'coreLimit': '1200m',
                'memory': '512m',
                'labels':{
                    'version': '3.4.1'
                },
                'serviceAccount': 'spark'
            },
            'executor':{
                'cores': 1,
                'instances': 1,
                'memory': '512m',
                'labels':{
                    'version': '3.4.1'
                }
            }
        }
    }
    deployment = yaml.safe_dump(json.dumps(yamlFile))
    k8s_apps_v1 = client.AppsV1Api()
    resp = k8s_apps_v1.create_namespaced_deployment(body=deployment, namespace=namespace)
    print("Deployment created. status='%s'" % resp.metadata.name)
    return resp
