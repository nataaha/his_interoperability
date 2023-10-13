# -*- coding: utf-8 -*-
# Facility Registry Service
# Creates facility registry from systems delegated as Master based on openHIE architecture
# 

from __future__ import absolute_import, division, print_function, unicode_literals
# stdlib
import json,bunch,uuid
import riak,pandas as pd, jellyfish as jf

from urlparse import parse_qs
# Custom services
import datetime

from django import forms
from django.db import models
from django.core.exceptions import ValidationError
from django.utils.translation import ugettext_lazy as _
from django.shortcuts import get_object_or_404
from django.http import HttpResponseRedirect
from django.views.generic import CreateView

#import database
# Zato

from zato.server.service.internal.helpers import HTMLService

HTML_TEMPLATE = """
{% load static %}
<!DOCTYPE html>
<html>
    <head>
        <title>Integration Layer Platform</title>
        <link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css">
        <link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap-theme.min.css">
        <link href='//fonts.googleapis.com/css?family=Lobster&subset=latin,latin-ext' rel='stylesheet' type='text/css'>
        <link rel="stylesheet" href="{% static 'css/blog.css' %}">
    </head>
    <body>
        <div class="page-header">
            <a href="api?dataType=json&serviceType=frs" class="top-menu"><span class="glyphicon glyphicon-plus">MFR API</span></a>
            <h1><a href="reports">MFR Reports</a></h1>
            <h1><a href="/ts/reports">TS Reports</a></h1>
        </div>
        <div class="content container">
            <div class="row">
                <div class="col-md-8">
                    
					{% block content %}

						<form name = "form" action = "login" 
						 method = "POST" >{% csrf_token %}
						 
						 <div style = "max-width:470px;">
						    <center> 
						       <input type = "text" style = "margin-left:20%;" 
						          placeholder = "Search" name = "Search" />
						    </center>
						 </div>
					
						 
						 <div style = "max-width:470px;">
						    <center> 
						    
						       <button style = "border:0px; background-color:#4285F4; margin-top:8%;
						          height:35px; width:80%;margin-left:19%;" type = "submit" 
						          value = "Login" >
						          <strong>Search</strong>
						       </button>
						       
						    </center>
						 </div>
						 
						</form>
						Master Facility Registry.
						<table class="table table-striped">
						    {% for i in organisationUnits %}
						    	<tr>
						    		<td>{{ i.Admin_1_name }}</td>
						    		<td>{{ i.Admin_2_name }}</td>
						    		<td>{{ i.Admin_3_name }}</td>
						    		<td>{{ i.Admin_4_name }}</td>
						    		<td>{{ i.name }}</td>
						    		<td>{{ i.id }}</td>
						    	</tr>
						    {% endfor %}
						</table>
					{% endblock %}
				</div>
      		</div>
        </div>
    </body>
</html>
"""
class CreateFacilityRegistryReports(HTMLService):
    """ Creates Master Facility Registry Reports endpoint.
    """
    def handle(self):

        # Python dict representing the payload we want to send across
        payload = {}

        # Python dict with all the query parameters, including path and query string
        params = {}

        # Headers the endpoint expects
        headers = {'X-Service': 'Facility Registry', 'X-Version':'Production'}

        ## Get connection to Riak Database
        #riak_frs = self.connect('dhis2.jsi.com',8087,'integration')
        riak_frs = self.connect('192.168.106.128',8087,'integration')

        riak_data = self.getItems(riak_frs,'organisationUnits')
        ctx = riak_data
        #self.logger.info(ctx)
        # Set HTML payload, will also produce the Content-Type header
        self.set_html_payload(ctx, HTML_TEMPLATE) 

    #Riak Routines
    def connect(self,host, port, bucket):
        self.logger.info("Connecting to Riak Database")
        client = riak.RiakClient(host=host, pb_port=port, protocol='pbc')
        current_bucket = client.bucket(bucket)
        self.logger.info("Connected to the bucket "+ bucket)        
        return current_bucket
    def getItems(self,client,items):
    	newItem = client.get(items)
    	retrievedItem = newItem.data
    	return retrievedItem

    def sendToRabbitMQ(self,message,sender,reciever):
        print("Sending messages to RabbitMQ")
    
    def recieveFromRabbitMQ(self,message,sender,reciever):
        print("Recieving messages from RabiitMQ")

	def login(self,request):
		username = "not logged in"

		if request.method == "POST":
		  #Get the posted form
		  MyLoginForm = LoginForm(request.POST)
		  
		  if MyLoginForm.is_valid():
		     username = MyLoginForm.cleaned_data['username']
		else:
		  MyLoginForm = LoginForm()
			
		return render(request, 'loggedin.html', {"username" : username})
	class LoginForm(forms.Form):
		user = forms.CharField(max_length = 100)
		password = forms.CharField(widget = forms.PasswordInput())
	class Person(models.Model):
	    name = models.CharField(max_length=130)
	    email = models.EmailField(blank=True)
	    job_title = models.CharField(max_length=30, blank=True)
	    bio = models.TextField(blank=True)
	class PersonCreateView(CreateView):
	    model = Person
	    fields = ('name', 'email', 'job_title', 'bio')