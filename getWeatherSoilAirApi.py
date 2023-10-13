# -*- coding: utf-8 -*-
import requests
# stdlib
import bunch
#from json import dumps,loads
# Zato
from zato.server.service import Service

class GetWeatherSoilAirApi(Service):
    name = "alkuip.api.get.weathersoilair"
    class SimpleIO:
        input_optional = ("source","type","exclude","query","lat","lon","place")
    def handle_GET(self):
        exclude = "hourly,daily,minutely"
        query = "onecall"
        type = "current"
        lat = 33.44
        lon = -94.04
        weatherSource = "Weather API"
        place = "Kampala"
        inputParamsQuery = f"current?q={place}&aqi=yes&alerts=yes"
        if self.request.input.exclude is not None:
            exclude = self.request.input.exclude
        if self.request.input.query is not None:
            query = self.request.input.query
        if self.request.input.lat is not None:
            lat = self.request.input.lat
        if self.request.input.lon is not None:
            lon = self.request.input.lon
        if self.request.input.place is not None:
            place = self.request.input.place
        if self.request.input.type is not None:
            type = self.request.input.type        
        if self.request.input.source is not None:
            if self.request.input.source == "openweatherapi":
                weatherSource = "OpenWeather API"
                if type is not None:
                    newType = type
                else:
                    newType = "data"
                inputParamsQuery = f"{newType}/2.5/{query}?lat={lat}&lon={lon}&exclude={exclude}"
            else:
                weatherSource = "Weather API"
                if lat is not None and lon is not None:
                    inputParamsQuery = f"{type}?q={lat},{lon}&aqi=yes&alerts=yes"
                else:
                    inputParamsQuery = f"{type}?q={place}&aqi=yes&alerts=yes"
        # make request
        req = self.outgoing.plain_http.get(weatherSource)
        inputParams = { "query": f"{inputParamsQuery}" }
        if "username" in req.config and "password" in req.config and req.config["sec_type"] == "apikey":
            inputParams = { "query": f"{inputParamsQuery}&{req.config['username']}={req.config['password']}" }
        response = req.conn.get(self.cid,params=inputParams)
        self.response.payload = bunch.bunchify(response.text)