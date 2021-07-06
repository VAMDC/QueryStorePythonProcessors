#!/usr/bin/python
#-*- coding: utf-8 -*-

import requests
import json
import sys
import urllib2
import bibtexparser
import config
import uuid
import os
from datetime import datetime

"""
    Procedure to publish dataset in zenodo
"""
class ZenodoRequest:
    
    def __init__(self, uuid):
        self.url = config.CITE_UUID+uuid
        self.jsonData = {}
        
    def getDataFromLowerTimestamp(self):
        res = self.jsonData["queryInformation"]["queryInvocationDetails"][0]
        lowerData = res["timestamp"]
        
        for i in range(1, len(self.jsonData["queryInformation"]["queryInvocationDetails"])):
            try:
                a = int(self.jsonData["queryInformation"]["queryInvocationDetails"][i]["timestamp"])
                b = int(lowerData)
                if(a < b):
                    lowerData = a
                    res = self.jsonData["queryInformation"]["queryInvocationDetails"][i]
            except ValueError:
                print("impossible to convert timestamp")
        return res
    
    def dateFormat(self,lowerData):
        DateFormat = '%Y-%m-%d'
        return datetime.fromtimestamp(lowerData["timestamp"]/1000).strftime(DateFormat)
    
    def getDescFromJson(self):
        data = self.jsonData["queryInformation"]
        
        queries = ""
        for j in data["parameters"]:
            queries += j+" "
        
        desc = "<div>This is a dataset extracted from %s VAMDC node.</div><div>Query originating this dataset: %s</div><div> Data source version: %s</div><div>Data format: XSAMS %s</div><div>Query uuid in VAMDC query store: %s</div>" % (data["accededResource"],queries, data["resourceVersion"],data["outputFormatVersion"],data["UUID"])
            
        return desc
    
    def getReference(self):
        reference =[]
        fileName = str(uuid.uuid4())+'.bib'
        
        with open('/tmp/'+fileName, 'w') as bibfile:
            biblio = self.jsonData["queryInformation"]["biblioGraphicReferences"]
            bibfile.write(biblio.encode('utf-8'))
        with open('/tmp/'+fileName) as bibtex_file:
            bib_database = bibtexparser.bparser.BibTexParser(common_strings=True).parse_file(bibtex_file)
        
        for i in bib_database.entries:
            title = ""
            journal = ""
            doi = ""
            if("title" in i):
                title = i["title"]+". "
            if("journal" in i):
                journal = i["journal"]+". "
            if("doi" in i):
                doi = i["doi"]
            test = i["author"]+ " ("+i["year"]+"). "+title+journal+doi
            reference.append(test)
        
        os.remove('/tmp/'+fileName)
        return reference
    
    def checkDoiExists(self):
        res = True
        if(self.jsonData["queryInformation"]["DOI"] != None):
            res= False
        return res
    
    def checkStatusCode(self,message, result):
        result["code"] = "fail"
        result["message"] = message
        return result
    
    """
        Get the metadata
    """
    def getMetadata(self):
        lowerData = self.getDataFromLowerTimestamp()
        references = self.getReference()
        data = {
            'metadata': {
                    'upload_type': "dataset",
                    'publication_date': self.dateFormat(lowerData),
                    'title': 'VAMDC extraction with identifier = '+self.jsonData["queryInformation"]["UUID"],
                    'creators': [{'name': 'VAMDC, Consortium'}],
                    'description': self.getDescFromJson(),
                    'version': self.jsonData["queryInformation"]["resourceVersion"],
                    'language':'eng',
                    'related_identifiers':[{'relation': 'isIdenticalTo', 'identifier':config.CITE_I_UUID+self.jsonData["queryInformation"]["UUID"]}],
                    'references':references
                }
            }
        if("contributors" in self.jsonData):
            data["metadata"]["contributors"] = self.jsonData["contributors"]

        return data
    
    def submitDatasetToZenodo(self, result):
        headers = {"Content-Type": "application/json"}
        
        """ access to zenodo API """
        res = requests.post(config.ZENODO_URL,
                   params={'access_token': config.TOKEN}, json={},
                   headers=headers)
        
        if(res.status_code != 201):
           return self.checkStatusCode("The access to zenodo fail, please check the error_code %d"%res.status_code +" in zenodo",result)
       
        """ if the access works """
        deposition_id = res.json()['id']
        
        data = {'name': self.jsonData["queryInformation"]["dataURL"].split("/")[-1]}
        
        url = self.jsonData["queryInformation"]["dataURL"]
        
        try: 
            xsamsFile = urllib2.urlopen(url)
            files = {'file': xsamsFile}
        
            """ upload file"""
            res = requests.post(config.ZENODO_URL+'/%s/files' % deposition_id,params={'access_token': config.TOKEN}, data=data,    files=files)
        
            if(res.status_code != 201):
                return self.checkStatusCode("The dataset is not accepted, please check the error_code %d"%res.status_code+ " in zenodo",result)
        except urllib2.URLError as e:
            return self.checkStatusCode("The xsams file %s"%url +" does not exist",result)
            
        
        data = self.getMetadata()
        
        """ add metadata to the file"""
        res = requests.put(config.ZENODO_URL+'/%s' % deposition_id,
                  params={'access_token': config.TOKEN}, data=json.dumps(data),
                  headers=headers)
        
        if(res.status_code != 200):
           return self.checkStatusCode("The metadata linked to dataset is not accepted, please check the error_code %d"%res.status_code +" in zenodo",result)
        
        """ publish """
        res = requests.post(config.ZENODO_URL+'/%s/actions/publish' % deposition_id,
                      params={'access_token': config.TOKEN})
        
        if(res.status_code != 202):
           return self.checkStatusCode("The publication is not accepted, please check the error_code %d"%res.status_code +" in zenodo",result)
        
        result["code"] = "success"
        result["doi"] = res.json()['doi'] 
        result["id_zenodo"] = deposition_id
        
        return result
        
    """
        publish dataset in zenodo and return a dictionary with the doi and the zenodoid related to the submission
    """
    def publish(self):
        result = {}
        try:
            f = urllib2.urlopen(self.url)
            self.jsonData = json.loads(f.read())
        except ValueError:
            return self.checkStatusCode("No JSON object could be decoded", {})
        
        if(not self.checkDoiExists()):
           result["code"] = "fail"
           result["message"] = "Doi already exists, submission could not be done twice"
           return result
           
        return self.submitDatasetToZenodo(result)
    
    """ Save the doi in db """
    def saveInDatabase(self):
        res = {}
        result = self.publish()
        
        if(result["code"] == "success"):
            parameters = {'uuid':self.jsonData["queryInformation"]["UUID"], 'DoiSubmitId':result["id_zenodo"], 'secret':config.SECRET , 'Doi':result["doi"]}
            response = requests.post(config.PUSHDOI, params=parameters)
            if(response.status_code == 200):
                res["code"] =  "success"
                res["message"] = "Doi is saved"
            else:
                res["code"] =  "fail"
                res["message"] = "Doi is not saved"
        else:
            res = result
        return res
