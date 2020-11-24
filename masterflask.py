from flask import Flask, request, render_template, jsonify
from flask_request_params import bind_request_params
from helper import *
import requests
import boto3
import json
import os
import configparser
import sys, traceback
import time
import random
import multiprocessing
from multiprocessing import Pool
import re
import gc
gc.disable()

config = configparser.RawConfigParser()
##########################################################################################
config.read(config.read(os.getcwd()+'/config.property'))
#########################################################################################
S3BUCKETNAME= dict(config.items('S3BUCKETNAME'))
bucket=S3BUCKETNAME["bucket"]
VERSION=dict(config.items('VERSION'))
version=VERSION["version"]
PORTS= dict(config.items('PORTS'))
port_no= PORTS['port_no']
ENDPOINTURL=dict(config.items("ENDPOINTURL"))
endpoint=ENDPOINTURL["endpoint"]
ENDPOINTDOCUMENTPROCESSOR=dict(config.items("ENDPOINTDOCUMENTPROCESSOR"))
urldocumentprocessor=ENDPOINTDOCUMENTPROCESSOR["urldocumentprocessor"]
DOCACCEPTED=dict(config.items("DOCACCEPTED"))
accepteddoctype=DOCACCEPTED["accepteddoctype"]
DBURL=dict(config.items("DBURL"))
urldb=DBURL["urldb"]
HTTPSPATH=dict(config.items("HTTPSPATH"))
https_path = HTTPSPATH["https_path"]
MASTERDATAENDPOINT=dict(config.items("ENDPOINTMASTERDATAAPI"))
masterapi = MASTERDATAENDPOINT["masterapi"]
DOCCODEENDPOINT=dict(config.items("ENDPOINTDOCCODEAPI"))
docCodeEndpoint = DOCCODEENDPOINT["doccodeendpoint"]
docPurposeList = fetch_doc_purpose_list()


app = Flask(__name__)
app.secret_key = 'secret'
# bind rails like params to request.params
app.before_request(bind_request_params)


# function to call ocr individual API
def callIndividualOCR(dictionary):
  print("Inside callIndividualOCR...")
  print(dictionary)
  try:
    try:
      docpurpose = re.findall(docPurposeList, dictionary['docpath'].split("/")[-1])[0]
      docpurpose =check_masterdata(docpurpose)
    except:
      print("File name not supported for document " + dictionary['docpath'])
      docpurpose="NA"
    dict_for_Ind_OCR={"docType": "NA", "docpath": dictionary['docpath']}
    response = requests.post(urldocumentprocessor, json = dict_for_Ind_OCR)
    result = response.json()
    result["docPurpose"] = docpurpose
    print('Getting DOC TYPE CODE')
    # docTypeCodeURL = docCodeEndpoint + result["docPurpose"].upper() + '/' + result["response"]["documentType"].upper()
    # docTypeCode = requests.get(docTypeCodeURL).json()
    # if(docTypeCode['statusCode'][0:4]=='L200'):
        # result["response"]["documentType"] = docTypeCode['response'][0]['codeValue']

    result["sourceService"]=dictionary["sourceService"]
    result["sourceUser"] = dictionary["sourceUser"]
    applicationNoDB=dictionary["applicationNoDB"]
    # DB PUSH 
    DB = {"sourceService":result["sourceService"],
          "sourceUser":result["sourceUser"],
          "applicationNumber":str(applicationNoDB),
          "documentAccuracy": result["response"]["documentAccuracy"], 
          "documentMatch": result["response"]["documentMatch"], 
          "documentMatchProbability": result["response"]["documentMatchProbability"], 
          "documentPath": result["response"]["documentPath"], 
          "documentType": result["response"]["documentType"],
          "docPurpose":result["docPurpose"],
          "extractedFields": result["response"]["extractedFields"],
          "extractedFaces":result["response"]["extractedFaces"],
          "extractedSignatures":result["response"]["extractedSignatures"],
          "version":result["version"]}
    #succesfull database push
    dbresult = pushDB(DB["documentType"].lower(),str(applicationNoDB), DB)
    print(dbresult)
    print("### TIME TAKEN TO RUN OCR ON DOCUMENT "+ result["response"]["documentPath"] + " IS " + str(round(result["timeTakenForResponse"],3))+" seconds###")
  except Exception as e :
    applicationNoDB=dictionary["applicationNoDB"]
    return  Exception_Ocr_Ind("E50001", repr(str(e)), version,str(applicationNoDB),"NA",docpurpose, dictionary['docpath'].split("/")[-2] + "/" +  dictionary['docpath'].split("/")[-1],"NA","NA")
  return result

@app.route(endpoint, methods=['POST'])
# Main OCR consolidated fucntion
def ocrconsolidated():
    doctypes=accepteddoctype.split("/")
    start = time.time()
    requestParam = request.get_json()
    print(requestParam)
    try:
      applicationFolder = requestParam["applicationNumber"]
      sourceService= requestParam["sourceService"]
      sourceUser=requestParam["sourceUser"]
      # application number feteches first element of document path in application number DB
      applicationNoDB = applicationFolder.split("/")[0]
      counter = 0
      try:
        master_data = requests.get(masterapi,timeout=10).json()
      except:
        # except_response = generate_Exception("E50004","cannot connect to Master Data API",totaltime,version,applicationFolder,[],[])
        return jsonify({"statusCode":"E50004","statusMessage":"Master Data API not working".format(counter), "timeTakenForResponse":time.time()-start,"version": version,
                "applicationNumber":applicationFolder,"data":{"extractedDocuments":[],"failedDocuments":[] }}),500
      
    except Exception as inst:
      print(inst)
      traceback.print_exc(file=sys.stdout)
      totaltime=time.time() - start
      except_response = generate_Exception("E40001","bad request (add fields sourceUser,sourceService or check field name)",totaltime,version,"NA",[],[])
      return jsonify(except_response),400
    try:
      # fetches list of document from the S3
      docList = feeder(bucket,applicationFolder)
    except Exception as inst:
      # return if the name of the bucket is incorrect
      print(inst)
      traceback.print_exc(file=sys.stdout)
      totaltime=time.time() - start
      except_response = generate_Exception("E50002","incorrect bucket name , connection issue or invalid access keys",totaltime,version,applicationFolder,[],[])
      return jsonify( except_response),500
    # empty list for appending the document reponse given by OCR individual
    # doctionary is the list of dicntion given to ocr individual as parallel processing
    if(len(docList)==0 or applicationFolder == ""):
      totaltime=time.time() - start
      except_response = generate_Exception("E40002","Application Number not in bucket",totaltime,version,applicationFolder,[],[])
      return jsonify( except_response),400
    dictionaires = []
    counter = 0
    successful_documents = []
    failed_documents = []
    # looping in doc list

    for doc in docList:
      if "PRO" in doc.split("/")[-1]:
          try:
            docpurpose = re.findall("PRO",doc.split("/")[-1])[0]
            docpurpose =check_masterdata(docpurpose)
          except:
            print("File name not supported for document "+doc)
            docpurpose="NA"
          indResponse, conResponse = manufactureArtificialResponses("L20001", "Successfully parsed", version, str(applicationNoDB), "NA", docpurpose,doc.split("/")[-2] + "/" +  doc.split("/")[-1],sourceService,sourceUser)
          dbresult = pushDB("PRO", str(applicationNoDB), indResponse)
          successful_documents.append(conResponse)
          counter = counter + 1
      elif ("PHO" in doc.split("/")[-1]) or ("ATS" in doc.split("/")[-1]) or ("BAS" in doc.split("/")[-1]) or ("ECS" in doc.split("/")[-1]):
          try:
            docpurpose =  re.findall("(PHO|ATS|BAS|ECS)",doc.split("/")[-1])[0]
            docpurpose =check_masterdata(docpurpose)
          except:
            print("File name not supported for document "+doc)
            docpurpose="NA"
          indResponse, conResponse = manufactureArtificialResponses("L20001", "Successfully parsed", version, str(applicationNoDB), "NA", docpurpose, doc.split("/")[-2] + "/" +  doc.split("/")[-1],sourceService,sourceUser)
          dbresult = pushDB(docpurpose, str(applicationNoDB),indResponse)
          successful_documents.append(conResponse)
          counter = counter + 1
      elif any([dockind in doc.split("/")[-1]  for dockind in doctypes]):
        for key in doctypes:
          if key in doc.split("/")[-1]:
            dictionary = {"docType": "NA", "docpath": doc,"applicationNoDB":applicationNoDB,"sourceService":sourceService,"sourceUser":sourceUser}
            dictionaires.append(dictionary)
      # if docpurpose fails to match with any of the docpurpose provided it will be considered as failed document
      else:
        try:
          img_name = doc.split("/")[-1]
          docpurpose=re.findall(docPurposeList, img_name)[0]
          if(docpurpose == "F"):
            if("FNA" in doc):
              docpurpose = "FNA"
            elif("FIQ" in doc):
              docpurpose = "FIQ"
            elif("FCR" in doc):
              docpurpose = "FCR"
          docpurpose =check_masterdata(docpurpose)
          print(docpurpose)
        except Exception as e:
          print("File name not supported for document "+doc,e)
          docpurpose="NA"

        counter = counter + 1
        indResponse, conResponse = manufactureArtificialResponses("L20001", "Successfully parsed", version, str(applicationNoDB),"NA", docpurpose, doc.split("/")[-2] + "/" +  doc.split("/")[-1],sourceService,sourceUser)
        dbresult=pushDB("failed",str(applicationNoDB), indResponse)
        successful_documents.append(conResponse)

    



    # for multiprocessing of the documents in parallel
    dictionaires = (n for n in dictionaires)
    pool = Pool(processes = multiprocessing.cpu_count()-1)
    concatResponse = pool.map(callIndividualOCR, dictionaires)
    pool.close()
    # print("concat reponse ",concatResponse)
    for result in concatResponse:
      successful_documents.append({"sourceService":result["sourceService"],"sourceUser":result["sourceUser"],"s3Path": result["response"]["documentPath"],"docType": result["response"]["documentType"],"statusCode": result["statusCode"],"statusMessage": result["statusMessage"],"timeTakenForResponse": result["timeTakenForResponse"],"version": result["version"],"docPurpose": result["docPurpose"],"applicationNumber": applicationFolder})
      if result["statusCode"] in ["L20001","L20010","L20011","L20012","L20002","L20013","L20014","L20015"] or (result["response"]["documentType"] == "OTHERS"):
        counter = counter + 1  
        
      # else:
        # failed_documents.append({"sourceService":result["sourceService"],"sourceUser":result["sourceUser"],"s3Path":result["response"]["documentPath"],"docType":result["response"]["documentType"],"statusCode":result["statusCode"],"statusMessage":result["statusMessage"],"timeTakenForResponse":result["timeTakenForResponse"],"version":result["version"],"docPurpose": result["docPurpose"],"applicationNumber":applicationFolder})

    totaltime=time.time() - start
    # final response to be added in the database of consolidated api
    final_reponse_db={"statusCode":"L20001","statusMessage":"Total of {} documents succesfully extracted".format(counter), "timeTakenForResponse":totaltime,"version": version,"applicationNumber":str(applicationNoDB),
                      "data":{"extractedDocuments": successful_documents,"failedDocuments": failed_documents}}
    try:
      dbresult=pushDB("supportingdocument",str(applicationNoDB),final_reponse_db)
      print(dbresult)
      if(dbresult == "CONNECTION ISSUES"):
        except_response = generate_Exception("E50003","cannot push to database",totaltime,version,applicationFolder,[],[])
        return jsonify(except_response),500
    except Exception as inst:
      print(inst)
      traceback.print_exc(file=sys.stdout)
      totaltime=time.time() - start
      except_response = generate_Exception("E50003","cannot push to database",totaltime,version,applicationFolder,[],[])
      return jsonify(except_response),500
      print("failed_list",failed_documents)
      print("successful_documents",successful_documents)
      
    # final output of the function
    totaltime=time.time() - start
    if(len(failed_documents)==0):
      return  jsonify({"statusCode":"L20002","statusMessage":"Total of {} documents succesfully extracted".format(counter), "timeTakenForResponse":totaltime,"version": version,
              "applicationNumber":applicationFolder,"data":{"extractedDocuments":successful_documents,"failedDocuments":failed_documents }}),200
    else:
      return  jsonify({"statusCode":"E40002","statusMessage":"Total of {} documents extracted".format(counter), "timeTakenForResponse":totaltime,"version": version,
              "applicationNumber":applicationFolder,"data":{"extractedDocuments":successful_documents,"failedDocuments":failed_documents }}),200


# serve at localhost:5000
app.run(debug=False,host= "0.0.0.0",port=port_no,threaded=True )
