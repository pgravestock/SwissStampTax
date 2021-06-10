#===============================================================================================================
# Refinitiv Data Platform demo app to return Reference Data using GraphQL
#===============================================================================================================
import requests
import json
import rdp_Prod_Authentication_Token
from datetime import datetime
import os


def graphQLRequest(graphQLQuery,queryType, Seq, IdentifierStamp):

    EDP_version = "/v1"
    base_URL = "https://api.refinitiv.com"
    category_URL = "/data-store"
    endpoint_URL = "/graphql"
    CLIENT_SECRET = ""

    SCOPE = "trapi"

    RESOURCE_ENDPOINT = base_URL + category_URL + EDP_version + endpoint_URL
    #print (f"\nAPI Endpoint: {RESOURCE_ENDPOINT}")

    #key=".json"
    #baseLoc=fileObject.find(key)
    outputfile='./Output/'+queryType+'_' + str(Seq) + '_Response_'+ IdentifierStamp + '.json'

    #=================================================
    # Get the latest access token
    #print("Getting OAuth access token....")
    accessToken=rdp_Prod_Authentication_Token.getToken()
    #print("Lookup ReferenceData")

    headerData = {"Authorization" : "Bearer " + accessToken, "content-type" : "application/json"}
    
    dResp = requests.post(RESOURCE_ENDPOINT, headers= headerData, data=graphQLQuery)

    if dResp.status_code !=200:
        print("There was a problem. Code %s, Message: %s" % (dResp.status_code, dResp.text))
    else:
        jResp=json.loads(dResp.text)
        GraphQLResponse=open(outputfile,"w")
        GraphQLResponse.write(json.dumps(jResp,indent=4))
        GraphQLResponse.close()
        file_size=os.path.getsize(outputfile)
        
        return jResp, file_size

    #=====================================================

