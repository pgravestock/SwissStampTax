import json
import csv
import rdp_Prod_GraphQL
from datetime import datetime

import rdp_Prod_Log_Processing

def RequestSwissTaxData (Symbology_Output,IdentifierStamp, SymbologyRunTime):
    symbology_Payload={} #Stores the Symbology API output

    outputFile="./Output/Symbology_Summary_" + IdentifierStamp+ ".csv" #Defines the file name of the Symbology CSV summary file

    symbology_results=open(outputFile, 'w')
    symbology_results.write("Identifier Type, Identifier, Object Type, PermID\n")
    
    symbology_Payload=json.loads(Symbology_Output)
    
    ValidObjectTypes=("GovCorpInstrument", "FundShareClass","EDInstrument","AbsCmoInstrument","MuniInstrument","SPInstrument","MbsPoolInstrument","MbsTbaInstrument")
    GovCorpInstrument=[]
    FundShareClass=[]
    EDInstrument=[]
    AbsCmoInstrument=[]
    MuniInstrument=[]
    SPInstrument=[]
    MbsPoolInstrument=[]
    MbsTbaInstrument=[]

    #Batch Sizes for each query
    FCSChunkSize=200     #FundShareClass BatchSize
    GCChunkSize=200      #GovCorp Batch Size
    MuniChunkSize=200    #Muni Batch Size
    EDChunkSize=200      #EDF Batch Size
    SPChunkSize=200      #Structurted Product Batch Size
    AbsCmoChunkSize=200  #Abs CMO Batch Size


    #GraphQL Query Paths
    FundShareClassQuery="./gql_Queries/gql_DerivedContent_FundShareClass_Swiss_Tax.gql"
    GovCorpInstrumentQuery="./gql_Queries/gql_DerivedContent_GovCorpInstrument_Swiss_Tax.gql"
    MuniInstrumentQuery="./gql_Queries/gql_DerivedContent_MuniInstrument_Swiss_Tax.gql"
    EDInstrumentQuery="./gql_Queries/gql_DerivedContent_EDFInstrument_Swiss_Tax.gql"
    AbsCmoInstrumentQuery="./gql_queries/gql_DerivedContent_Cmo_Swiss_Tax.gql"
    SPInstrumentQuery="./gql_Queries/gql_DerivedContent_StructurtedProducts_Swiss_Tax.gql"


    for sequence, payload in symbology_Payload.items():
        if sequence=="data":
            
            instruments=symbology_Payload['data']
            results=[] #This list will be built up with the fields being output in a single row for the resulting csv filer
            
            for instrument in instruments:
                inputs=instrument['input']
                for userInputs in inputs:
                    identifierType=userInputs['identifierType']
                    identifierValue=userInputs['value']
                    responses=instrument['output']

                    for response in responses:
                        objectType=response['objectType']
                        permID=response['value']
                        results.extend((identifierType, identifierValue, objectType, permID))
                        if objectType in ValidObjectTypes:
                            if objectType=="GovCorpInstrument":
                                GovCorpInstrument.append(permID)

                            elif objectType=="FundShareClass":
                                FundShareClass.append(permID)
                                # Model not yet in production

                            elif objectType=="EDInstrument":
                                EDInstrument.append(permID)

                            elif objectType=="AbsCmoInstrument":
                                AbsCmoInstrument.append(permID)

                            elif objectType=="MuniInstrument":
                                MuniInstrument.append(permID)

                            elif objectType=="SPInstrument":
                                SPInstrument.append(permID)

                            elif objectType=="MbsPoolInstrument":
                                MbsPoolInstrument.append(permID)

                            elif objectType=="MbsTbaInstrument":
                                MbsTbaInstrument.append(permID)

                        else:
                            print(f"Unknown ObjectType {objectType} encountered. Please update rdp_Prod_GraphQL_Requests.py code")
        
                        wr=csv.writer(symbology_results,dialect='excel')
                        wr.writerow(results)
                        results=[]

    symbology_results.close()
    print(f"\n\n{outputFile} Done")
    
    print(f"\nGovCorpInstrument {len(GovCorpInstrument)}")
    print(f"FundShareClass {len(FundShareClass)}")
    print(f"AbsCmoInstrument {len(AbsCmoInstrument)}")
    print(f"EDInstrument {len(EDInstrument)}")
    print(f"MuniInstrument {len(MuniInstrument)}")
    print(f"SPInstrument {len(SPInstrument)}")
    print(f"MbsPoolInstrument {len(MbsPoolInstrument)}")
    print(f"MbsTbaInstrument {len(MbsTbaInstrument)}\n")

    log={}
    log["SymbologyRunTime"]= SymbologyRunTime
    log["objectTypes"]=[]

    AbsCmoBatchLog=[]
    if AbsCmoInstrument:
        #Write the list of ABSCMO Instrumdent PermIds to a file for future use
        AbsCmoInstrumentList={}
        AbsCmoInstrumentList["ObjectList"]=AbsCmoInstrument
        jsonStr=json.dumps(AbsCmoInstrumentList,indent=4)

        AbsCmoOutput=open("./Output/AbsCmoInstrument_Object_List_" + IdentifierStamp + ".json","w")
        AbsCmoOutput.write(jsonStr)        
        AbsCmoOutput.close()

        # Break the CmoInstrument list into batches of [CmoChunkSize]. Platform limit is 200, but
        # depending on query complexity it may be necesary to request smaller batches
        ChunkedAbsCmo=[]
        ChunkedAbsCmo = Chunk(AbsCmoInstrument,AbsCmoChunkSize)

        print(f"There are {len(AbsCmoInstrument)} AbsCmoInstruments split into {len(ChunkedAbsCmo)} batches")
        StartTimeAbsCmo=datetime.now()

        AbsCmoi=0

        AbsCmoBatchDetails=[]
        # Step through each batch of ObjectIDs and run graphQL Query
        while AbsCmoi<len(ChunkedAbsCmo):
            AbsCmo_Query={}
            #This is the path/file name of the graphQl query that will be run
            AbsCmo_Query['query']=loadQuery(AbsCmoInstrumentQuery)
            AbsCmo_Query['variables']={
                "ObjectList":ChunkedAbsCmo[AbsCmoi]
                }
            AbsCmo_GraphQL_Query=json.dumps(AbsCmo_Query)

            StartTimeAbsCmoB=datetime.now()
            AbsCmoGraphQLResponse, fileSize=rdp_Prod_GraphQL.graphQLRequest(AbsCmo_GraphQL_Query,"AbsCmo", AbsCmoi, IdentifierStamp)
            EndTimeAbsCmoB=datetime.now()
            BatchRunTimeAbsCmo=(EndTimeAbsCmoB - StartTimeAbsCmoB).total_seconds()
            batchStatus=None
            for sequence,payload in AbsCmoGraphQLResponse.items():
                if sequence=="errors":
                    errors=AbsCmoGraphQLResponse['errors']
                    errorInfo=errors[0]
                    message=errorInfo['message']
                    extensions=errorInfo['extensions']
                    batchErrorCode=extensions['errorCode']
                    batchStatus="Fail"
            
            if batchStatus!="Fail":
                    batchStatus="Success"
                    batchErrorCode = None

            if batchStatus=="Success":

                AbsCmoBatchDetails.append({
                    "batch": AbsCmoi,
                    "batchGraphQLRunTime": BatchRunTimeAbsCmo,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedAbsCmo[AbsCmoi])
                })
            else:
                 AbsCmoBatchDetails.append({
                    "batch": AbsCmoi,
                    "batchGraphQLRunTime": BatchRunTimeAbsCmo,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedAbsCmo[AbsCmoi]),
                    "objectIds": ChunkedAbsCmo[AbsCmoi]
                })               

            batchStatus= None

            AbsCmoi += 1

        EndTimeAbsCmo=datetime.now()

        AbsCmoBatchLog.append({
            "objectType": "AbsCmoInstrument",
            "objectCount": len(AbsCmoInstrument),
            "chunkSize": AbsCmoChunkSize,
            "totalGraphQLRunTime": (EndTimeAbsCmo - StartTimeAbsCmo).total_seconds(),
            "batchList": AbsCmoBatchDetails
        })
        log["objectTypes"].append({
            "objectType":"AbsCmoInstrument",
            "results": AbsCmoBatchLog
        })

        print("\nTotal GraphQL API Run Time for AbsCmo Instrument ", (EndTimeAbsCmo - StartTimeAbsCmo).total_seconds())
        print(f"\n")        

    FCSBatchLog=[]
    if FundShareClass:
        #Write the list of GovCorp Instrumdent PermIds to a file for future use
        FundShareClassList={}
        FundShareClassList["ObjectList"]=FundShareClass
        jsonStr=json.dumps(FundShareClassList,indent=4)

        FCSOutput=open("./Output/FCSInstrument_Object_List_" + IdentifierStamp + ".json","w")
        FCSOutput.write(jsonStr)        
        FCSOutput.close()

        # Break the GovCorpInstrument list into batches of [GCChunkSize]. Platform limit is 200, but
        # depending on query complexity it may be necesary to request smaller batches
        ChunkedFCS=[]
        ChunkedFCS = Chunk(FundShareClass,FCSChunkSize)
        print(f"There are {len(FundShareClass)} GovCorpInstruments split into {len(ChunkedFCS)} batches")
        StartTimeFCS=datetime.now()
        
        FCSi=0
                
        FCSBatchDetails=[]
        # Step through each batch of ObjectIDs and run graphQL Query
        while FCSi<len(ChunkedFCS):
            FCS_Query={}
            #This is the path/file name of the graphQl query that will be run
            FCS_Query['query']=loadQuery(FundShareClassQuery)
            FCS_Query['variables']={
                "ObjectList":ChunkedFCS[FCSi]
            }
            FCS_GraphQL_Query=json.dumps(FCS_Query)

            StartTimeFCSB=datetime.now()
            FCSGraphQLResponse, fileSize=rdp_Prod_GraphQL.graphQLRequest(FCS_GraphQL_Query,"FundShareClass", FCSi, IdentifierStamp)
            EndTimeFCSB=datetime.now()
            BatchRunTimeFCS=(EndTimeFCSB - StartTimeFCSB).total_seconds()
            batchStatus=None
            for sequence,payload in FCSGraphQLResponse.items():
                if sequence=="errors":
                    errors=FCSGraphQLResponse['errors']
                    errorInfo=errors[0]
                    message=errorInfo['message']
                    extensions=errorInfo['extensions']
                    batchErrorCode=extensions['errorCode']
                    batchStatus="Fail"
            
            if batchStatus!="Fail":
                    batchStatus="Success"
                    batchErrorCode = None

            if batchStatus=="Success":

                FCSBatchDetails.append({
                    "batch": FCSi,
                    "batchGraphQLRunTime": BatchRunTimeFCS,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedFCS[FCSi])
                })
            else:
                 FCSBatchDetails.append({
                    "batch": FCSi,
                    "batchGraphQLRunTime": BatchRunTimeFCS,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedFCS[FCSi]),
                    "objectList": ChunkedFCS[FCSi]
                })               

            batchStatus= None

            FCSi += 1

        EndTimeFCS=datetime.now()

        FCSBatchLog.append({
            "objectType": "FundShareClass",
            "objectCount": len(FundShareClass),
            "chunkSize": FCSChunkSize,
            "totalGraphQLRunTime": (EndTimeFCS - StartTimeFCS).total_seconds(),
            "batchList": FCSBatchDetails
        })
        log["objectTypes"].append({
            "objectType":"FundShareClass",
            "results": FCSBatchLog
        })

        print("\nTotal GraphQL API Run Time for FundShareClass Instrument ", (EndTimeFCS - StartTimeFCS).total_seconds())
        print(f"\n")   



    GCBatchLog=[]
    if GovCorpInstrument:
        #Write the list of GovCorp Instrumdent PermIds to a file for future use
        GCInstrumentList={}
        GCInstrumentList["ObjectList"]=GovCorpInstrument
        jsonStr=json.dumps(GCInstrumentList,indent=4)

        GCOutput=open("./Output/GCInstrument_Object_List_" + IdentifierStamp + ".json","w")
        GCOutput.write(jsonStr)        
        GCOutput.close()

        # Break the GovCorpInstrument list into batches of [GCChunkSize]. Platform limit is 200, but
        # depending on query complexity it may be necesary to request smaller batches
        ChunkedGC=[]
        ChunkedGC = Chunk(GovCorpInstrument,GCChunkSize)
        print(f"There are {len(GovCorpInstrument)} GovCorpInstruments split into {len(ChunkedGC)} batches")
        StartTimeGC=datetime.now()
        
        Gi=0
                
        GCBatchDetails=[]
        # Step through each batch of ObjectIDs and run graphQL Query
        while Gi<len(ChunkedGC):
            GC_Query={}
            #This is the path/file name of the graphQl query that will be run
            GC_Query['query']=loadQuery(GovCorpInstrumentQuery)
            GC_Query['variables']={
                "ObjectList":ChunkedGC[Gi]
                }
            GC_GraphQL_Query=json.dumps(GC_Query)
            print(GC_GraphQL_Query)
            StartTimeGCB=datetime.now()
            GCGraphQLResponse, fileSize=rdp_Prod_GraphQL.graphQLRequest(GC_GraphQL_Query,"Gov", Gi, IdentifierStamp)
            EndTimeGCB=datetime.now()
            BatchRunTimeGCP=(EndTimeGCB - StartTimeGCB).total_seconds()
            batchStatus=None
            for sequence,payload in GCGraphQLResponse.items():
                if sequence=="errors":
                    errors=GCGraphQLResponse['errors']
                    errorInfo=errors[0]
                    message=errorInfo['message']
                    extensions=errorInfo['extensions']
                    batchErrorCode=extensions['errorCode']
                    batchStatus="Fail"
            
            if batchStatus!="Fail":
                    batchStatus="Success"
                    batchErrorCode = None

            if batchStatus=="Success":

                GCBatchDetails.append({
                    "batch": Gi,
                    "batchGraphQLRunTime": BatchRunTimeGCP,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedGC[Gi])
                })
            else:
                 GCBatchDetails.append({
                    "batch": Gi,
                    "batchGraphQLRunTime": BatchRunTimeGCP,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedGC[Gi]),
                    "objectIds": ChunkedGC[Gi]
                })               

            batchStatus= None

            Gi += 1

        EndTimeGC=datetime.now()

        GCBatchLog.append({
            "objectType": "GovCorpInstrument",
            "objectCount": len(GovCorpInstrument),
            "chunkSize": GCChunkSize,
            "totalGraphQLRunTime": (EndTimeGC - StartTimeGC).total_seconds(),
            "batchList": GCBatchDetails
        })
        log["objectTypes"].append({
            "objectType":"GovCorpInstrument",
            "results": GCBatchLog
        })

        print("\nTotal GraphQL API Run Time for GovCorp Instrument ", (EndTimeGC - StartTimeGC).total_seconds())
        print(f"\n")        

    MuniBatchLog=[]
    if MuniInstrument:
        #Write the list of Muni Instrumdent PermIds to a file for future use
        MuniInstrumentList={}
        MuniInstrumentList["ObjectList"]=MuniInstrument
        jsonStr=json.dumps(MuniInstrumentList,indent=4)

        MuniOutput=open("./Output/MuniInstrument_Object_List_" + IdentifierStamp + ".json","w")
        MuniOutput.write(jsonStr)
        
        MuniOutput.close()

        ChunkedMuni=[]
        ChunkedMuni = Chunk(MuniInstrument,MuniChunkSize)
        print(f"There are {len(MuniInstrument)} Muni Instruments split into {len(ChunkedMuni)} batches")
        StartTimeMuni=datetime.now()
        
        Mi=0
                
        MuniBatchDetails=[]

        while Mi<len(ChunkedMuni):
            Muni_Query={}
            #This is the path/file name of the graphQl query that will be run
            Muni_Query['query']=loadQuery(MuniInstrumentQuery)
            Muni_Query['variables']={
                "ObjectList":ChunkedMuni[Mi]
                }
            Muni_GraphQL_Query=json.dumps(Muni_Query)

            StartTimeMuniB=datetime.now()
            MuniGraphQLResponse, fileSize=rdp_Prod_GraphQL.graphQLRequest(Muni_GraphQL_Query,"Muni", Mi, IdentifierStamp)
            EndTimeMuniB=datetime.now()
            BatchRunTimeMuni=(EndTimeMuniB - StartTimeMuniB).total_seconds()
            batchStatus=None
            for sequence,payload in MuniGraphQLResponse.items():
                if sequence=="errors":
                    errors=MuniGraphQLResponse['errors']
                    errorInfo=errors[0]
                    message=errorInfo['message']
                    extensions=errorInfo['extensions']
                    batchErrorCode=extensions['errorCode']
                    batchStatus="Fail"
            
            if batchStatus!="Fail":
                    batchStatus="Success"
                    batchErrorCode = None

            if batchStatus=="Success":

                MuniBatchDetails.append({
                    "batch": Mi,
                    "batchGraphQLRunTime": BatchRunTimeMuni,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedMuni[Mi])
                })
            else:
                 MuniBatchDetails.append({
                    "batch": Mi,
                    "batchGraphQLRunTime": BatchRunTimeMuni,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedMuni[Mi]),
                    "ObjectList": ChunkedMuni[Mi]
                })               

            batchStatus= None

            Mi += 1

        EndTimeMuni=datetime.now()

        MuniBatchLog.append({
            "objectType": "MuniInstrument",
            "objectCount": len(MuniInstrument),
            "chunkSize": MuniChunkSize,
            "totalGraphQLRunTime": (EndTimeMuni - StartTimeMuni).total_seconds(),
            "batchList": MuniBatchDetails
        })
        log["objectTypes"].append({
            "objectType":"MuniInstrument",
            "results": MuniBatchLog
        })

        print("\nTotal GraphQL API Run Time for GovCorp Instrument ", (EndTimeMuni - StartTimeMuni).total_seconds())
        print(f"\n")

    # if FundShareClass:

    EDBatchLog=[]
    if EDInstrument:
        #Write the list of EDInstrumdent PermIds to a file for future use
        EDInstrumentList={}
        EDInstrumentList["ObjectList"]=EDInstrument
        jsonStr=json.dumps(EDInstrumentList,indent=4)

        EDOutput=open("./Output/EDInstrument_ObjectList_"+IdentifierStamp+".json","w")
        EDOutput.write(jsonStr)
        
        EDOutput.close()
        
        ChunkedED=[]
        ChunkedED = Chunk(EDInstrument,EDChunkSize)
        print(f"There are {len(EDInstrument)} EDInstruments split into {len(ChunkedED)} batches")
        StartTimeED=datetime.now()
        
        Ei=0
                
        EDBatchDetails=[]

        while Ei<len(ChunkedED):
            ED_Query={}
            #This is the path/file name of the graphQl query that will be run
            ED_Query['query']=loadQuery(EDInstrumentQuery)
            ED_Query['variables']={
                "ObjectList":ChunkedED[Ei]
                }
            ED_GraphQL_Query=json.dumps(ED_Query)

            StartTimeEDB=datetime.now()
            EDGraphQLResponse, fileSize=rdp_Prod_GraphQL.graphQLRequest(ED_GraphQL_Query,"EDF", Ei,IdentifierStamp)
            EndTimeEDB=datetime.now()
            BatchRunTimeEDP=(EndTimeEDB - StartTimeEDB).total_seconds()
            batchStatus=None
            for sequence,payload in EDGraphQLResponse.items():
                if sequence=="errors":
                    errors=EDGraphQLResponse['errors']
                    errorInfo=errors[0]
                    message=errorInfo['message']
                    extensions=errorInfo['extensions']
                    batchErrorCode=extensions['errorCode']
                    batchStatus="Fail"
            
            if batchStatus!="Fail":
                    batchStatus="Success"
                    batchErrorCode = None

            if batchStatus=="Success":

                EDBatchDetails.append({
                    "batch": Ei,
                    "batchGraphQLRunTime": BatchRunTimeEDP,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedED[Ei])
                })
            else:
                 EDBatchDetails.append({
                    "batch": Ei,
                    "batchGraphQLRunTime": BatchRunTimeEDP,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedED[Ei]),
                    "objectIds": ChunkedED[Ei]
                })               

            batchStatus= None

            Ei += 1

        EndTimeED=datetime.now()

        EDBatchLog.append({
            "objectType": "EDInstrument",
            "objectCount": len(EDInstrument),
            "chunkSize": EDChunkSize,
            "totalGraphQLRunTime": (EndTimeED - StartTimeED).total_seconds(),
            "batchList": EDBatchDetails
        })
        log["objectTypes"].append({
            "objectType":"EDFInstrument",
            "results": EDBatchLog
        })

        print("\nTotal GraphQL API Run Time for EDInstrument ", (EndTimeED - StartTimeED).total_seconds())
        print(f"\n")        
   
    SPBatchLog=[]
    if SPInstrument:
        #Write the list of SPInstrumdent PermIds to a file for future use
        SPInstrumentList={}
        SPInstrumentList["Objects"]=SPInstrument
        jsonStr=json.dumps(SPInstrumentList,indent=4)

        SPOutput=open("./Output/SPInstrument_Object_List_"+IdentifierStamp+".json","w")
        SPOutput.write(jsonStr)
        
        SPOutput.close()

        ChunkedSP=[]
        ChunkedSP = Chunk(SPInstrument,SPChunkSize)
        print(f"There are {len(SPInstrument)} SPInstruments split into {len(ChunkedSP)} batches")
        StartTimeSP=datetime.now()
        
        Si=0
                
        SPBatchDetails=[]

        while Si<len(ChunkedSP):
            SP_Query={}
            #This is the path/file name of the graphQl query that will be run
            SP_Query['query']=loadQuery(SPInstrumentQuery)
            SP_Query['variables']={
                "ObjectList":ChunkedSP[Si]
                }
            SP_GraphQL_Query=json.dumps(SP_Query)

            StartTimeSPB=datetime.now()
            SPGraphQLResponse, fileSize=rdp_Prod_GraphQL.graphQLRequest(SP_GraphQL_Query,"SP", Si,IdentifierStamp)
            EndTimeSPB=datetime.now()
            #rdp_Prod_GraphQL_Processing.SPInstrumentGQL(SPGraphQLResponse)
            BatchRunTimeSBP=(EndTimeSPB - StartTimeSPB).total_seconds()
            batchStatus=None
            for sequence,payload in SPGraphQLResponse.items():
                if sequence=="errors":
                    errors=SPGraphQLResponse['errors']
                    errorInfo=errors[0]
                    message=errorInfo['message']
                    extensions=errorInfo['extensions']
                    batchErrorCode=extensions['errorCode']
                    batchStatus="Fail"
            
            if batchStatus!="Fail":
                    batchStatus="Success"
                    batchErrorCode = None

            if batchStatus=="Success":

                SPBatchDetails.append({
                    "batch": Si,
                    "batchGraphQLRunTime": BatchRunTimeSBP,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedSP[Si])
                })
            else:
                 SPBatchDetails.append({
                    "batch": Si,
                    "batchGraphQLRunTime": BatchRunTimeSBP,
                    "batchStatus": batchStatus,
                    "batchErrorCode": batchErrorCode,
                    "fileSize": fileSize,
                    "objectCount": len(ChunkedSP[Si]),
                    "objectIds": ChunkedSP[Si]
                })               

            batchStatus= None

            Si += 1

        EndTimeSP=datetime.now()

        SPBatchLog.append({
            "objectType": "SPInstrument",
            "objectCount": len(SPInstrument),
            "chunkSize": SPChunkSize,
            "totalGraphQLRunTime": (EndTimeSP - StartTimeSP).total_seconds(),
            "batchList": SPBatchDetails
        })

        print("\nTotal GraphQL API Run Time for SPInstrument ", (EndTimeSP - StartTimeSP).total_seconds())
        print(f"\n")

        log["objectTypes"].append({
            "objectType":"SPInstrument",
            "results": SPBatchLog
        })

    OutputFile="./Logs/GraphQL_Log_"+ IdentifierStamp +".json"
    print(f"Log output to : {OutputFile}\n")
    with open(OutputFile,"w") as outFile:
        json.dump(log,outFile,indent=4)
    outFile.close

    rdp_Prod_Log_Processing.log_file_conversion(log, IdentifierStamp)
 
#====================================

# This function accepts the file path/name of a .json file, opens the file, and reads the file into an object that is
# then returned back to the script that called this function. Its main use is to load a graphQL query into an object so that it
# can then be sent to the graphQL API.     
def loadQuery(graphQLQueryFile):
    fileobj=open(graphQLQueryFile)
    outstr=fileobj.read()
    fileobj.close()

    return outstr

#============

# This function takes a list - OriginalList, and breaks it into chunks specified by Size. It is used to split the ObjectID lists 
# generated when the output from a Symbollogy request is processed into the chunks of 'Size' defined by the scipt that generates
# the graphQL request. Depending on the graphQL quey being used, it is necessary to break the requests into chunks less than the
# platform limit of 200 objects per graphQL request. 
def Chunk (OriginalList, Size):

    ChunkedList=[]
    ChunkedList=[OriginalList[i:i+Size] for i in range(0,len(OriginalList),Size)]

    return ChunkedList