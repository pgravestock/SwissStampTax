#import json
import csv
from csv import writer

# This function accepts the json output that is generated when a users graphQL request has been fully processed, and generates
# a csv file that lists the batch details that are included in the json log report. The output is saved in the ./Logs directory

def log_file_conversion(JsonObject,IdentifierStamp):
    
    outputFile="./Logs/Summary_" + IdentifierStamp+".csv"
    logcsv=open(outputFile, 'w')
    logline=[]
    logcsv.write("ObjectType,BatchNumber,BatchSize,FileSize,RunTime,BatchStatus\n")

    for key,value in JsonObject.items():

        if key=="objectTypes":
            batchSummary=JsonObject['objectTypes']
            for batchSummaryList in batchSummary:
                objectType=batchSummaryList['objectType']
                results=batchSummaryList['results']


                for batchSummaryItems in results:
                    batchObjectType=batchSummaryItems['objectType']
                    objectCount=batchSummaryItems['objectCount']
                    chunkSize=batchSummaryItems['chunkSize']
                    totalGraphQLRunTime=batchSummaryItems['totalGraphQLRunTime']
                    batches=batchSummaryItems['batchList']

                    for batch in batches:
                        batchNo=batch['batch']
                        batchGraphQLRunTime=float(batch['batchGraphQLRunTime'])
                        batchStatus=batch['batchStatus']
                        batchErrorCode=batch['batchErrorCode']
                        fileSize=batch['fileSize']
                        objectCount=batch['objectCount']

                        logline.extend((batchObjectType, batchNo, objectCount, fileSize, batchGraphQLRunTime, batchStatus))
                        wr=csv.writer(logcsv,dialect='excel')
                        wr.writerow(logline)
                        logline=[]
    logcsv.close()
    print(f"\n\nLog File {outputFile} Done")
                       

#=======


