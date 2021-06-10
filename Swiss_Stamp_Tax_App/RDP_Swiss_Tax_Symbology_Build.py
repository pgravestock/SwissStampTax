import json
import csv
from datetime import datetime

#This function takes a csv file name supplied by the user, and generates a json structure that is the payload to the 
#Symbology v2 API. As well as returning the JSON structure, a file is written to ./Output that shows the Symbology response
def ConvertInstrumentList(fileObject, IdentifierStamp):
    print(f"\n\n")
    #This List (tuple) contains the accepted Identifier Types. When processing the input file, if an identifier type is found that is not in this list, the row will be skipped
    #and the user notified
    validIdentifiers=("Isin","Cusip","ValorenNumber", "Sedol", "RIC", "LEI","Wpk")
    
    #Lists that will store identifiers of each recognised identifier type
    Isin=[]
    Cusip=[]
    ValorenNumber=[]
    Sedol=[]
    RIC=[]
    Lei=[]
    Wpk=[]

    with open(fileObject, "r", encoding="utf-8-sig") as csv_file:
        csv_reader=csv.reader(csv_file)
        line_count = 0
        error_count = 0
        for row in csv_reader:
            idtype=row[0]
            if idtype in validIdentifiers:
                line_count += 1
                if idtype=="Isin":
                    Isin.append(row[1])
                elif idtype=="Cusip":
                    Cusip.append(row[1])
                elif idtype=='ValorenNumber':
                    ValorenNumber.append(row[1])
                elif idtype=="Sedol":
                    Sedol.append(row[1])
                elif idtype=="RIC":
                    RIC.append(row[1])
                elif idtype=="LEI":
                    Lei.append(row[1])
                elif idtype=="Wpk":
                    Wpk.append(row[1])

            else:
                print(f"Unrecognised identifier type  {row[0]} {row[1]}")
                error_count += 1
    csv_file.close
    print(f"Processing complete. {line_count} identifiers correctly processed, and {error_count} identifiers rejected due to unrecognised Identifier Type\n\n")

    #Build the 'from' python dictionary that will be loaded with the different identifiers.
    data={}
    data["from"]=[]
    if Isin:
        data["from"].append({
            "identifierTypes":["Isin"],
            "values":Isin,    
        })
    if Cusip:
        data["from"].append({
            "identifierTypes":["Cusip"],
            "values":Cusip,
        })
    if ValorenNumber:
        data["from"].append({
            "identifierTypes":["ValorenNumber"],
            "values":ValorenNumber,
        })
    if Sedol:
        data["from"].append({
            "identifierTypes":["Sedol"],
            "values":Sedol,
        })
    if RIC:
        data["from"].append({
            "identifierTypes":["RIC"],
            "values":RIC,
        })
    if Lei:
        data["from"].append({
            "identifierTypes":["LEI"],
            "values":Lei,
        })
    if Wpk:
        data["from"].append({
            "identifierTypes":["Wpk"],
            "values":Wpk,
        })

    data["to"]=[
        {
            "objectTypes": [
                "anyinstrument"
            ],
            "identifierTypes":[
                "PermID"
            ]
        }
    ]
    
    data["type"]="auto"
    data["pageSize"]=line_count

    #Write the output of the Symbology response to a file
    with open('./Output/Symbology_Input_' + IdentifierStamp + '.json',"w") as outFile:
        json.dump(data,outFile,indent=4)
    outFile.close
    
    return json.dumps(data,indent=4)

#===============================