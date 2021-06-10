# SwissStampTax
Sample Python scripts and instrument lists for requesting Swiss Stamp Tax Data from the Refinitiv Data Platform via GraphQL

## **Pre-requisites**
In order to run this sample code, the user must have been issued with a UserID for the RDP API Gateway, and have the necessary entitlements assigned to the userID to retrieve Swiss Stamp Tax Data. Please contact your Refinitiv account manager to arrange for the necessary access.

Once a UserID has been issued, the user should follow the instructions in the Welcome Email to set the password. Once the password has been set, the user must log on to the RDP API Playground (https://apidocs.refinitiv.com/) and use the App Key Generator link to create an App Key. The user should copy the app key to the clipboard, and then edit the `rdp_Prod_Authentication_Token` script and populate their UserID, Password into the appropriate variables at the start of the script. The App Key must be pasted into the `CLIENT_ID` variable.

## **Introduction**
The purpose of this package is to demonstrate a potential solution for clients who want to retrieve Swiss Stamp Tax Data for a cross asset portfolio of instruments using the Refinitiv Data Platform. The solution combines the RDP Symbology API and GraphQL endpoints into a workflow that can handle a portfolio of up to 1,500 instruments. The limit of 1,500 insturments is due to the Symbology API having a limit of 1,500 instruments per request, and accomodating instrument lists larger than 1,500 is outside the scope of this sample code.

The overarching python script that should be run by the user is `RDP_Swiss_Tax_Workflow`. This script contains the variable

`Portfolio="./Input/GovCorpList_10.csv"`

that contains the path to the sample portolio that will be used in the demonstration. A number of sample portfolio lists of different sizes and asset types in the fomat the scripts expect can be found in the `./Input` directory and the user in encouraged to experiment with the different portfolio lists. The user can load their own instrument list into this directory as long as it is in the correct csv format - the first column contains a text identifier that defines the type of Identifer - Isin, Sedol Cusip, ValorenNumber, wpk, RIC), the second column contains the identifer.

When the script is run, a timestamp is generated - this time stamp is then incorporated into all file names generated by the process, so that users can idenrtify which particular request a particular file was generated by.

The `RDP_Swiss_Tax_Build` script converts the csv portfolio list into the json structure that the Symbology API requireds in order to return the Instrument PermID associated with each instrument in the portfolio. To discover the full capabilities of the Symbology API and how it can be used, please refer to the documentation in the API Playground for the Symbology API endpoint.

The json output from this script is then passed to the `RDP_Swiss_Tac_Symbology_Post` script, where the users RDP account credentials are authenticated. When the user is authenticated, a token is saved to the users machine, and used for subsequent API requests. The token lasts for 5 minutes, and will be automatically renewed as required. The script calls the RDP Symbology API Endpoint /discovery/symbology/v1/lookup and delivers the json statement genetated in the previous step. 

The json response from the Symbology API - assuming the api returns a HTTP 200 status - is then passed to `RDP_Swiss_Tax_GraphQL.RequestSwissTaxData` where the response is deserialised, storing the resulting Insturment PermIDs in a series of Python Lists - one list per objectType - GovCorpInstrument, FundShareClass, EDInstrument, AbsCmoInstrument, MuniInstrument, SPInstrument, MbsPoolInstrument, MbsTbaInstrument.

If popululated, these lists are then broken down into batches of a size defined in `RequestSwissTaxData`. The defaut batch sizes are defined in the following variables:

`#Batch Sizes for each query`

`FCSChunkSize=200     #FundShareClass BatchSize`

`GCChunkSize=200      #GovCorp Batch Size`

`MuniChunkSize=200    #Muni Batch Size`

`EDChunkSize=200      #EDF Batch Size`

`SPChunkSize=200      #Structurted Product Batch Size`

`AbsCmoChunkSize=200  #Abs CMO Batch Size`

The platform limit for the number of objects a graphQL query can return in a request is 200, however some queries can be sufficiently complex to require a smaller batch size to avoid a time out of the graphQL API. For each batch, a graphQL API payload request is generated with two parameters - 'query' contains a pre-defined graphQL query applicable to the objectType, and the 'variables' parameter contains the list of ObjectIDs that should be used with the query. The queries used in this example are not complex - they do not join between different content sets, or use relationships that rerturn large numbers of related records, meaning that a batch size for each request can be the platform limit - 200 objects.

The underlying Swiss Stamp Tax data is stored in different containers, depending on the asset type of the insturment - this means it is not possible to make a single request for Swiss Stamp Tax data for a cross asset portfolio, but instead make separate requdests for Structured Products, Equities, Government/Corporate Bonds, Funds etc. As a result, the necessary graphQL query is probramatically selected by the code based on what objectType(s) are present in the output from the Symbology API.

A sample grapghQL payload generated by this script is shown below - the `query` element is sourced from files in the `./gql_Queries` directory, and the `ObjectList` in the `variables` element comes from the appropriate Python list that store the Instrument PermIDs for that objectType.

`{"query": "query InstrumentRefData($ObjectList: [String!]) {\n  DerivedContentGovCorpBonds(objectIds: $ObjectList) {\n    ObjectId\n    SwissStampDutyTax {\n      AssetTypeDescription\n      IssuerDomicileText\n      SwissStampDutyFlag\n      TaxationComment\n      TaxationType\n    }\n  }\n}", "variables": {
       "ObjectList": ["15628633304", "15629318046", "15628302734", "15628694651", "15629237436", "192872864800", "15629550055", "15628496144", "15628508916", "15628302729"]}}`

The output of each grahQL query is written to the `./Output` directory as a json file. In an addition to the graphQL output, a log file is generated that captures the status (success or fail) of each batch, the run time of each query, along with the resulting file size and batch size, and written to a log in json format. If the graphQL response contains an error message, this is written to the log along with the list of ObjectIDs in the request, so that the user has the option of running the graphQL query along with the objectID List in API Playground to troubleshoot the issue. This json file is written to the `./Logs` directory.

Finally, the json log is also converted to a csv file for ease of reading. Ths csv file is written to the `./Logs` directory




