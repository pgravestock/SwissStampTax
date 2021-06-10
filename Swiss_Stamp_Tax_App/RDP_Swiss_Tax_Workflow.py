import RDP_Swiss_Tax_Symbology_Build
import RDP_Swiss_Tax_Symbology_Post
import RDP_Swiss_Tax_GraphQL_Request
 
from datetime import datetime

Portfolio="./Input/GovCorpList_10.csv"

# All files output by this script will embed the same timestamp in the file name to allow the user to match up the outputs from
# the different stages of the sript. 

# The overall purpose of this script is to take a user supplied list of instruments, obtain the InstrumentPermIDs of those
# instruments from the Symbology API, and then request reference data for those instruments using the appropriate reference data
# graphQL queries.

# This scrip will convert a instrument list csv file (defined on row 50) into the format requred by
# the Symbology API, and request the Instrument PermID and Object Type associated with each identifier.

# When the response is received, the script will group the insturment PermIDs by Object Type, and then run a specific reference data
# graphQL query applicable to each Object Type that is returned. The specific graphQL query for each objectType is defined in the 
# rdp_Prod_GraphQL_Requests.RequestRefData function. The output of this script will generate the following files in the ./Output
# directory :

# o the request to the Symbology API in JSON(Symbology_Input_yyyymmdd-hhmmss.json), 
# o the Symbology response in JSON (Symbology_Output_yyyymmdd-hhmmss.json), 
# o a csv file of the response (Symbology_Summary_yyyymmdd-hhmmss.csv),
# o a series of json files containing the reference data graphQL output.

# In the ./Log directory, the script will generate the following files:

# o an audit log detailing the status of all graphQL requests made based on the instrument types contained in the instrument list
# o a csv summary of the audit log

# When the request to graphQL is made for each query, the code defines a 'chunk size' for each query, and will break down the list
# of ObjectIDs into chunks of the defined size. Although the platform limit for the graphQL endpoint is 200 objects, the complexity
# of the query means that a query may time out if too may objects are requested in a single request.

# The output of each objectType is saved as a json file (with the file name identifying the object type), and the response 
# of each batched graphQL request is saved in a file in json format in the ./Output directory.
# 
# Finally a log file of the overall request (all objectTypes and the individual batches) is written to the ./Log directory. This
# file is useful to check whether an individual batch timed out or returned an error. Where there is an error, the list of ObjectIDs
# in that batch is included, to allow the user to manually re-run the query in the API Playground to troubleshoot the query. The query
# can be copied and pasted from the appropriate query file in the ./gql_Queries directory.

#This is timestamp identifier that is used for all output files generated when this script is run.

BatchIdentifier=datetime.now().strftime("%Y%m%d-%H%M%S")

#ConvertInstrumentList takes the insturment list file and converts it into JSON format.
#The function returns the json structure that is then passed to the Symbology API
SymbologyInput = RDP_Swiss_Tax_Symbology_Build.ConvertInstrumentList(Portfolio, BatchIdentifier)

#symbology_v2_api takes the json structure generated above calls the Symbology API. 
#The function returns the json response from the API and the run time of the API request
SymbologyOutput, SymbologyRunTime =RDP_Swiss_Tax_Symbology_Post.symbology_api(SymbologyInput,BatchIdentifier)

#symbology_v2_tocsv takes the json output that was generated by the Symbology API, and converts it into a flat CSV file
#listing the Identifier Type, the Identifier, the ObjectType Name, and the Instrument PermID
RDP_Swiss_Tax_GraphQL_Request.RequestSwissTaxData(SymbologyOutput, BatchIdentifier, SymbologyRunTime)

print(f"\nSymbology / GraphQL Response Complete\n")