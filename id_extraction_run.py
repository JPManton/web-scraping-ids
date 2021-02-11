

# Import ---------------------------------------------------------------------------------------------------------
from bs4 import BeautifulSoup as bs
import requests
from functions import load_from_mssql
from functions import write_to_blob
from functions import exec_stored_proc


# Bring in data from SQL -----------------------------------------------------------------------------------------
qdata = load_from_mssql("select * from playpen.affiliate_GA_affiliate_capable_pages")
qdatatest = qdata["PAGE"]

# Create empty list for new column -------------------------------------------------------------------------------
pageidColumnList = []

# Start loop to scrape pageids from url --------------------------------------------------------------------------
for x in qdatatest:
	x_sel = "https://" + x
	#print(x)

	# Bring in the details of the lionheart youtube channel
	r = requests.get(x_sel)

	# Move the HTML code into a beautiful soup object
	soup = bs(r.content, features= "lxml")
	#print(soup)
	search = soup.find("meta", {"name": "ntv-kv", "key": "pageid"})

	#Extracting things
	if search is not None:
		first_link = search['values']
		#print("First option")
	elif "<body class=" in str(soup):  #soup.find(body)
		first_link = [item for item in soup.find("body")["class"] if item.startswith("page-id-")]
		#print("Second Option")
	elif "<body data-ad-settings=" in str(soup):
		workingstring = soup.find("body")["data-ad-settings"]
		indexNo = workingstring.index("pageid")
		endIndex = workingstring.find('"', indexNo + 10)
		first_link = workingstring[indexNo + 10: endIndex]
		#print("Third Option")
	else:
		first_link = "Not Found"
		#print("Else")

	#print(first_link)
	pageidColumnList.append(first_link)


# Add new list to qdata and tidy up ------------------------------------------------------------------------------
print(pageidColumnList)
qdata["page_id"] = pageidColumnList
qdata["page_id"] = qdata["page_id"].replace("page-id-", None).replace("[", None).replace("]", None)


# Write to blob -------------------------------------------------------------------------------------------------
file_location = "./affiliate_capable_page_IDs.csv"
qdata.to_csv(file_location, index=False, sep='|')

write_to_blob(file_location, 'jon', 'affiliate_capable_page_ids')


# Import blob to storage ----------------------------------------------------------------------------------------
exec_stored_proc("[playpen].[sp_import_from_blob_affiliate_capable_pageids]")

