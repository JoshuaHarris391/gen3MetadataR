parser <- Gen3MetadataParser$new("/Users/harrijh/keys/acdc_api_key_test.json")

# Create an instance (just like in Python!)
parser <- Gen3MetadataParser$new("path/to/your/keyfile.json")

# Authenticate
parser$authenticate()

# Fetch some data
parser$fetch_data("program1", "AusDiab_Simulated", "demographic")

# Convert to data.frames
parser$data_to_pd()

# Access your data
head(parser$data_store_pd[["program1/AusDiab_Simulated/demographic"]])
