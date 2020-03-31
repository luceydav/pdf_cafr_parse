
# Libraries
library(paws.machine.learning)
library(paws.common)
library(paws.storage)
library(rlist)
library(data.table)

# Set AWS system credentials if needed
Sys.setenv(AWS_ACCESS_KEY_ID = "")
Sys.setenv(AWS_SECRET_ACCESS_KEY = "")
Sys.setenv(AWS_REGION = "us-east-1")

# Save file to S3 Bucket
s3 <- 
  s3( 
    config = list(
      credentials = list(
        creds = list(
          access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
          secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY")
        )
      ),
      region = Sys.getenv("AWS_REGION")
    )
  )
# List your buckets
s3$list_buckets()

# Create bucket
bucket_name <- "masscafr"
s3$create_bucket(
  Bucket = bucket_name
)

# Load the file as a raw binary
file_name <- "joined.pdf"
read_file <- file(file_name, "rb")
s3_object <- 
  readBin(read_file, "raw", n = file.size(file_name))

# Put object in bucket
s3$put_object(
  Body = s3_object,
  Bucket = bucket_name,
  Key = file_name
)

#https://www.r-bloggers.com/an-amazon-sdk-for-r/
# Set up Amazon Textract object
# Need to set
svc <- 
  textract( 
    config = list(
      credentials = list(
        creds = list(
          access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
          secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY")
        )
      ),
      region = Sys.getenv("AWS_REGION")
    )
  )

# Run Textract on "cafr-parse" S3 bucket to extract from "subset.pdf"
# Textract function is "start_document_analysis" which asynsychroniously for PDF
# Output is JobID used for "get_document_analysis"
# Feature type is set to "TABLES"
JobId <- 
  svc$start_document_analysis(
    Document = list(
      S3Object = list(
        Bucket = "masscafr",
        Name = "joined.pdf"
    )
  ),
  FeatureTypes = list(
    "TABLES"
  )
)

# Extract block objects with paws "get_document_analysis"
# a object is nested list which can be saved as json
#https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/python/example_code/textract/textract_python_table_parser.py
#https://docs.aws.amazon.com/textract/latest/dg/textract-dg.pdf#how-it-works-tables

# Get 1st list of blocks and "NextToken"
a <- 
  svc$get_document_analysis(JobId= unlist(JobId))
cafr <- list(a[["Blocks"]])
nt_1 <- nt <- a[["NextToken"]]

# Stop looping when 1st NextToken repeats
repeat {
  
  # Call get_document_analysis to get another 1000 blocks
  a <- svc$get_document_analysis(
    JobId = unlist(JobId),
    NextToken = nt
  )
  
  # Append cafr list with new blocks
  cafr <- 
    list.append(cafr, a[["Blocks"]])
  
  # Update "NextToken"
  nt <- a[["NextToken"]] 
  
  # Stop when 1st token repeats
  if(nt == nt_1) break
}

# Move blocks one list level higher
cafr <- purrr::flatten(cafr)

# Reconstitute the blocks by Page
cafr <- list.group(cafr, Page)

cafr <- cafr[2:length(cafr)]

final <- 
  # Apply parse_json_table.py by table to convert to csv
  mclapply(cafr, function(list_item) {
    
    # Save table to disc as json
    list.save(list_item, file = "textract_obj_subset.json", type = "json")

    # Call parse_json_table.py in bash from working dir
    system("python3 parse_json_table.py 'textract_obj_subset.json' 'result.csv'")
  
    # Load parsed csv as data.table
    fs <- fread("result.csv", check.names = FALSE)
  
    # Return csv
    return(fs)
  
  })


# Cleanup
s3$delete_object(Bucket = bucket_name, Key = file_name)
s3$delete_bucket(Bucket = bucket_name)
#close(read_file)
#file.remove(file_name)
