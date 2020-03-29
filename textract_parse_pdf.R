
# Libraries
library(paws.machine.learning)
library(paws.common)
library(rlist)

# Set AWS system credentials if needed
Sys.setenv(AWS_ACCESS_KEY_ID = "")
Sys.setenv(AWS_SECRET_ACCESS_KEY = "")
Sys.setenv(AWS_REGION = "us-east-1")

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
        Bucket = "cafr-parse",
        Name = "subset.pdf"
    )
  ),
  FeatureTypes = list(
    "TABLES"
  )
)


# Extract block object with paws "get_document_analysis"
# a object is nested list which can be saved as json
#https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/python/example_code/textract/textract_python_table_parser.py
#https://docs.aws.amazon.com/textract/latest/dg/textract-dg.pdf#how-it-works-tables
a <- 
  svc$get_document_analysis(JobId= unlist(JobId))

# Save a as json with rlist list.save
list.save(a, file = "textract_obj.json", type="json")

