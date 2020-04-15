


setwd("/Users/davidlucey/Desktop/David/Projects/pdf_cafr_parse")
system("python3 trptest.py textract_obj_subset.json")

json_file = getURL("https://raw.githubusercontent.com/isrini/SI_IS607/master/books.json")

json_file2 = RJSONIO::fromJSON("textract_obj_subset.json")
json_file3 <- jsonlite::read_json("textract_obj_subset.json", simplifyVector = TRUE)
json_file3 %>%
  as.tbl_json()

library(jsonlite)
# read url and convert to data.frame
document <- fromJSON(txt="textract_obj_subset.json")
df <- as.data.frame(document)

head(json_file2)

setwd("/Users/davidlucey/Desktop/David/Projects/pdf_cafr_parse")
jsonFile <- jsonlite::fromJSON("textract_obj_subset.json")
page <- setDT(jsonFile)
page[BlockType == "LINE", .(paste(Text, sep=" ")), Geometry.Top]



for (line in a$Blocks[16:1000]) {
  if (line$BlockType == "LINE") {
    y <- line$Geometry$BoundingBox$Left
    if (y == line$Geometry$BoundingBox){
      row <- cbind(row, paste0(line$Text,","))
    }
    print(row)
  }
}

library(rjson)

# You can pass directly the filename
#my.JSON <- fromJSON(file="test.json")

df <- lapply(a$Blocks[16:1000], function(play) # Loop through each "play"
{
  # Convert each group to a data frame.
  # This assumes you have 6 elements each time
  data.frame(matrix(unlist(play), ncol=, byrow=T))
})

# Now you have a list of data frames, connect them together in
# one single dataframe
df <- do.call(rbind, df)

# Make column names nicer, remove row names
colnames(df) <- names(my.JSON[[1]][[1]])
rownames(df) <- NULL

df
