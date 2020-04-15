

# Libraries
library(pdftools)
library(tabulizer)
library(rlist)

# Code for the following steps:

# 1. Take nested list object from reason_pdf_cafr.R
# 2. extract pages of key tables from names of table list 
# 3. extract only those pages from pdf by city cafr in dir
# 4. merge tables into new pdf and save in path

# Directory holding pdfs
dir <- 
  "/Users/davidlucey/Desktop/David/Projects/mass_munis/data/pdf_cafr/"
files <- list.files(dir)

# Create cities vector of all cities in pdf_cafr
cities <- str_remove(files, "_2018.pdf")
pdfs <- paste0(dir, cities, "_2018.pdf")

# Dir to put pdf subset in
path <- 
  "/Users/davidlucey/Desktop/David/Projects/pdf_cafr_parse/"

# Set up joined.pdf so doesn't fail when merged in mapply loop
pdf_subset(
  pdfs, 
  pages = 3, 
  output = paste0(path, "joined.pdf")
  )

# Mapply to extract key pages using table list and table list names
mapply(function(pages, city) {
  
  # Pdf index for extraction converted to integer vector
  pages <- as.integer(names(pages))
  
  # City
  city <- str_replace(city, " ", "_")
  
  # Dir in/Path out
  dir <- 
    "/Users/davidlucey/Desktop/David/Projects/mass_munis/data/pdf_cafr/"
  path <- 
    "/Users/davidlucey/Desktop/David/Projects/pdf_cafr_parse/"
  
  # Build pdf references with dir and suffix
  pdf <- 
    paste0(dir, city, "_2018.pdf")
  
  # Make sure page in range
  num_pages <- pdf_length(pdf)
  pages <- pages[pages <= num_pages]
  
  # Subset pages needed using pdftools function
  pdf_subset(
    pdf,
    pages = pages,
    output = paste0(path, "subset.pdf")
    )
  
  # Merge joined.pdf and subset.pdf using tabulizer function
  merge_pdfs(
    c(paste0(path, "joined.pdf"), 
      paste0(path, "subset.pdf")), 
    outfile=paste0(path, "joined.pdf")
    )
  
}, table, names(table)
)
