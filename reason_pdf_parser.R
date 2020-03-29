

# Libraries
library(pdftools)
library(tabulizer)
library(stringr)
library(data.table)
library(rlist)
library(pipeR)
library(parallel)
#library(readxl)


# Code to download Center for Muni Finance pdfs

# Get city names from Reason spreadsheet
cities <- 
  readxl::read_excel(
    "/Users/davidlucey/Desktop/David/Projects/mass_munis/reason.xlsx",
    sheet = "GSheets Long",
    range = "B1:B151",
    col_names = TRUE)
names(cities) <- cities[1,]
cities <- cities$Entity[2:150]

# Prepare cities for url paste
cities_url <- 
  ifelse(
    str_detect(cities, "\\w\\s\\w"),
    paste0(str_extract(cities, "^\\w+"),
           "%20",
           str_extract(cities, "\\w+$")),
    cities)

# Apply download.file to url to download specified pdfs, rename and save in data/pdf_cafr folder
#year <- "2018"
#lapply(cities_url, function(city) {
#  try(download.file(
#    paste0("https://cafr.file.core.windows.net/cafr/General%20Purpose/2018/MA%20",city,"%202018.pdf?sv=2017-07-29&ss=f&srt=sco&sp=r&se=2120-03-06T21:34:56Z&st=2020-03-06T21:34:56Z&spr=https&sig=lFqyP8wwH1giyaFhjj6lCVuaAZ9xgbSnjEtzEtpusgA%3D",collapse=""),
#    destfile = 
#      paste0(
#        "data/pdf_cafr/",
#        tolower(str_replace(city, "\\%20", "_")),
#        "_", year,
#        ".pdf"),
#    mode = "wb"))})


# Create list of all pdfs in data/pdf_cafr
dir <- "/Users/davidlucey/Desktop/David/Projects/mass_munis/data/pdf_cafr/"
files <- list.files(dir)
city <- str_remove(files, "_2018.pdf")
pdfs <- 
  paste0(dir, city, "_2018.pdf")


# Apply pdf_data function to 1st 75 pages all pdfs in list
# Extract pages of key tables based on regex matches
# Store in nested list for each town with tables
pdf_list <- 
  
  mclapply(pdfs, try(function(pdf){
    
    # Keep only 1st 75 pages
    rpt <- pdf_data(pdf)[1:75]
    
    # Name by index for each muni
    names(rpt) <- 1:length(rpt)
    
    rpt <- 
      rpt[unlist(lapply(rpt, function(page) {
        # Filter pages with notes to financial statement language
        ((str_detect(
          tolower(paste(page$text[page$y %in% tail(unique(page$y), 5)], collapse = " ")), 
          "notes to basic financial statements|accompanying notes|integral part of these financial statements|notes to the financial statements are an integral part of this statement"
        ) |
          # Filter pages with key statemnt names
          str_detect(
            tolower(paste(page$text[page$y %in% head(unique(page$y), 5)], collapse = " ")), 
            "statement of net position|statement of activities|statement of revenues|balance sheet"
          )
        ) & 
          !str_detect(
            # Then drop pages with these phrases
            tolower(paste(page$text, collapse = " ")), 
            "discussion & analysis|contents|discussion and analysis|fiduciary|enterprise|proprietary|reconciliation|combining|comparative|highlights|budget|non(-)major"
          ))
      }))]
    
    # Convert to dt
    rpt <- mclapply(rpt, setDT)
    
    # Drop tables mistakenly picked up on indices far from financial statements
    names <- as.integer(names(rpt))
    mean_names <- mean(names)
    rpt <- rpt[names - mean_names < 10]
    
    # Return 
    rpt
    
  }))

# Name pdf_list by muni
names(pdf_list) <- tolower(cities)


# Get area specifications for each page for Tabula input 
specs <- 
  
  pdf_list[unlist(lapply(pdf_list, length))>0] %>>%
  
  list.map(x ~ mclapply(x, function(page) {
    
    # Convert to dt
    page <- setDT(page)
    
    # Horizontal
    x <- 8.5 * 72
    y <- 11 * 72
    max_x <- max(page$x)
    max_y <- max(page$y)
    orientation <- 
      ifelse(x < max_x, "horizontal", "verticle")
    
    # Top
    table_top <-
      min(page$y[str_detect(page$text, "2018") & page$space==FALSE])
    height_top <- unique(page$height[page$y == table_top])
    top <- table_top + height_top 
    
    # Bottom
    table_bottom <-
      max(page$y[str_detect(page$text, "\\$")])
    height_bottom <- unique(page$height[page$y == table_bottom])
    bottom <- table_bottom + height_bottom
    
    # Convert empty pages to null
    if(table_top == max_y | table_bottom == max_x) { page <- NULL }
    
    # Left
    left <-     
      ifelse( min(page$x) - 30 > 0,
              min(page$x) - 30, 1 )
    
    # Right
    width_max_x <- max(page$width[page$x == max_x])
    right <- 
      max_x + width_max_x + ifelse(orientation == "verticle", 30, 50)
    
    # Area list parameter
    a <- c(top, left, bottom, right)
    
    # Return
    a
    
  })) %>>% 
  
  # Drop null pages
  list.map(x ~ x[!sapply(x, function(x) is.null(x))]) 


# Take pdfs from pdf_cafr file
# Mapply all cities, all tables to tabula extract_tables function
# Store in nested list of lists
table  <- 
  
  mapply(function(x, y) {
    
    # Params from mapply
    a <- x
    page <- as.integer(names(x))
    city <- gsub(" ", "_", y)
    
    # Set up pdf using city
    dir <- 
      "/Users/davidlucey/Desktop/David/Projects/mass_munis/data/pdf_cafr/"
    pdf <-
      paste0(dir, city, "_2018.pdf")
    
    # Loop to apply params in tabulizer
    l <- mapply(function(a, page, pdf) {
      
      # Tabulizer
      if(length(a) == 4) {
        t <- 
          try(extract_tables(pdf, 
                             pages = page, 
                             area = list(a), 
                             guess = F,
                             output = "data.frame"))

      } else { t <- data.frame() }
    
    }, a, page, pdf)
    
    # Loop to clean up and drop unneeded cols
    l <- lapply(l, function(t) {
      
      # Convert to dt
      t <- setDT(t)
      
      # Select $ or all is.na columns and drop
      if(length(t) > 0) {
        drops <-
          sapply(t, function(col) which(any(str_detect(col, "^\\$$")) | all(is.na(col))))
        drops <- 
          which(sapply(drops, function(col) sum(col) > 0))
        t[ , (drops) := NULL] 
      }
      
      # Return dt
      t
      
    })
    
    l
    
  }, specs, names(specs))


# Clean raw character output after tabula
# Remove punctuation, convert to numeric
# Clean aand set names
clean_table <- function(page) {
  
  # Get names function to clean up names
  get_names <- function(page) {
    
    # Extract incomplete column names resulting from 
    # faulty tabula 
    names <- t(colnames(page))
    
    # Find empty rows at top to set to be combined with names
    empty  <-
      min(which(apply(page, function(row)
        ifelse(all(!str_detect(row[-1], "\\w")), 
               TRUE, 
               FALSE), 
        MARGIN = 1)))
    
    # Convert to matrix of first few rows and rbind with names
    page <- as.matrix(page)
    new <- 
      rbind(names, page)
    
    # Drop empty rows
    new <- new[1:empty,]
    
    # Build new names by pasting together rows by column
    names <- sapply(2:ncol(new), function(col) {
      name <- paste(new[, col], collapse = " ")
      name <- str_trim(str_remove(name, "X\\.?\\d?"))
      new[, col] <- name
    })
    names <- c("element", names)
  }
  
  #Run get names function 
  names(page) <- get_names(page)
  
  # Clean names to snake case
  page <- janitor::clean_names(page)
  
  # Drop empty rows
  page <- page[element != ""]
  
  # Clean and convert to numeric
  num <- names(page)[2:ncol(page)]
  page[, (num) := mclapply(.SD, function(col) {
    col[1] <- str_remove(col[1], "\\w*")
    col <- str_replace(col, "-", NA_character_)
    col <- str_remove(col, " ")
    col <- readr::parse_number(col, c("NA"))
    #col <- nafill(col, fill = 0L)
    col
  }), .SDcols=num]
  
  # Clean punctuation from rownames
  page[, element := str_remove(element, "\\W[\\s\\.\\$]*$")]
  
  # Return
  page
}

# Run clean_table on tables of first five munis
cleaned <- 
  
  table[8:20] %>>%
  
  list.map(x ~ lapply(x, clean_table))


page <- table[[7]][2]