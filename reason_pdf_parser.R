

# Libraries
library(pdftools)
library(tabulizer)
library(re2r)
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
    re2_detect(cities, "\\w\\s\\w"),
    paste0(re2_extract(cities, "^\\w+"),
           "%20",
           re2_extract(cities, "\\w+$")),
    cities)

# Create list of all pdfs in data/pdf_cafr
dir <- "/Users/davidlucey/Desktop/David/Projects/mass_munis/data/pdf_cafr/"
files <- list.files(dir)
city <- re2_replace(files, "_2018.pdf","")
pdfs <- 
  paste0(dir, city, "_2018.pdf")

r <- 
  readRDS("mass.RDS")
specs <- r$specs
pdf_list <- r$pdf_list
table <- r$table

rm(r)


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
        
        # Get table top y param for text filtering
        if (any(re2_detect(page$text, "\\$"), parallel = TRUE)) {
          table_top <- min(page$y[min(which(re2_detect(page$text, "\\$")))])
        } else { table_top <- 300 }
        
        # Filter pages with notes to financial statement language at bottom
        ((re2_detect(
          tolower(paste(page$text[page$y %in% tail(unique(page$y), 5)], collapse = " ")), 
          "notes to basic financial statements|accompanying notes|integral part of these financial statements|notes to the financial statements are an integral part of this statement",
          parallel=TRUE
        ) | 
          re2_detect(
            tolower(paste(page$text[page$y %in% head(unique(page$y), 5)], collapse = " ")), 
            "statement of net position|statement of activities|statement of revenues|balance sheet",
            parallel = TRUE
          )
        ) &
            !re2_detect(
              # Then drop pages with these phrases
              tolower(paste(page$text, collapse = " ")), 
              "discussion & analysis|contents|discussion and analysis|fiduciary|enterprise|proprietary|reconciliation|combining|comparative|highlights|budget|non(-)major|trust funds",
              parallel = TRUE
            ))
      }))]
    # Convert to dt
    rpt <- mclapply(rpt, setDT)
    
    # Return 
    rpt
    
  }))

# Name pdf_list by muni
names(pdf_list) <- tolower(cities)

# Filter non tables
pdf_list <- 
  pdf_list %>>% 
  list.map(x ~ x[unlist(mclapply(x, function(dt) {
    lines <- dt[, paste(text, collapse = " "), y][1:10]
    any(re2_detect(tolower(lines), "june\\s\\d{2}\\,\\s201\\d", parallel = TRUE))
  }))]) %>>%
  # Keep only chart pages based on digit/letter ratio > 0.2
  list.map(x ~ x[mclapply(x, function(page) {
    text <- paste(page$text, collapse = " ")
    d <- re2_count(text, "\\d", parallel = TRUE)
    w <- re2_count(text, "[[:alpha:]]", parallel = TRUE)
    d / w
    }) > 0.20])

pdf_list <- mclapply(pdf_list, function(muni){
  names <- as.integer(names(muni))
  mean_names <- mean(names)
  muni[abs(names - mean_names) < 10]
})

# Drop empty (zero length) lists
pdf_list <- 
  pdf_list[unlist(lapply(pdf_list, length)) > 0]

# Get area specifications for each page for Tabula input 
specs <- 
  
  pdf_list %>>%
  
  list.map(x ~ mclapply(x, function(page) {
    #page <- pdf_list[["boston"]][["36"]]
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
    year_lines <- page$y[re2_detect(page$text, "201\\d") & page$space==FALSE]
    year_lines <- year_lines[year_lines < 200]
    table_top <- max(year_lines)
    height_top <- unique(page$height[page$y == table_top])
    top <- table_top + height_top 
    
    # Bottom
    table_bottom <-
      max(page$y[re2_detect(tolower(page$text), "\\$|total")])
    if( table_bottom != max(page$y) ) {
      next_y <- min(page$y[page$y > table_bottom])
      bottom <- max(next_y - 3, table_bottom) }
    else { bottom <- table_bottom }
    
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
    
    #a <- specs[["winchester"]][["14"]]
    #page <- 14
    #city <- gsub(" ", "_", y)
    # city <- "winchester"
    #pdf <- pdfs[146]
    
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
      if( is.data.frame(t) ) {
        t <- setDT(t)
      } else {
        t <- as.data.table(t)
      }
      
      # Select $ or all is.na columns and drop
      if(length(t) > 0) {
        drops <-
          sapply(t, function(col) which(any(re2_detect(col, "^\\$$", parallel = TRUE)) | all(is.na(col))))
        drops <- 
          which(sapply(drops, function(col) sum(col) > 0))
        t[ , (drops) := NULL] 

      }
      
      # Return dt
      t
      
    })
    
    l
    
  }, specs, names(specs))


saveRDS(list(specs=specs,
             table=table,
             pdf_list=pdf_list), 
        "mass.RDS")

