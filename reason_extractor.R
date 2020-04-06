

library(pdftools)
library(stringr)
library(data.table)
library(rlist)
library(pipeR)
library(parallel)
#library(readxl)


# Clean raw character output after tabula
# Remove punctuation, convert to numeric
# Clean aand set names
clean_table <- function(page) {
  
  # Get names function to clean up names
  get_names <- function(page) {
    
    # Extract incomplete column names resulting from faulty tabula 
    names <- t(colnames(page))
    names <- 
      str_replace(tolower(names),"town.*|x\\.\\d|statement.*", "")
    names[1] <- ""
    names <- matrix(names, nrow=1)
    
    # Bottom of header
    flags <- c("\\$", "assets", "liabilities", "revenues", "activities")
    patterns <- paste(flags, collapse="|")
    if (any(str_detect( tolower(as.vector(t(page[1:10,]))), patterns))) {
      top_bottom <-
        min(which(apply(page, 1, function(row) any(str_detect(row, "\\$|\\,\\d{3}"))))) -1
    }
    if(top_bottom > 1) { 
      page <- page[1:top_bottom]
    }
    
    # Clean up above header
    if(any(apply(page[1:nrow(page)], 1, function(row) any(str_detect(row[!is.na(row)], "2018"))))) {
      header_top <- 
        min(which(apply(page, function(row) any(str_detect(row, "2018")), MARGIN=1))) +1
      page <- page[header_top:nrow(page)] 
    }
    
    # Drop empty rows
    any_empty <- 
      which(apply(page[,-1], 1, function(row) all(!str_detect(row, ""))))
    if(length(any_empty) > 0 ) {
      page <- page[-any_empty]
    }
    
    # Convert to matrix of first few rows and rbind with names
    page <- as.matrix(page)
    if (any(str_detect(names, "\\w"))) {
      page <-
        rbind(names, page)
    }
    
    # Build new names by pasting together rows by column
    if(ncol(page) > 2) { 
      page <- page[,-1] 
      names <- 
        lapply(1:ncol(page), function(col) {
          v <- t(page[,col])
          return(v)
        })
      names <- 
        sapply(names, function(name){
          paste(name, collapse=" ")
        })
    } else { names <- paste(page[, -1], collapse = " ") }
    
    return(names)
  }
  
  #Run get names function to convert dt rows to vector
  names <- c("element", get_names(page))
  if (length(names) == ncol(page)) { 
    names(page) <- names 
  } else {
    #cat(“Caught an error during fread, trying to set names\n”)
    names(page) <- c("element", rep("error", ncol(page)-1))
  }
  
  # Clean names to snake case
  page <- janitor::clean_names(page)
  
  # Drop empty rows
  page <- page[element != ""]
  
  # Clean and convert to numeric
  if(ncol(page) > 2) {
    num <- names(page)[2:ncol(page)]
    page[, (num) := lapply(.SD, function(col) {
      col[1] <- str_remove(col[1], "\\w*")
      col <- str_replace(col, "-", NA_character_)
      col <- str_remove(col, " ")
      col <- readr::parse_number(col, c("NA"))
      col
    }), .SDcols=num]
  } else { 
    num <- names(page)[2:ncol(page)]
    page[, (num) := lapply(.SD, readr::parse_number), .SDcol=num]
  }
  
  # Clean punctuation from rownames
  page[, element := str_remove(element, "\\W[\\s\\.\\$]*$")]
  
  # Return
  return(page)
  
}

# Run clean_table on tables of first five munis
cleaned <- 
  
  table %>>%
  
  list.map(x ~ lapply(x, try(clean_table)))


np <-
  cleaned %>>% 
  list.map(x ~ 
             x[unlist(lapply(x, function(page) {
               any(str_detect(tolower(page$element), "liabilities")) & 
                 any(str_detect(tolower(page$element), "pension"))
             }))]) 

np <-
  cleaned %>>% 
  list.map(x ~ 
             x[unlist(lapply(x, function(page) {
               any(str_detect(tolower(page$element), "total revenues"))
             }))]) 

np <- 
  np[lapply(np, length)>0]

np<- mapply(function(muni, names) { 
  names(muni) <- names 
  muni },
  np, names(np))

#np <- lapply(np, function(page) {
#  page[which(str_detect(tolower(element), "liability"))]
#})

np <- 
  rbindlist(np, fill=TRUE, idcol="muni")

np <- 
  np[str_detect(tolower(element), "total revenues")]
np <- 
  np[!str_detect(tolower(element), "deferred|related")]

totals <- names(np)[str_detect(names(np), "total")]
total_all <- c("element","muni", totals[-6])
np <- np[, .SD, .SDcols=total_all]
np[, muni := str_remove(muni, "\\..*$")]
#np <- np[,c(1:3)]
np <- np[, (totals):= lapply(.SD, as.numeric), .SDcols=totals]
np[, total := fcoalesce(.SD), .SDcols=totals[-6]]

comp <- 
  reason[,.(muni = tolower(as.character(municipality)), 
            total_revenues = as.numeric(total_revenues))  ]
comps <- np[comp, on="muni"]
comps[, diff:= total - abs(as.numeric(total_revenues))]

xlsx::write.xlsx(comps, "mass_unrestricted.xls", sheetName="Sheet1")


net_position <- readxl::read_excel("mass_net_position.xls",sheet = "Sheet1")
net_position <- setDT(net_position)
missing_np <- net_position[is.na(diff)]$muni

unrestricted <- readxl::read_excel("mass_unrestricted.xls", sheet="Sheet1")
unrestricted <- setDT(unrestricted)
missing_unres <- unrestricted[is.na(diff)]$muni

intersect(missing_np, missing_unres)

