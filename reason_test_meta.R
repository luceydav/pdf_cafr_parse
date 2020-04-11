
# Script to look for patterns in metadata for pdfs which fail to scrape

library(pdftools)

# Build meta dt
meta <- lapply(pdfs, pdf_info)
names(meta) <- cities
keys <- lapply(meta, function(muni){
  key_list <- purrr::flatten(meta$Abington$keys)
})
meta <- 
  rlist::list.join(meta,keys, .name)
meta <- 
  lapply(meta, function(list) list[!str_detect(names(list),"keys")])
meta <- 
  rbindlist(meta, use.names = TRUE, idcol = "muni")
meta <- janitor::clean_names(meta)
meta[, muni := tolower(gsub(" ", "_", muni))]

# Join with compare dt which shows failed pdfs
scraped <- 
  names(compare)[str_detect(names(compare), "_scraped")]
compare[,problem:=as.logical(apply(.SD, 1, function(x) all(x==0))),.SDcols=scraped]
compare[, problem:=as]
test <- meta[compare[,.(muni,problem)], on="muni"]

# Try to predict fail
cols <- names(test)[-c(1,6:10,12:15)]
model <- glm(problem ~ ., test[,..cols],family=binomial(link='logit'), maxit=100)
summary(model)
