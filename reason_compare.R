
# Libraries
library(stringr)
library(data.table)
library(rlist)
library(pipeR)
library(parallel)

# Script to extract key elements and compare with Reason

# Extract Net Position
np <-
  cleaned %>>% 
  list.map(x ~ 
             x[unlist(lapply(x, function(page) {
               (any(str_detect(tolower(page$element), "^liabilities$|^assets$")) &
                 any(str_detect(tolower(page$element), "fund balances?")))
             }))]) 
np <- 
  np[lapply(np, length) ==1]
np<- mapply(function(muni, names) { 
  names(muni) <- names 
  muni },
  np, names(np))
np <- 
  rbindlist(np, fill=TRUE, idcol="muni")

# Extract Fund Balances from Statement of Net Position
fb <- 
  np[str_detect(tolower(element), "unassigned|assigned")]
fb <- 
  fb[,.(muni= str_remove(muni, "\\..*$"), 
        element,
        total =
          fcoalesce(total_governmental_funds,
                   general))]
fb[, element := str_extract(tolower(element), "unassigned|assigned")]
fb <- dcast(fb, muni ~ element, fun.aggregate = sum)

# Extract Statement of Activities
act <-
  cleaned %>>% 
  list.map(x ~ 
             x[unlist(lapply(x, function(page) {
               any(str_detect(tolower(page$element), "activities")) &
                 any(str_detect(tolower(page$element), "public safety|education"))
             }))]) 
act <- 
  act[lapply(act, length) ==1]
act <- mapply(function(muni, names) { 
  names(muni) <- names 
  muni },
  act, names(act))
act <- 
  rbindlist(act, fill=TRUE, idcol="muni")

# Extract Total Expenses from Statment of Activities
exp <- 
  act[str_detect(tolower(element), "^total$|total governmental|total primary"), .(muni, element, expenses)]
exp <- exp[, max(expenses), muni]
exp <- exp[,.(muni= str_remove(muni, "\\..*$"), 
              total_exp = V1)]

# Balance Sheet
bs <-
  cleaned %>>% 
  list.map(x ~ 
             x[unlist(lapply(x, function(page) {
               any(str_detect(tolower(page$element), "net position")) & 
                 any(str_detect(tolower(page$element), "liabilities")) 
             }))]) 
bs <- 
  bs[lapply(bs, length) ==1]
bs<- mapply(function(muni, names) { 
  names(muni) <- names 
  muni },
  bs, names(bs))
bs <- 
  rbindlist(bs, fill=TRUE, idcol="muni")

# Extract Net position data
tnp <- 
  bs[str_detect(tolower(element), "total net position|unrestricted")]
tnp[, element := str_extract(tolower(element), "unrestricted|total net position")]
tnp <- tnp[,.(muni= str_remove(muni, "\\..*$"), 
            element, 
            total = 
              fcoalesce(total,
                       governmental_activities,
                       primary_government_governmental_activities))]
tnp <- dcast(tnp, muni ~ tolower(element))

# Build scraped by joining 
#scraped <- exp[fb, on= "muni"]
scraped <- list(exp, tnp, fb)
scraped <- 
  Reduce(function(table1,table2) merge(table1, table2, by="muni", all=TRUE), scraped)
scraped <- janitor::clean_names(scraped)
names(scraped)[-1] <- 
  paste0(names(scraped)[-1],"_scraped")
scraped[, muni := gsub("\\s","\\_", muni)]

# Prepare Reason spreadsheet for comparison
reason <- 
  readxl::read_excel("~/Desktop/David/Projects/mass_munis/reason.xlsx", 
                     skip = 1)
reason <- setDT(reason)
reason <- janitor::clean_names(reason)
cols <- 
  names(reason)[
    str_detect(names(reason), 
               "entity|element|unassigned_govern|assigned_gove|^total_expend|total_unrestricted|total_net_position")]
reason <- reason[,..cols]
reason[, muni := tolower(gsub("\\s","\\_", entity))]
names(reason) <- str_remove(names(reason), "_fund_balance")
reason[, entity:=NULL]

# Join Reason and scraped dataa
compare <- scraped[reason, on = "muni"]
compare[is.na(compare)] <- 0
compare$diff_assigned <- abs(compare$assigned_scraped) -abs(compare$assigned_governmental)
compare$diff_unassigned <- abs(compare$unassigned_scraped) -abs(compare$unassigned_governmental)
compare$diff_total_exp <- abs(compare$total_exp_scraped) -abs(compare$total_expenditures)
compare$diff_unrestricted <- abs(compare$unrestricted_scraped) -abs(compare$total_unrestricted_net_position)
compare$diff_net_position <- abs(compare$total_net_position_scraped) -abs(compare$total_net_position)


# Save output
xlsx::write.xlsx(compare, "mass_compare.xls", sheetName="Sheet1")

