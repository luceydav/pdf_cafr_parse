

library(xlsx)
library(pdftools)

# Save to workbook with each list element as tab
wb <- createWorkbook()
names(final) <- as.character(1:15)
sheetnames <- names(final)
sheets <- lapply(sheetnames, createSheet, wb = wb)
void <- Map(addDataFrame, final, sheets)
saveWorkbook(wb, file = "sample_cafr.xlsx")

pdf_subset(
  "joined.pdf",
  pages = c(1:15),
  output = "subset.pdf"
)