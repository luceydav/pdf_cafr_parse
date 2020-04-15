
library(stringr)

# Apply download.file to url to download specified pdfs, rename and save in data/pdf_cafr folder
year <- "2018"
lapply(cities_url, function(city) {
  try(download.file(
    paste0("https://cafr.file.core.windows.net/cafr/General%20Purpose/2018/MA%20",city,"%202018.pdf?sv=2017-07-29&ss=f&srt=sco&sp=r&se=2120-03-06T21:34:56Z&st=2020-03-06T21:34:56Z&spr=https&sig=lFqyP8wwH1giyaFhjj6lCVuaAZ9xgbSnjEtzEtpusgA%3D",collapse=""),
    destfile = 
      paste0(
        "data/pdf_cafr/",
        tolower(str_replace(city, "\\%20", "_")),
        "_", year,
        ".pdf"),
    mode = "wb"))})

