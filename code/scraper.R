# Scraper for FTS Ebola data.
library(RCurl)
library(jsonlite)

# ScraperWiki deployed
onSw <- function(p = NULL, d = 'tool/', a = TRUE) {
  if (a == T) return(paste0(d,p))
  else return(p)
}

###################
## Configuration ##
###################
YEAR = 2015

# Loading helper funtions
source(onSw('code/sw_status.R'))  # for changing status in SW
source(onSw('code/write_tables.R'))  # for writing db tables

# Function to fetch all transaction data
# from FTS using appeal IDs.
ftsEmergencySummary <- function(id = NULL, year = NULL) {

  # checking for parameters
  if (is.null(id) & is.null(year)) {
    stop('Please provide an emergency id or year. See documentation for more details. ?fetchEmergencyData')
  }
  if (!is.null(id) & !is.null(year)) {
    stop('Provide either an emergency id OR a year. See documentation for more details. ?fetchEmergencyData')
  }

  ## For emergency ids.
  if (!is.null(id)) {
    cat("--------------------------------\n")
    url = 'http://fts.unocha.org/api/v1/Appeal/id/'  # base url
    for (i in 1:length(id)) {
      m = paste("Emergency summary:", id[i], " | ")
      cat(m)

      query_url = paste0(url, id[i], ".json")
      it <- fromJSON(getURL(query_url))
      if(i == 1) out <- it
      else out <- rbind(out, it)
      cat("done!\n")
    }
    cat("--------------------------------\n")
  }

  ## For years.
  if (!is.null(year)) {
    cat("--------------------------------\n")
    url = 'http://fts.unocha.org/api/v1/Emergency/year/'  # base url
    for (i in 1:length(year)) {
      m = paste("Emergencies for year:", year[i], " | ")
      cat(m)

      query_url = paste0(url, year[i], ".json")
      it <- fromJSON(getURL(query_url))
      if(i == 1) out <- it
      else out <- rbind(out, it)
      cat("done!\n")
    }
    cat("--------------------------------\n")
  }

  return(out)
}


# Function to fetch record-by-record of each
# appeal in FTS.
ftsEmergencyTransactions <- function(id = NULL) {

  # checking for parameters
  if (is.null(id)) {
    stop('Please provide an emergency id or year. See documentation for more details. ?fetchEmergencyData')
  }


  ## For emergency ids.
  cat("--------------------------------\n")
  url = 'http://fts.unocha.org/api/v1/Contribution/emergency/'  # base url
  for (i in 1:length(id)) {
    m = paste("Emergency details:", id[i], "| ")
    cat(m)
    TABLE_NAME = paste0("Emergency_",id[i])

    query_url = paste0(url, id[i], ".json")
    it <- fromJSON(getURL(query_url))
    if (is.data.frame(it)) {
      # write.csv(it, paste0("data/",TABLE_NAME, ".csv"), row.names=F)
      writeTable(it, TABLE_NAME, "scraperwiki", overwrite=TRUE)
    }
    cat("done!\n")
  }
  cat("--------------------------------\n")
}

# Scraperwiki wrapper.
runScraper <- function() {
  fts_summary = ftsEmergencySummary(year=YEAR)
  ftsEmergencyTransactions(fts_summary$id)
}


# Changing the status of SW.
tryCatch(runScraper(),
         error = function(e) {
           cat('Error detected ... sending notification.')
           system('mail -s "FTS contributions scraper failed." luiscape@gmail.com')
           changeSwStatus(type = "error", message = "Scraper failed.")
           { stop("!!") }
         }
)
# If success:
changeSwStatus(type = 'ok')