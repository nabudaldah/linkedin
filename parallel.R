
# Nabi Abudaldah, April 9th, 2016 - parallel.R - template for robust parallel data processing in R

# Load libraries

library(magrittr)
library(dplyr)
library(tidyr)
library(data.table)
library(doParallel)

# Fire up R instances ready for parallel processing
cores <- detectCores()
cl <- makeCluster(cores, outfile = './outfile.txt')
clusterEvalQ(cl, {
  library(magrittr)
  library(dplyr)
  library(tidyr)
  library(data.table)
  print('R instance ready.')
})

# Your work function (EDIT THIS)
work <- function(item){
  df <- read.csv(...)
  return(df)
}

# Add work function to R instances
clusterExport(cl, list('work'))

# Make list of items to process (we look for CSV files in the 'data' folder)
items <- dir('./data', 'csv')

# Now execute work for every item!
results <- parLapply(cl, items, function(item){

  # Print to outfile.txt our progress
  print(item)
  
  # Try-Catch our work
  result <- tryCatch({
    
    # Check cache
    if(file.exists(paste0('./cache/', item))) {
      
      # Load item from cache
      df <- load(paste0('./cache/', item))
      
    } else {
      
      # Process item and add to cache
      df <- work(item)
      save(df, file = paste0('./cache/', item))
      
    }
    
    # Finish with results
    return(list(item = item, result = df,   error = ''))
    
  }, error = function(err){
    
    # Something went wrong, finish with error
    return(list(item = item, result = NULL, error = err$message))
    
  })
  
  # Finish this item
  return(result)
  
})



# Stop all R instances
tryCatch({ stopCluster(cl) }, error = warning)

# Bind all separate dataframes into one big data frame
data <- results %>%
  Filter(function(result) result[['error']] == '', .) %>%
  Map(function(result) result[['result']], .) %>%
  rbindlist

# Gather all errors from the results
errors <- results %>%
  Filter(function(result) result[['error']] != '', .) %>%
  Map(function(result) result[['error']], .) %>%
  rbindlist
