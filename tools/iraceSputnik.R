# detect missing packages and install them
neededPackages <- c("irace", "RJSONIO", "RCurl", "argparser")
newPackages <- neededPackages[!(neededPackages %in% installed.packages()[,"Package"])]
if(length(newPackages))
{
  install.packages(newPackages)
}

library(irace)
library(RJSONIO)
library(RCurl)
library(argparser) # for CLI options


cliParamList <- function(values, switches) {
  stopifnot(length(values) == length(switches))
  params <- vector()  
  for (i in seq_along(values)) {
    value <- values[[i]]
    if (!is.na(value)) {
      params <- c( params, switches[[i]], value )
    }
  }
  return(params)
}


candidateData <- function(candidate, listIndex, instance, extra.params)
{
  data <- vector("list")
  data[["list-index"]] <- listIndex
  data[["instance"]] <- instance
  data[["parameters"]] <- c( extra.params, cliParamList(candidate$values, candidate$labels) )  
  
  return(data)
}

trim <- function(s)
{
  return(sub(" +$", "", sub("^ +", "", s)))
}

createTaskDataList <- function(candidates, instance, extra.params)
{
  additionalParams <- unlist(strsplit(trim(extra.params), " "))
  
  lapply( 1:length(candidates),
          function(i)
          {
            cand <- candidates[[i]]
            return( candidateData( cand, i, instance, additionalParams) )
          })
}


checkException <- function(context, data)
{  
  if( !is.null( data[["exception"]] ) )
  {
    print( data )
    stop( paste( "Exception on ", context, ":\n", data[["exception"]], "\nException on ", context, " (see above)", sep="" ) )
  }
}


request <- function(user, password, url, method, content = NULL)
{  
  # options for self-signed certificates: ssl.verifypeer = FALSE, ssl.verifyhost = FALSE
  if( is.null(content) )
    # request
    tryCatch({
      response <- getURL( url=url, customrequest=method, httpheader=c('Content-Type'='application/json'),
                          ssl.verifypeer = FALSE, ssl.verifyhost = FALSE, username=user, password=password )
    },
    error = function(e) {
      message(sprintf("%s %s request without content failed with the following error:", method, url))
      message(e)
      stop(e)
    })
  else{
    # JSON encoding
    tryCatch({
      jsonContent <- toJSON(content)
    },
    error = function(e) {
      message(sprintf("%s %s request: JSON encoding of content failed with the following error:", method, url))
      message(e)
      message("Tried to encode the following data:")
      message(content)
      stop(e)
    })
    # request
    tryCatch({
      response <- getURL( url=url, customrequest=method, httpheader=c('Content-Type'='application/json'),
                          ssl.verifypeer = FALSE, ssl.verifyhost = FALSE, username=user, password=password,
                          postfields=jsonContent )  
    },
    error = function(e) {
      message(sprintf("%s %s request with content failed with the following error:", method, url))
      message(e)
      stop(e)
    })    
  }
              
  #responseData <- NULL
  # JSON decoding
  tryCatch({
    responseData <- fromJSON( response )  
  },
  error = function(e) {
    message(sprintf("%s %s request: JSON decoding of response failed with the following error:", method, url))
    message(e)
    message("Tried to decode the following JSON data:")
    message(response)
    stop(e)
  })
  
  return(as.vector(responseData, mode="list"))
}


submitCandidates <- function(user, password, serviceURL, jobFunction, taskDataList)
{
  submitURL <- paste(serviceURL, "/irace/submit", sep="")
  jobData <- list( "fn" = jobFunction, "task-data-list" = taskDataList )
  resultData <- request( user, password, submitURL, "POST", jobData )
  
  checkException( "candidate submission", resultData )
    
  return(resultData[["job-id"]])
}


getProgress <- function(user, password, serviceURL, jobID)
{
  progressURL <- paste(serviceURL, "/irace/", jobID, "/progress", sep="")
  resultData <- request(user, password, progressURL, "GET")
  
  checkException( "progress query", resultData )
  
  return(resultData[["finished"]])
}


getResults <- function(user, password, serviceURL, jobID)
{
  resultsURL <- paste(serviceURL, "/irace/", jobID, "/results", sep="")
  resultData <-  request(user, password, resultsURL, "GET")
  
  checkException( "result query", resultData )
  
  return(resultData[["results"]])
}


removeJob <- function(user, password, serviceURL, jobID)
{
  removeURL <- paste(serviceURL, "/irace/", jobID, sep="")
  resultData <-request(user, password, removeURL, "DELETE")
  
  checkException( "job removal", resultData )
}



sputnikHookRunParallel <- function(serviceURL, user, password, sputnikFunction, sleepDuration, candidates, hook.run, instance, extra.params, config)
{
  # submit candidate evaluation tasks
  jobID <- submitCandidates( user, password, serviceURL,  sputnikFunction, createTaskDataList( candidates, instance, extra.params ) )
  
  # while evaluation is not finished
  while( !getProgress( user, password, serviceURL, jobID ) )
  {
    # wait the specified duration before querying again
    Sys.sleep( sleepDuration )
  }
    
  # evaluation is done, retrieve the result data
  results <- getResults( user, password, serviceURL, jobID )
  
  # build a list of the candidate costs in the same order as the candidate list
  output <- vector("list", length(candidates))
  for (res in results) {
    idx <- res[["list-index"]] # original list index of the candidate
    val <- res[["result-data"]][["costs"]]
    output[[idx]] <- val
  }
  
  # remove the job data on the rest server
  removeJob( user, password, serviceURL, jobID )
  
  return (output)
}



iraceSputnik.main <- function()
{
  ap <- arg.parser("iraceSputnik -- Parameter tuning with irace using Sputnik for distributed, parallel evaluation")
  
  ap <- add.argument(ap, "tuneconf", help="irace tune-conf file")
  ap <- add.argument(ap, "serviceURL", help="URL of the Sputnik REST service (hostname and custom port, if any), e.g. http://localhost:12345")
  ap <- add.argument(ap, "evalFunction", help="The function that Sputnik will use for the evaluation.(It is run with a map containing the instance URI and the candidate parameters.)")
  ap <- add.argument(ap, "--sleep", short="-s", default=0.5, type="numeric", help="irace tune-conf file")
  ap <- add.argument(ap, "--credfile", short="-c", default="pw.txt", help="File containing the credentials (username and password, one per line) to access the Sputnik service")
  ap <- add.argument(ap, "--parameters", short="-p", help="File containing the parameter specification. This is either used to specify the parameters file or override the one given in the tuneconf.")
  ap <- add.argument(ap, "--results", short="-r", help="Specifies the filename where the R dataset of the tuning results shall be saved.")
  
  options <- parse.args(ap, commandArgs(trailingOnly = TRUE))
  
  print("using the following options:")
  print( options )
  
  configurationFile <- options$tuneconf
  serviceURL <- options$serviceURL
  sputnikFunction <- options$evalFunction
  sleepDuration <- options$sleep
  
  credfile <- file(options$credfile, "r", blocking = FALSE)
  lines <- readLines( credfile, 2 )
  close(credfile)
  
  user <- lines[1]
  password <- lines[2]
  
  # build hook function with given options
  hookClosure <- function(candidates, hook.run, instance, extra.params, config)
  {
    sputnikHookRunParallel(serviceURL, user, password, sputnikFunction, sleepDuration, candidates, hook.run, instance, extra.params, config)
  }
  
  # read in configuration file
  tunerConfig <- readConfiguration(configurationFile)
  # add hooRunParallel function
  tunerConfig[["hookRunParallel"]] <- hookClosure
  # if given, override parameterFile
  if( ! is.na(options$parameters) )
    tunerConfig[["parameterFile"]] <- options$parameters
  # if given, override logFile containing the R dataset
  if( ! is.na(options$results) )
    tunerConfig[["logFile"]] <- options$results

  # run irace
  irace.main(tunerConfig) 
}