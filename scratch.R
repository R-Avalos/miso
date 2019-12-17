##### Download process, improved, historical

library(tidyverse)
library(lubridate)
library(RCurl)
# Using R for ETL, because why not?
# download files to directory
# then load and transform?

# Variables
historical_url_directory <-  "https://docs.misoenergy.org/marketreports/"


################################
### Base Level Functions
### 

miso_file_date_format_func <- function(start_date = "2019-01-01", 
                                        end_date = "2019-02-01") {
  timeframe_dates <- seq.Date(from = ymd(start_date), to = ymd(end_date), by = "days")
  timeframe <- as.data.frame(timeframe_dates)
  timeframe$date <- as.numeric(format(as.Date(timeframe$timeframe), '%Y%m%d'))
  
  return(timeframe)
}  # Function for required data format from MISO used in following functions


miso_file_date_format_func()



download_if_not_exist <- function(url, refetch=FALSE, path="."){
  dest <- file.path(path, basename(url))
  if(refetch || !file.exists(dest))
    download.file(url, dest)
  else
    print(paste0(url, " ====  Already Downloaded ===="))
  dest
}


#############################
## Second order functions

miso_download_historical_real_time_lmp <- function(start_date = "2019-12-01",
                                                   end_date = "2019-12-12",
                                                   path = "test_data/rt_lmp",
                                                   file_suffix = "_rt_lmp_final.csv",
                                                   url_prefix = historical_url_directory) {
  dir.create(path, showWarnings = FALSE) # create directory if does not exist, else nothing

  df <- miso_file_date_format_func(start_date = start_date, end_date = end_date) # list of dates
  df$rt_lmp_final <- paste0(url_prefix, df$date, file_suffix) # build urls
  sapply(df$rt_lmp_final, download_if_not_exist, path=path) # download files
}


miso_download_historical_real_time_lmp()




#######
# Read and transform data
files <- list.files(path = "test_data/rt_lmp")
files

miso_transform_rt_lmp <- function(filename,
                                  directory = "test_data/rt_lmp/"){
  # tidy up the data frame
  file <- read_csv(paste0(directory, filename),
                   skip = 8,
                  col_names = TRUE,
                  cols(
                    Node = col_character(),
                    Type = col_character(),
                    Value = col_character(),
                    `HE 1` = col_double(),
                    `HE 2` = col_double(),
                    `HE 3` = col_double(),
                    `HE 4` = col_double(),
                    `HE 5` = col_double(),
                    `HE 6` = col_double(),
                    `HE 7` = col_double(),
                    `HE 8` = col_double(),
                    `HE 9` = col_double(),
                    `HE 10` = col_double(),
                    `HE 11` = col_double(),
                    `HE 12` = col_double(),
                    `HE 13` = col_double(),
                    `HE 14` = col_double(),
                    `HE 15` = col_double(),
                    `HE 16` = col_double(),
                    `HE 17` = col_double(),
                    `HE 18` = col_double(),
                    `HE 19` = col_double(),
                    `HE 20` = col_double(),
                    `HE 21` = col_double(),
                    `HE 22` = col_double(),
                    `HE 23` = col_double(),
                    `HE 24` = col_double())
  ) %>%
    mutate(date = "20191102",
           transform_date = Sys.Date()
           ) %>%
    gather(key = hour_text, value = price, `HE 1`:`HE 24`) %>%
    separate(hour_text, into = c("test", "hour_numeric"), sep = " ", remove = FALSE) %>%
    mutate(temp = mapply(function(x, y) paste0(rep(x, y), collapse = ""), 0, 2 - nchar(hour_numeric))) %>%
    mutate(hour_numeric = as.numeric(paste0(temp, hour_numeric))) %>%
    mutate(datetime = ymd_hm(paste0(date, " ", hour_numeric, ":00"))) %>%
    arrange(Node, date, hour_numeric) %>%
    mutate(rt_source = "rt_lmp_final") %>%
    select(Node, Type, Value, date, datetime, rt_lmp_price = price, hour_text, hour_numeric, rt_source, transform_date)
  return(file)

}

miso_transform_rt_lmp(files)


 
test_dl <- read_csv("https://docs.misoenergy.org/marketreports/20191102_rt_lmp_final.csv",
                    skip = 4,
                    col_names = TRUE,
                    cols(
                      Node = col_character(),
                      Type = col_character(),
                      Value = col_character(),
                      `HE 1` = col_double(),
                      `HE 2` = col_double(),
                      `HE 3` = col_double(),
                      `HE 4` = col_double(),
                      `HE 5` = col_double(),
                      `HE 6` = col_double(),
                      `HE 7` = col_double(),
                      `HE 8` = col_double(),
                      `HE 9` = col_double(),
                      `HE 10` = col_double(),
                      `HE 11` = col_double(),
                      `HE 12` = col_double(),
                      `HE 13` = col_double(),
                      `HE 14` = col_double(),
                      `HE 15` = col_double(),
                      `HE 16` = col_double(),
                      `HE 17` = col_double(),
                      `HE 18` = col_double(),
                      `HE 19` = col_double(),
                      `HE 20` = col_double(),
                      `HE 21` = col_double(),
                      `HE 22` = col_double(),
                      `HE 23` = col_double(),
                      `HE 24` = col_double())
                    ) %>%
        mutate(date = "20191102",
        download_date = Sys.Date())
# test_dl

test_dl %>%
  gather(key = hour_text, value = price, `HE 1`:`HE 24`) %>%
  arrange(date, Node, hour_text) %>%
  separate(hour_text, into = c("test", "hour_numeric"), sep = " ", remove = FALSE) %>%
  mutate(temp = mapply(function(x, y) paste0(rep(x, y), collapse = ""), 0, 2 - nchar(hour_numeric))) %>%
  mutate(hour_numeric = as.numeric(paste0(temp, hour_numeric))) %>%
  mutate(datetime = ymd_hm(paste0(date, " ", hour_numeric, ":00"))) %>%
  mutate(rt_source = "rt_lmp_final") %>%
  select(Node, Type, Value, date, datetime, rt_lmp_price = price, hour_text, hour_numeric, rt_source, download_date)


##
historical_realtime_final_download <- function(file_suffix = "_rt_lmp_final.csv",
                                               url_prefix = historical_url_directory,
                                               begin_ymd = "2019-01-01",
                                               end_ymd = "2019-01-10"){
  
  # Setup range of dates converted to date format used
  df <- historical_date_format_func(start_date = begin_ymd,
                                    end_date = end_ymd)
  
  # Create file names to be used with csv download calls
  df$rt_lmp_final <- paste0(url_prefix, df$date, file_suffix)
  
  # download files
  for(i in 1:length(df$rt_lmp_final)) {
    
    downloaded_file <- read_csv(df$rt_lmp_final[i], skip = 4) %>%
      mutate(date = df$timeframe_dates[i],
             cr_dt = Sys.Date()) 
    
    write_csv(downloaded_file, unlist(strsplit(df$rt_lmp_final[i], split = "/"))[5])
  } 
  
  # conclude with print
  print(paste0(length(df$rt_lmp_final), " files downloaded covering dates between ",
               min(df$timeframe_dates), " and ", max(df$timeframe_dates)))
  
} # Function to download real time final between dates 
# should probably update read_csv to be explicit about column types
