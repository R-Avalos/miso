##### Download process, improved, historical

library(tidyverse)
library(lubridate)
library(RCurl)
# Using R for ETL, because why not?
# download files to directory
# then load, transform, and push to a local database

# Variables
miso_historical_url_directory <-  "https://docs.misoenergy.org/marketreports/"



##################################
### Base Level Functions  #######
################################

miso_file_date_format_func <- function(start_date = "2019-01-01", 
                                        end_date = "2019-01-02") {
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
} #download file if it does not exist



extract_transform_load_rt <- function(filename,
                                      write_directory){
  ## Hardcoded transformations
  transformed_df <- read_csv(paste0("./data/rt_lmp/", filename),
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
    mutate(date = test$date[1],
           data_transform_date = Sys.Date()
    ) %>%
    gather(key = hour_text, value = price, `HE 1`:`HE 24`) %>%
    separate(hour_text, into = c("test", "hour_numeric"), sep = " ", remove = FALSE) %>%
    mutate(temp = mapply(function(x, y) paste0(rep(x, y), collapse = ""), 0, 2 - nchar(hour_numeric))) %>%
    mutate(hour_numeric = as.numeric(paste0(temp, hour_numeric))) %>%
    mutate(datetime = ymd_hm(paste0(date, " ", hour_numeric, ":00"))) %>%
    arrange(Node, date, hour_numeric) %>%
    mutate(rt_source = "rt_lmp_final") %>%
    mutate(date = ymd(date)) %>%
    select(Node, Type, Value, date, datetime, rt_price = price, hour_text, hour_numeric, rt_source, data_transform_date)
  
  print(paste0(filename, " transformed and saving to ", write_directory))
  write_csv(transformed_df, path=paste0(write_directory, filename), Encoding('UTF-8')) # write transformed file
  
}




##################################
## Second order functions  ######
################################

# Download
miso_download_historical_real_time <- function(start_date = "2019-01-01",
                                                   end_date = "2019-01-01",
                                                   path = "data/rt_lmp",
                                                   file_suffix = "_rt_lmp_final.csv",
                                                   url_prefix = miso_historical_url_directory) {
  dir.create(path, showWarnings = FALSE, recursive = TRUE) # create directory if does not exist, else nothing

  df <- miso_file_date_format_func(start_date = start_date, end_date = end_date) # list of dates
  df$rt_lmp_final <- paste0(url_prefix, df$date, file_suffix) # build urls
  sapply(df$rt_lmp_final, download_if_not_exist, path=path) # download files
}

# Transform and save to sub directory
miso_transform_real_time_within_directory <- function(read_path = "./data/rt_lmp/", 
                                                      write_path = "./data/rt_lmp/transformed/",
                                                      min_date = "2019-01-01",
                                                      max_date = "2019-12-31") {
  # create sub directory to write transformed csv files
  dir.create(write_path, showWarnings = FALSE, recursive = TRUE)
  
  # List files in read directory
  list_files_into_df <- list.files(path = read_path, pattern = ".csv") %>%
    as.data.frame() %>%
    select(filename = 1) %>%
    mutate(filename = as.character(filename)) %>%
    separate(filename, into = c("date", "file_type"), sep = 8, remove = FALSE) %>%
    mutate(date = ymd(date)) %>%
    subset(date >= min_date & date <= max_date)
  
  # List already transformed files in write directory
  already_transformed_files <- list.files(path = write_path, pattern = ".csv") %>%
    as.data.frame() %>%
    select(filename = 1) %>%
    mutate(filename = as.character(filename)) %>%
    separate(filename, into = c("date", "file_type"), sep = 8, remove = FALSE) %>%
    mutate(date = ymd(date)) %>%
    subset(date >= min_date & date <= max_date)

    print(paste0("Of the ", length(list_files_into_df$filename), " non-transformed files selected, ", length(already_transformed_files$filename), " have already been transformed"))
  
  # remove any files already transformed from list of files to transform
  list_files_into_df <- list_files_into_df %>%
    anti_join(already_transformed_files, 
              by = "filename")
  
  list_files_into_df <- list_files_into_df %>%
    separate(filename, into = c("date", "file_type"), sep = 8, remove = FALSE) # this separate at 8.... seems fragile, no?
  print(paste0("Converting ", length(list_files_into_df$filename), " files from wide to long format"))
  
  lapply(list_files_into_df$filename, extract_transform_load_rt, write_directory =  write_path) # extract, transform, load to directory
  
}





### Add for day ahead files ####

##################################
## Testing                 ######
################################

miso_download_historical_real_time(start_date = "2019-01-01",
                                   end_date = "2019-01-04")



miso_transform_real_time_within_directory()



