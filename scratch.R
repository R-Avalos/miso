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



##################################
## Second order functions  ######
################################

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


miso_download_historical_real_time()




#######
# Read and transform data "real time market" data, as opposed to day ahead market


miso_download_historical_real_time(start_date = "2019-01-01",
                                   end_date = "2019-01-02")




transform_real_time <- function(read_path = "./data/rt_lmp/", 
                                write_path = "./data/rt_lmp/transformed/") {
  
  # List files in directory
  list_files_into_df <- as.data.frame(list.files(path = read_path, pattern = ".csv"))
  colnames(list_files_into_df) <- c("filename")
  list_files_into_df <- list_files_into_df %>%
    separate(filename, into = c("date", "file_type"), sep = 8, remove = FALSE) # this separate at 8.... seems fragile, no?
  
  return(list_files_into_df)
  
  
  
}

test <- transform_real_time()
test


# test <- as.data.frame(list.files(path = "./data/rt_lmp/", pattern = ".csv"))
# test
# 
# colnames(test) <-  c("filename")
# test <- test %>%
#   separate(filename, into = c("date", "file_type"), sep = 8, remove = FALSE)
# 
# test
test$filename
lapply(test$filename, transform_real_time_load_transform, write_directory =  "data/rt_lmp/transformed/")

transform_real_time_load_transform <- function(filename,
                                               write_directory){
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
  
  write_csv(transformed_df, path=paste0(write_directory, filename), Encoding('UTF-8')) # write transformed file
  #print(paste0(filename, " transformed and saved to ", write_directory))
  
}






transformed_test <- read_csv(paste0("./data/rt_lmp/", test$filename[1]),
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

transformed_test

#dir.create("./data/rt_lmp/transformed/", showWarnings = FALSE, recursive = TRUE)## write to sub directory
transformed_test %>%
  select(date) %>%
  distinct()
paste0(transformed_test$date[1],
       "_rt_lmp_final_transformed.csv")

###

















rt_base_files <- list.files(path = "./data/rt_lmp/", pattern = ".csv")
rt_base_files
filename <- list.files(path = "./data/rt_lmp/", pattern = ".csv")
remove(filename)



# miso_transform_rt <- function(read_directory = "data/rt_lmp/",
#                               write_directory = "data/rt_lmp/transformed/",
#                               filename){
#   
#   
#   dir.create(write_directory, showWarnings = FALSE, recursive = TRUE) # create directory if does not exist
#   
#   # tidy up the data frame, miso saves historical in wide format with an annoying header.... *sigh*
#   file <- read_csv(paste0(read_directory, filename),
#                    skip = 8,
#                   col_names = TRUE,
#                   cols(
#                     Node = col_character(),
#                     Type = col_character(),
#                     Value = col_character(),
#                     `HE 1` = col_double(),
#                     `HE 2` = col_double(),
#                     `HE 3` = col_double(),
#                     `HE 4` = col_double(),
#                     `HE 5` = col_double(),
#                     `HE 6` = col_double(),
#                     `HE 7` = col_double(),
#                     `HE 8` = col_double(),
#                     `HE 9` = col_double(),
#                     `HE 10` = col_double(),
#                     `HE 11` = col_double(),
#                     `HE 12` = col_double(),
#                     `HE 13` = col_double(),
#                     `HE 14` = col_double(),
#                     `HE 15` = col_double(),
#                     `HE 16` = col_double(),
#                     `HE 17` = col_double(),
#                     `HE 18` = col_double(),
#                     `HE 19` = col_double(),
#                     `HE 20` = col_double(),
#                     `HE 21` = col_double(),
#                     `HE 22` = col_double(),
#                     `HE 23` = col_double(),
#                     `HE 24` = col_double())
#   ) %>%
#     mutate(date = "20191102",
#            data_transform_date = Sys.Date()
#            ) %>%
#     gather(key = hour_text, value = price, `HE 1`:`HE 24`) %>%
#     separate(hour_text, into = c("test", "hour_numeric"), sep = " ", remove = FALSE) %>%
#     mutate(temp = mapply(function(x, y) paste0(rep(x, y), collapse = ""), 0, 2 - nchar(hour_numeric))) %>%
#     mutate(hour_numeric = as.numeric(paste0(temp, hour_numeric))) %>%
#     mutate(datetime = ymd_hm(paste0(date, " ", hour_numeric, ":00"))) %>%
#     arrange(Node, date, hour_numeric) %>%
#     mutate(rt_source = "rt_lmp_final") %>%
#     select(Node, Type, Value, date, datetime, rt_lmp_price = price, hour_text, hour_numeric, rt_source, data_transform_date)
#   
#   write_csv(file, path=paste0(write_directory, filename), Encoding('UTF-8')) # write transformed file
#   print(paste0(file, " transformed and saved to ", write_directory))
# 
# }

test$filename
sapply(test$filename, miso_transform_rt())


miso_transform_rt(filename = test$filename[1])

#### Need to add checks if file exists in write directory, than skip.... 