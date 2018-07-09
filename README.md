# NYCParkingtickets
Working on SparkR on AWS EMR cluster
#Load the SparkR library and initiate SparkR session 

library(tidyverse)
library(SparkR)
sparkR.session(master = "local")

#Load the three datasets in the s3 bucket corresponding to the year 2015,2016,2017

df2015 <- read.df("s3://faraz-ds-bucket/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", 
                  source = "csv", inferSchema ="true", header = "true")

df2016 <- read.df("s3://faraz-ds-bucket/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", 
                  source = "csv", inferSchema = "true", header = "true")

df2017 <- read.df("s3://faraz-ds-bucket/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", 
                  source = "csv", inferSchema = "true", header = "true")

#Examining the data

head(df2015)

head(df2016)

head(df2017)

#checking number of columns in each data set

ncol(df2015) # 51 columns
ncol(df2016) # 51 columns
ncol(df2017) # 43 columns

#checking number of rows in each dataset

nrow(df2015) #Number of rows 11809233
nrow(df2016) #Number of rows 10626899
nrow(df2017) #Number of rows 10803028

#Checking the structure of the data

str(df2015)
str(df2016)
str(df2017)

#print schema

printSchema(df2015)
printSchema(df2016)
printSchema(df2017)

#visualizing the missing values
summary(df2015)
summary(df2016)
summary(df2017)
