#Load the SparkR library and initiate SparkR session 



library(ggplot2)

library(stringr)

library(dplyr)

library(tidyr)
library(SparkR)

sparkR.session(master = "local")



#Load the three datasets in the s3 bucket corresponding to the year 2015,2016,2017



df2015 <- read.df("s3://faraz-ds-bucket/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", 
                  
                  source = "csv", inferSchema ="true", header = "true")


colnames(df2015) <- str_replace_all(colnames(df2015),' ','_');

#How often does each violation code occur? (frequency of violation codes - find the top 5)

violation_code <- summarize(groupBy(df2015, df2015$Violation_Code),
                            count = n(df2015$Violation_Code))

head(arrange(violation_code, desc(violation_code$count)))


#How often does each vehicle body type get a parking ticket? 

vehicle_body <- summarize(groupBy(df2015, df2015$Vehicle_Body_Type),
                          count = n(df2015$Vehicle_Body_Type))

head(arrange(vehicle_body, desc(vehicle_body$count)))


#How about the vehicle make? (find the top 5 for both)

vehicle_make <- summarize(groupBy(df2015, df2015$Vehicle_Make),
                          count = n(df2015$Vehicle_Make))

head(arrange(vehicle_make, desc(vehicle_make$count)))

##Find the (5 highest) frequencies of:
#Violating Precincts (this is the precinct of the zone where the violation occurred)

violating_precints <- summarize(groupBy(df2015, df2015$Violation_Precinct),
                                count = n(df2015$Violation_Precinct))

head(arrange(violating_precints, desc(violating_precints$count)))

#Issuing Precincts (this is the precinct that issued the ticket)

issuing_precints <- summarize(groupBy(df2015, df2015$Issuer_Precinct),
                              count = n(df2015$Issuer_Precinct))

head(arrange(issuing_precints, desc(issuing_precints$count)))

#Find the violation code frequency across 3 precincts which have issued the most number of tickets - 
#do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts?

#Top 3 issuer precints, 0,19,18

precint0 <- filter(df2015, df2015$Issuer_Precinct == 0)
precint0ViolationCode <- summarize(groupBy(precint0, precint0$Violation_Code),
                                   count = n(precint0$Violation_Code))

head(arrange(precint0ViolationCode, desc(precint0ViolationCode$count)))


precint19 <- filter(df2015, df2015$Issuer_Precinct == 19)
precint19ViolationCode <- summarize(groupBy(precint19, precint19$Violation_Code),
                                    count = n(precint19$Violation_Code))

head(arrange(precint19ViolationCode, desc(precint19ViolationCode$count)))



precint18 <- filter(df2015, df2015$Issuer_Precinct == 18)
precint18ViolationCode <- summarize(groupBy(precint18, precint18$Violation_Code),
                                    count = n(precint18$Violation_Code))

head(arrange(precint18ViolationCode, desc(precint18ViolationCode$count)))


