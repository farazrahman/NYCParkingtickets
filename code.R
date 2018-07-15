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


#5. You’d want to find out the properties of parking violations across different times of the day:
The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.

Find a way to deal with missing values, if any.

Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. 
For each of these groups, find the 3 most commonly occurring violations. Now, try another direction. 


#Adding M at the end of the violation time string

df2015$Violation_Time <- rpad(df2015$Violation_Time, 6, "M")


#Converting the violation time in timestamp format
df2015$Violation_Time <- to_timestamp(df2015$Violation_Time, format = "hhmmaaa")

#Making a new column by extracting hour of the day from the violation timestamp.
df2015 <- mutate(df2015, hr = hour(df2015$Violation_Time))

#Some timestamps have NA values, hence, querying the rows having into a dataframe, where violation hour is not NULL

createOrReplaceTempView(df2015, "df1")


dfviolationtime15 <- SparkR::sql("SELECT * from df1 where hr is NOT NULL")

# Binning the violation hour into 6 different bins

createOrReplaceTempView(dfviolationtime15, "df2")

bins <- sql("SELECT Summons_Number, hr,Violation_Code, 
                   
		CASE 
                   
		WHEN hr >= 1  and hr <= 4  THEN '1AM-4AM'
                   
		WHEN hr >= 5   and hr <= 8 THEN '5AM-8AM'
                   
		WHEN hr >= 9 and  hr <= 12 THEN '9AM-12NOON'
                   
		WHEN hr >= 13 and hr <= 16 THEN '1PM-4PM'
                   
		WHEN hr >= 17 and hr <= 20 THEN '5PM-8PM'
                   
		WHEN hr >= 21 and hr <= 24 THEN '9PM-12MIDNIGHT'
                   
		ELSE 5 END  as bin_number FROM df2")

#Finding top3 Violation codes for time bin 1AM-4AM
bin1to4AM <- filter(bins, bins$bin_number == "1AM-4AM")


bin1to4AM_code <- summarize(groupBy(bin1to4AM, bin1to4AM$Violation_Code),
                            
                            
			count = n(bin1to4AM$Violation_Code))





head(arrange(bin1to4AM_code, desc(bin1to4AM_code$count)), num = 3)


Violation_Code 		count
             21 	37251
             40 	35906
             78 	31803



#Finding top3 Violation codes for time bin 5AM-8AM
bin5to8AM <- filter(bins, bins$bin_number == "5AM-8AM")


bin5to8AM_code <- summarize(groupBy(bin5to8AM, bin5to8AM$Violation_Code),
                            
                            
			count = n(bin5to8AM$Violation_Code))





head(arrange(
bin5to8AM_code, desc(
bin5to8AM_code$count)), num = 3)


Violation_Code  count
             21 467614
             14 202496
             20 124232




#Finding top3 Violation codes for time bin 9AM-12NOON
bin9AMto12NOON <- filter(bins, bins$bin_number == "9AM-12NOON")

bin9AMto12NOON_code <- summarize(groupBy(bin9AMto12NOON, bin9AMto12NOON$Violation_Code),
                            
                            
			count = n(bin9AMto12NOON$Violation_Code))





head(arrange(
bin9AMto12NOON_code, desc(
bin9AMto12NOON_code$count)), num = 3)


Violation_Code  count
             21 921280
             38 507053
             36 386238




#Finding top3 Violation codes for time bin 1PM-4PM
bin1PMto4PM <- filter(bins, bins$bin_number == "1PM-4PM")

bin1PMto4PM_code <- summarize(groupBy(bin1PMto4PM, bin1PMto4PM$Violation_Code),
                            
                            
			count = n(bin1PMto4PM$Violation_Code))





head(arrange(bin1PMto4PM_code, desc(bin1PMto4PM_code$count)), num = 3)


Violation_Code  count
             38 532648
             37 404105
             14 283451



#Finding top3 Violation codes for time bin 5PM-8PM
bin5PMto8PM <- filter(bins, bins$bin_number == "5PM-8PM")

bin5PMto8PM_code <- summarize(groupBy(bin5PMto8PM, bin5PMto8PM$Violation_Code),
                            
                            
			count = n(bin5PMto8PM$Violation_Code))





head(arrange(bin5PMto8PM_code, desc(bin5PMto8PM_code$count)), num = 3)


Violation_Code  count
             38 155321
              7 122553
             37  97692




#Finding top3 Violation codes for time bin 9PM-12MIDNIGHT
bin9PMto12MIDNIGHT <- filter(bins, bins$bin_number == "9PM-12MIDNIGHT")

bin9PMto12MIDNIGHT_code <- summarize(groupBy(bin9PMto12MIDNIGHT, bin9PMto12MIDNIGHT$Violation_Code),
                            
                            
			count = n(bin9PMto12MIDNIGHT$Violation_Code))





head(arrange(bin9PMto12MIDNIGHT_code, desc(bin9PMto12MIDNIGHT_code$count)), num = 3)


Violation_Code count
              7 47992
             38 34226
             14 33584



###For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)


#Finding most common violation bin for top Violation codes 21
code21 <- filter(bins, bins$Violation_Code == "21")

code21_bin <- summarize(groupBy(code21, code21$bin_number),
                            
                            
			count = n(code21$bin_number))





head(arrange(code21_bin, desc(code21_bin$count)))



bin_number  count
 9AM-12NOON 921280
    5AM-8AM 467614
    1AM-4AM  37251
    1PM-4PM   4380
          7   3689
    5PM-8PM    570


#Finding most common violation bin for top Violation codes 38
code38 <- filter(bins, bins$Violation_Code == "38")

code38_bin <- summarize(groupBy(code38, code38$bin_number),
                            
                            
			count = n(code38$bin_number))





head(arrange(code38_bin, desc(code38_bin$count)))



bin_number  count
        1PM-4PM 532648
     9AM-12NOON 507053
        5PM-8PM 155321
        5AM-8AM  70516
 9PM-12MIDNIGHT  34226
        1AM-4AM    634

#Finding most common violation bin for top Violation codes 14
code14 <- filter(bins, bins$Violation_Code == "14")

code14_bin <- summarize(groupBy(code14, code14$bin_number),
                            
                            
			count = n(code14$bin_number))





head(arrange(code14_bin, desc(code14_bin$count)))


 bin_number  count
        1PM-4PM 283451
     9AM-12NOON 277898
        5AM-8AM 202496
        5PM-8PM  76482
 9PM-12MIDNIGHT  33584
        1AM-4AM  20010

#6. Let’s try and find some seasonality in this data

#First, divide the year into some number of seasons, and find frequencies of tickets for each season.

#Then, find the 3 most common violations for each of these season

df2015 <- mutate(df2015, quarter = quarter(df2015$Issue_Date))
seasons <- summarize(groupBy(df2015, df2015$quarter),                                  
                                      count = n(df2015$quarter))
head(arrange(seasons, desc(seasons$count)))

 quarter   count
       2 2870263
       3 2788963
       1 2466640
       4 2435101

####Three most common Violations for quarter 1

Qtr1 <- filter(df2015, df2015$quarter == 1)

violationQtr1 <- summarize(groupBy(Qtr1, Qtr1$Violation_Code),
                                   
count = n(Qtr1$Violation_Code))


head(arrange(violationQtr1, desc(violationQtr1$count)), num = 3)


 Violation_Code  	count
             38 	336746
             21 	281386
             14 	219828

####Three most common Violations for quarter 2


Qtr2 <- filter(df2015, df2015$quarter == 2)
violationQtr2 <- summarize(groupBy(Qtr2, Qtr2$Violation_Code), 
		count = n(Qtr2$Violation_Code))
head(arrange(violationQtr2, desc(violationQtr2$count)), num = 3)

Violation_Code  count
             21 431255
             38 322629
             14 243485

####Three most common Violations for quarter 3

Qtr3 <- filter(df2015, df2015$quarter == 3)

violationQtr3 <- summarize(groupBy(Qtr3, Qtr3$Violation_Code),
                                   
count = n(Qtr3$Violation_Code))


head(arrange(violationQtr3, desc(violationQtr3$count)), num = 3)


Violation_Code  count
             21 397809
             38 348466
             14 234565

####Three most common Violations for quarter 4

Qtr4 <- filter(df2015, df2015$quarter == 4)

violationQtr4 <- summarize(groupBy(Qtr4, Qtr4$Violation_Code),
                                   
count = n(Qtr4$Violation_Code))


head(arrange(violationQtr4, desc(violationQtr4$count)), num = 3)


Violation_Code  count
             21 350517
             38 292637
             14 207365


####7.The fines collected from all the parking violation constitute a revenue source for the NYC police department. 
#Let’s take an example of estimating that for the 3 most commonly occurring codes.
#Find total occurrences of the 3 most common violation codes
#Then, search the internet for NYC parking violation code fines. 
#You will find a website (on the nyc.gov URL) that lists these fines. 
#They’re divided into two categories, one for the highest-density locations of the city, 
#the other for the rest of the city. For simplicity, take an average of the two.
#Using this information, find the total amount collected for all of the fines. 
#State the code which has the highest total collection.
#What can you intuitively infer from these findings?

violation_code <- summarize(groupBy(df2015, df2015$Violation_Code),
               
			count = n(df2015$Violation_Code))



head(arrange(violation_code, desc(violation_code$count)),num = 3)

	Violation_Code   count
1             21 	1460967
2             38 	1300478
3             14  	905243

Average fine for Violation_Code 21 is $55
Average fine for Violation_Code 38 is $50
Average fine for Violation_Code 14 is $115

Amount generated for Violation_Code 21 = 55*1460967 = $80,353,185
Amount generated for Violation_Code 38 = 50*1300478 = $65,023,900
Amount generated for Violation_Code 14 = 115*905243 = $104,102,945

