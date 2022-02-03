
![alt text](https://github.com/BillMarkEg/Assignments/blob/main/images.jpg)


#################### Hi Dear !! ###########################
### Gelato Assignment
###  yet another python repo

this repo is about air quality measures across different counties in USA
to download json data https://data.cdc.gov/api/views/cjae-szjv/rows.json?accessType=DOWNLOAD


what this all about ??


The Environmental Protection Agency (EPA) provides air pollution data about ozone and particulate matter (PM2.5) to CDC for the Tracking Network. The EPA maintains a database called the Air Quality System (AQS) which contains data from approximately 4,000 monitoring stations around the country, mainly in urban areas. Data from the AQS is considered the "gold standard" for determining outdoor air pollution. However, AQS data are limited because the monitoring stations are usually in urban areas or cities and because they only take air samples for some air pollutants every three days or during times of the year when air pollution is very high. CDC and EPA have worked together to develop a statistical model (Downscaler) to make modeled predictions available for environmental public health tracking purposes in areas of the country that do not have monitors and to fill in the time gaps when monitors may not be recording data. This data does not include "Percent of population in counties exceeding NAAQS (vs. population in counties that either meet the standard or do not monitor PM2.5)". Please visit the Tracking homepage for this information.View additional information for indicator definitions and documentation by selecting Content Area "Air Quality" and the respective indicator at the following website: http://ephtracking.cdc.gov/showIndicatorsData.action


what this repo contains ??
there are 3 python files as follows :
             1-db class to open/close database 
             2-logger class to log data into elastic search and kafka btw
             3-main code to make an ETL operations with python (my first time to do it with native python so i am open for any advice :) )


darewarehouse mode used is -> star schema why ?? just to keep it simple and there is only one fact table check a sample of it through this link 
 
![alt text](https://github.com/BillMarkEg/Assignments/blob/main/fact_table.png)


kindly note that there is alot to do and this repo is not complete yet.

######################## that's all ################################
######################## Thank You  ################################
