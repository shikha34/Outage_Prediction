
##########AUTOMATED CODE##################################


#############################################DATA CLEANING##################
#####OMS FLASH ADMS CLEANING

library(data.table)
library(dummies)
library(ORCH)
library(sqldf)
library(stringdist)
library(stringr)
library(dplyr)
library(plyr)
library(h2o)
library(reshape)
library(fastDummies)
library(ROCR)
library(lubridate)
library(changepoint)
library(fastDummies)

ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='uc_ta',all=TRUE) 

ore.ls()
di=ore.pull(flash_interruption_details_part)

gridmaster<-ore.pull(flash_grid_master_part)
pole_mounted1<-ore.pull(bd_with_pole_mounted_ar_cos)
grid_operations_list1<-ore.pull(grid_operations_list)
omss=ore.pull(oms_interruption_old)
oms_adms<-ore.pull(oms_interruption_adms)
load<-ore.pull(scada_avg_old)
loadadms<-ore.pull(scada_daily_peak)
trunk1<-ore.pull(trunk_feeder_zonewise_2)

ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='flatfiles',all=TRUE) 

ore.ls()
master<-ore.pull(feeder_master_april_2018)
annual<-ore.pull(annual_distribution)
miss_oms<-ore.pull(missing_oms)
gis<-ore.pull(wires_lat_long_11kv)

names(master)
#feeder and grid
x <- "a1~!@#$%^&*(){}_+:\"<>?,./;'[]-="
#DI MASTER
master$concatenate1<-paste(master$grid,master$feeder_name)
master$concatenate1=sapply(master$concatenate1,toupper)
master$concatenate1=gsub("GRID","",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("(?!-)[[:punct:]]","",master$concatenate1,perl=TRUE)
master$concatenate1=gsub("$","",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("=","",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("+","",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("~","",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("^","",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("\\s+"," ",master$concatenate1)
master$concatenate1=gsub("  ","",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub(" ","",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("1","ONE",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("2","TWO",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("3","THREE",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("4","FOUR",master$concatenate1,fixed=TRUE)
master$concatenate1=gsub("-III{1}","-THREE",master$concatenate1)
master$concatenate1=gsub("III{1}$","-THREE",master$concatenate1)
master$concatenate1=gsub("-II{1}","-TWO",master$concatenate1)
master$concatenate1=gsub("-I{1}","-ONE",master$concatenate1)
master$concatenate1=gsub("10{1}","TEN",master$concatenate1)
master$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",master$concatenate1)
master$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",master$concatenate1)
master$concatenate1[master$concatenate1==""]<-"MISS"
master$concatenate1[is.na(master$concatenate1)]<-"MISS"


#OMS FEEDER COLUMN IN MASTER DATA

master$oms_updated_satish=sapply(master$oms_updated_satish,toupper)
master$oms_updated_satish=gsub("GRID","",master$oms_updated_satish,fixed=TRUE)
#remove all except -
master$oms_updated_satish=gsub("(?!-)[[:punct:]]","",master$oms_updated_satish,perl=TRUE)
master$oms_updated_satish=gsub("\\s+"," ",master$oms_updated_satish)
master$oms_updated_satish=gsub("  "," ",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish<-gsub("+","",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish<-gsub("&","",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish<-gsub("~","",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish<-gsub("^","",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish<-gsub("=","",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish=gsub(" ","",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish=gsub("1","ONE",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish=gsub("2","TWO",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish=gsub("3","THREE",master$oms_updated_satish,fixed=TRUE)
master$oms_updated_satish=gsub("4","FOUR",master$oms_updated_satish,fixed=TRUE)

master$oms_updated_satish=gsub("-III{1}","-THREE",master$oms_updated_satish)
master$oms_updated_satish=gsub("III{1}$","-THREE",master$oms_updated_satish)
master$oms_updated_satish=gsub("-II{1}","-TWO",master$oms_updated_satish)
master$oms_updated_satish=gsub("-I{1}","-ONE",master$oms_updated_satish)
master$oms_updated_satish=gsub("10{1}","TEN",master$oms_updated_satish)
master$oms_updated_satish<-gsub("FEEDER-1|FEEDER1","FDR-ONE",master$oms_updated_satish)
master$oms_updated_satish<-gsub("FEEDER-2|FEEDER2","FDR-TWO",master$oms_updated_satish)
master$oms_updated_satish[master$oms_updated_satish==""]<-"MISS"
master$oms_updated_satish[is.na(master$oms_updated_satish)]<-"MISS"


#######adms feeder from master data
master$concatenate_adms=sapply(master$concatenate_adms,toupper)
master$concatenate_adms=gsub("GRID","",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("(?!-)[[:punct:]]","",master$concatenate_adms,perl=TRUE)
master$concatenate_adms=gsub("$","",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("=","",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("+","",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("~","",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("^","",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("\\s+"," ",master$concatenate_adms)
master$concatenate_adms=gsub("  ","",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub(" ","",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("1","ONE",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("2","TWO",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("3","THREE",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("4","FOUR",master$concatenate_adms,fixed=TRUE)
master$concatenate_adms=gsub("-III{1}","-THREE",master$concatenate_adms)
master$concatenate_adms=gsub("III{1}$","-THREE",master$concatenate_adms)
master$concatenate_adms=gsub("-II{1}","-TWO",master$concatenate_adms)
master$concatenate_adms=gsub("-I{1}","-ONE",master$concatenate_adms)
master$concatenate_adms=gsub("10{1}","TEN",master$concatenate_adms)
master$concatenate_adms<-gsub("FEEDER-1|FEEDER1","FDR-ONE",master$concatenate_adms)
master$concatenate_adms<-gsub("FEEDER-2|FEEDER2","FDR-TWO",master$concatenate_adms)
master$concatenate_adms[master$concatenate_adms==""]<-"MISS"
master$concatenate_adms[is.na(master$concatenate_adms)]<-"MISS"


##
di$fromdate=as.Date(di$fromdate,format="%Y-%m-%d")
di$entry_year=format(di$date_entry,"%Y")
di$from_year=format(di$fromdate,"%Y")
di$from_day=format(di$fromdate,"%d")
di$from_month=format(di$fromdate,"%b")
di$to_year=format(di$todate,"%Y")

dii<-di
#taking grid and feeder together of di data
names(dii)[names(dii)=="grid_name"]<-"grid_id"
dii<-merge(dii,gridmaster,by="grid_id",all.x=TRUE)
#equipment name: feeder
dii$concatenate1<-paste(dii$grid_name,dii$equipment_name)
dii$concatenate1=sapply(dii$concatenate1,toupper)
dii$concatenate1=gsub("GRID","",dii$concatenate1,fixed=TRUE)
dii$concatenate1=gsub("(?!-)[[:punct:]]","",dii$concatenate1,perl=TRUE)
dii$concatenate1=gsub("\\s+","",dii$concatenate1)
dii$concatenate1=gsub(" ","",dii$concatenate1,fixed=TRUE)
dii$concatenate1<-gsub("+","",dii$concatenate1,fixed=TRUE)
dii$concatenate1<-gsub("&","",dii$concatenate1,fixed=TRUE)
dii$concatenate1<-gsub("~","",dii$concatenate1,fixed=TRUE)
dii$concatenate1<-gsub("^","",dii$concatenate1,fixed=TRUE)
dii$concatenate1<-gsub("=","",dii$concatenate1,fixed=TRUE)
dii$concatenate1<-gsub("^NA","",dii$concatenate1)
dii$concatenate1=gsub("1","ONE",dii$concatenate1,fixed=TRUE)
dii$concatenate1=gsub("2","TWO",dii$concatenate1,fixed=TRUE)
dii$concatenate1=gsub("3","THREE",dii$concatenate1,fixed=TRUE)
dii$concatenate1=gsub("4","FOUR",dii$concatenate1,fixed=TRUE)
dii$concatenate1=gsub("-III{1}","-THREE",dii$concatenate1)
dii$concatenate1=gsub("III{1}$","-THREE",dii$concatenate1)
dii$concatenate1=gsub("-II{1}","-TWO",dii$concatenate1)
dii$concatenate1=gsub("-I{1}","-ONE",dii$concatenate1)
dii$concatenate1=gsub("10{1}","TEN",dii$concatenate1)

dii$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",dii$concatenate1)
dii$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",dii$concatenate1)
dii$concatenate1[dii$concatenate1==""]<-"MISS"
dii$concatenate1[is.na(dii$concatenate1)]<-"MISS"

dii2<-data.frame(unique(dii$concatenate1))
names(dii2)<-"concatenate1"
dii2<-subset(dii2,concatenate1!='MISS')
dii<-subset(dii,concatenate1!='MISS')

dii2$Approx_match<-c()
dii2$Approx_match1<-c()
dii2$Approx_match2<-c()
dii2$Approx_match3<-c()

for( i in 1: NROW(dii2))
{
  dii2$Approx_match[i]<-master$concatenate1[which.min(stringdist(dii2$concatenate1[i],master$concatenate1,method='jaccard',q=2,nthread=100))]
  dii2$Approx_match1[i]<-master$concatenate1[which.min(stringdist(dii2$concatenate1[i],master$concatenate1,method='cosine',q=2,nthread=100))]
  dii2$Approx_match2[i]<-master$concatenate1[which.min(stringdist(dii2$concatenate1[i],master$concatenate1,method='osa',q=2,nthread=100))]
  dii2$Approx_match3[i]<-master$concatenate1[which.min(stringdist(dii2$concatenate1[i],master$concatenate1,method='dl',nthread=100))]
  
}

#merging with original data of dii with dii1

finaldi<-merge(dii,dii2,by="concatenate1",all.x=TRUE)
finaldi1<-subset(finaldi,equipment_type=="FEEDER")
finaldi_1418<-subset(finaldi1,finaldi1$from_year>=2015)
finaldi_1418$inter_dur<-difftime(finaldi_1418$fromdate,finaldi_1418$todate,units="hours")
table(finaldi_1418$equipstatus)
finaldi_1418<-subset(finaldi_1418,equipstatus=="CLOSED")

##OMS
oms11<-omss
oms11$system_executime=strptime(oms11$system_executime,format="%Y-%m-%d %H:%M:%S")
oms11$year<-format(oms11$system_executime,"%Y")
oms11$month<-format(oms11$system_executime,"%b")
oms11$day<-format(oms11$system_executime,"%d")
oms11$system_executime=as.character(oms11$system_executime)
finaloms_1418<-subset(oms11,oms11$year>='2015')
finaloms_1418<-subset(finaloms_1418,cause=='HV')
finaloms_1418$cmi_start_time<-as.character(finaloms_1418$cmi_start_time)
finaloms_1418$creation_time<-as.character(finaloms_1418$creation_time)
finaloms_1418$cmi_start_time=ifelse(is.na(finaloms_1418$cmi_start_time),finaloms_1418$creation_time,finaloms_1418$cmi_start_time)

#calculating closing time 

finaloms_1418$closing_time<-rep(NA,NROW(finaloms_1418))
finaloms_1418$system_executime=as.character(finaloms_1418$system_executime)
finaloms_1418$closing_time<-ifelse(finaloms_1418$type=='close'|finaloms_1418$type=='remove_tag_and_close',finaloms_1418$system_executime,'0')
finaloms_1418$system_executime=strptime(finaloms_1418$system_executime,format="%Y-%m-%d %H:%M:%S")
finaloms_1418$closing_time=strptime(finaloms_1418$closing_time,format="%Y-%m-%d %H:%M:%S")
finaloms_1418$inter_dur<-difftime(finaloms_1418$closing_time,finaloms_1418$cmi_start_time,units="hours")
finaloms_1418<-subset(finaloms_1418,type=="close"|type=="CLOSED"|type=="remove_tag_and_close")


#grid and feeder concatenate.
finaloms_1418$concatenate1<-paste(finaloms_1418$grid_name,finaloms_1418$feeder_name)
finaloms_1418$concatenate1=sapply(finaloms_1418$concatenate1,toupper)
finaloms_1418$concatenate1=gsub("GRID","",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("(?!-)[[:punct:]]","",finaloms_1418$concatenate1,perl=TRUE)
finaloms_1418$concatenate1=gsub("$","",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("=","",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("+","",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("~","",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("^","",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("\\s+"," ",finaloms_1418$concatenate1)
finaloms_1418$concatenate1=gsub("  ","",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub(" ","",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("1","ONE",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("2","TWO",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("3","THREE",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("4","FOUR",finaloms_1418$concatenate1,fixed=TRUE)
finaloms_1418$concatenate1=gsub("-III{1}","-THREE",finaloms_1418$concatenate1)
finaloms_1418$concatenate1=gsub("III{1}$","-THREE",finaloms_1418$concatenate1)
finaloms_1418$concatenate1=gsub("-II{1}","-TWO",finaloms_1418$concatenate1)
finaloms_1418$concatenate1=gsub("-I{1}","-ONE",finaloms_1418$concatenate1)
finaloms_1418$concatenate1=gsub("10{1}","TEN",finaloms_1418$concatenate1)
finaloms_1418$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",finaloms_1418$concatenate1)
finaloms_1418$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",finaloms_1418$concatenate1)
finaloms_1418$concatenate1[finaloms_1418$concatenate1==""]<-"MISS"
finaloms_1418$concatenate1[is.na(finaloms_1418$concatenate1)]<-"MISS"

#MISSING VALUE TREATMENT

annual1<-annual[c("concatenate","gis_id","order_no","zone")]
names(annual1)[1]<-"concatenate1"
names(annual1)[2]<-"gis_id"
names(annual1)[3]<-"reference_label"

#extracting reference label,feedr,grid
annual1$concatenate1=sapply(annual1$concatenate1,toupper)
annual1$concatenate1=gsub("GRID","",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("(?!-)[[:punct:]]","",annual1$concatenate1,perl=TRUE)
annual1$concatenate1=gsub("$","",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("=","",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("+","",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("~","",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("^","",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("\\s+"," ",annual1$concatenate1)
annual1$concatenate1=gsub("  ","",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub(" ","",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("1","ONE",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("2","TWO",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("3","THREE",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("4","FOUR",annual1$concatenate1,fixed=TRUE)
annual1$concatenate1=gsub("-III{1}","-THREE",annual1$concatenate1)
annual1$concatenate1=gsub("III{1}$","-THREE",annual1$concatenate1)
annual1$concatenate1=gsub("-II{1}","-TWO",annual1$concatenate1)
annual1$concatenate1=gsub("-I{1}","-ONE",annual1$concatenate1)
annual1$concatenate1=gsub("10{1}","TEN",annual1$concatenate1)
annual1$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",annual1$concatenate1)
annual1$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",annual1$concatenate1)
annual1$concatenate1[annual1$concatenate1==""]<-"MISS"
annual1$concatenate1[is.na(annual1$concatenate1)]<-"MISS"
#deleting grid and feeder column

#without NA
oms3<-subset(finaloms_1418,concatenate1!='NANA'& concatenate1!='#NA#NA'&concatenate1!='MISS')
#with NA
oms4<-subset(finaloms_1418,concatenate1=='NANA'| concatenate1=='#NA#NA'|concatenate1=='MISS')
#oms4<-data.frame(unique(oms4))
annual1<-data.frame(unique(annual1))
annual1<-subset(annual1,concatenate1!='MISS')
#f<-subset(annual1,conca  tenate1=='MISS')

#annual distribution data where refernce label is missing
annual1$reference_label[annual1$reference_label==""]<-"MISS"
annual1$gis_id[annual1$gis_id==""]<-"MISS"

# vf1<-subset(annual1,reference_label=="MISS")
#deleting cases with reference label missing 
annual2<-subset(annual1,gis_id!='MISS')
drops<-"concatenate1"
#removing conacatenate columns
oms5<-oms4[,!(names(oms4) %in% drops)]
# oms5$reference_label[oms5$reference_label==""]<-"MISS"

#unique gis_id
oms55<-oms5[c("gis_id")]
oms55<-unique(oms55)

annual2<-annual1[c("concatenate1","gis_id")]
annual2<-data.frame(unique(annual2))

#####matching from master data gis id
names(master)[10]<-"gis_id"
mm<-master[c("concatenate1" ,"gis_id")]
mm<-unique(mm)
oms8<-merge(oms55,mm,by="gis_id")
oms8_<-merge(oms5,oms8,by="gis_id") ##MATCHED DATA
names(oms8_)
omstes<-subset(oms8_, select=-c(concatenate1))
oms5$system_executime<-as.character(oms5$system_executime)
oms5$creation_time<-as.character(oms5$creation_time)
oms5$cmi_start_time<-as.character(oms5$cmi_start_time)
oms5$closing_time <-as.character(oms5$closing_time )
omstes$system_executime<-as.character(omstes$system_executime)
omstes$creation_time<-as.character(omstes$creation_time)
omstes$cmi_start_time<-as.character(omstes$cmi_start_time)
omstes$closing_time <-as.character(omstes$closing_time )

##subtractinh from oms5(main missing data) - omstes (that which matched from gis matching from master data)
omsd<-setdiff(oms5, omstes)
oms66<-omsd[c("reference_label")]
oms66<-unique(oms66)

###############################matching from annual distrinution report


annual3<-annual1[c("concatenate1","reference_label")]
annual3<-data.frame(unique(annual3))
oms6<-merge(oms66,annual3,by=c("reference_label"))
#ore.create(oms6,"mapping_dioms")
oms7<-merge(omsd,oms6,by="reference_label")

#not matched from omsd main data and oms7 which is subset
#ore.create(oms6,"mapping_dioms")
oms7<-merge(omsd,oms6,by="reference_label")


#not matched
names(oms7)
oms77<-subset(oms7, select=-c(concatenate1))
oms_nuldata<-setdiff(omsd, oms77)
oms_nuldata<-unique(oms_nuldata)
###MATCHING FROM IMPORTED data from flatfiless : miss_oms
miss1<-miss_oms[c("order_no","grid","feeder")]
miss1<-unique(miss1)
names(miss1)[1]<-"reference_label"
miss1[miss1$grid=="",]<-"miss"
miss1<-subset(miss1,grid!="miss")
oms99<-merge(oms_nuldata,miss1,by="reference_label")
names(oms99)
oms99<-subset(oms99, select=-c(grid_name,feeder_name)) #deleting null values of feeder and grid
names(oms99)[names(oms99)=="grid"]<-"grid_name"
names(oms99)[names(oms99)=="feeder"]<-"feeder_name"

oms99$concatenate1<-paste(oms99$grid_name,oms99$feeder_name)

oms99$concatenate1=sapply(oms99$concatenate1,toupper)
oms99$concatenate1=gsub("GRID","",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("(?!-)[[:punct:]]","",oms99$concatenate1,perl=TRUE)
oms99$concatenate1=gsub("$","",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("=","",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("+","",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("~","",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("^","",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("\\s+"," ",oms99$concatenate1)
oms99$concatenate1=gsub("  ","",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub(" ","",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("1","ONE",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("2","TWO",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("3","THREE",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("4","FOUR",oms99$concatenate1,fixed=TRUE)
oms99$concatenate1=gsub("-III{1}","-THREE",oms99$concatenate1)
oms99$concatenate1=gsub("III{1}$","-THREE",oms99$concatenate1)
oms99$concatenate1=gsub("-II{1}","-TWO",oms99$concatenate1)
oms99$concatenate1=gsub("-I{1}","-ONE",oms99$concatenate1)
oms99$concatenate1=gsub("10{1}","TEN",oms99$concatenate1)
oms99$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",oms99$concatenate1)
oms99$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",oms99$concatenate1)

oms99$concatenate1[oms99$concatenate1==""]<-"MISS"
oms99$concatenate1[is.na(oms99$concatenate1)]<-"MISS"


oms10<-rbind(oms8_,oms7,oms99)
oms10<-unique(oms10)
oms33<-rbind(oms3,oms10)

oms22<-data.frame(unique(oms33$concatenate1))
names(oms22)<-"concatenate1"
oms22$Approx_match<-c()
oms22$Approx_match1<-c()
oms22$Approx_match2<-c()
oms22$Approx_match3<-c()

for( i in 1: NROW(oms22))
{
  oms22$Approx_match[i]<-master$concatenate1[which.min(stringdist(oms22$concatenate1[i],master$oms_updated_satish,method='jaccard',q=2,nthread=100))]
  oms22$Approx_match1[i]<-master$concatenate1[which.min(stringdist(oms22$concatenate1[i],master$oms_updated_satish,method='dl',nthread=100))]
  oms22$Approx_match2[i]<-master$concatenate1[which.min(stringdist(oms22$concatenate1[i],master$oms_updated_satish,method='jw',nthread=100))]
  oms22$Approx_match3[i]<-master$concatenate1[which.min(stringdist(oms22$concatenate1[i],master$oms_updated_satish,method='osa',nthread=100))]
  
}


#merging with original data of omss with oms111


finaloms2<-merge(oms33,oms22,by="concatenate1",all.x=TRUE)
####### OMS data (current)from ADMS ###############

oms_adms$concatenate1<-paste(oms_adms$bic_zgrid1,oms_adms$bic_zfedr_nme)

oms_adms$concatenate1=sapply(oms_adms$concatenate1,toupper)
oms_adms$concatenate1=gsub("GRID","",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("(?!-)[[:punct:]]","",oms_adms$concatenate1,perl=TRUE)
oms_adms$concatenate1=gsub("$","",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("=","",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("+","",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("~","",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("^","",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("\\s+"," ",oms_adms$concatenate1)
oms_adms$concatenate1=gsub("  ","",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub(" ","",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("1","ONE",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("2","TWO",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("3","THREE",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("4","FOUR",oms_adms$concatenate1,fixed=TRUE)
oms_adms$concatenate1=gsub("-III{1}","-THREE",oms_adms$concatenate1)
oms_adms$concatenate1=gsub("III{1}$","-THREE",oms_adms$concatenate1)
oms_adms$concatenate1=gsub("-II{1}","-TWO",oms_adms$concatenate1)
oms_adms$concatenate1=gsub("-I{1}","-ONE",oms_adms$concatenate1)
oms_adms$concatenate1=gsub("10{1}","TEN",oms_adms$concatenate1)

oms_adms$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",oms_adms$concatenate1)
oms_adms$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",oms_adms$concatenate1)

oms_adms$concatenate1[oms_adms$concatenate1==""]<-"MISS"
oms_adms$concatenate1[is.na(oms_adms$concatenate1)]<-"MISS"

omsad1<-data.frame(unique(oms_adms$concatenate1))
names(omsad1)<-"concatenate1"
omsad1$Approx_match<-c()
omsad1$Approx_match1<-c()
omsad1$Approx_match2<-c()
omsad1$Approx_match3<-c()

for( i in 1: NROW(omsad1))
{
  omsad1$Approx_match[i]<-master$concatenate1[which.min(stringdist(omsad1$concatenate1[i],master$concatenate_adms,method='jaccard',q=2,nthread=100))]
  omsad1$Approx_match1[i]<-master$concatenate1[which.min(stringdist(omsad1$concatenate1[i],master$concatenate_adms,method='dl',nthread=100))]
  omsad1$Approx_match2[i]<-master$concatenate1[which.min(stringdist(omsad1$concatenate1[i],master$concatenate_adms,method='jw',nthread=100))]
  omsad1$Approx_match3[i]<-master$concatenate1[which.min(stringdist(omsad1$concatenate1[i],master$concatenate_adms,method='osa',nthread=100))]
  
}


#merging with original data of omss with oms111


finaloms_adms<-merge(omsad1,oms_adms,by="concatenate1",all.x=TRUE)
table(finaloms_adms$bic_zstatus_1)
finaloms_adms<-subset(finaloms_adms,bic_zstatus_1=="Completed"|bic_zstatus_1=="Awaiting")
finaloms_adms$bic_zcls_dte1<-as.Date(finaloms_adms$bic_zcls_dte1,format="%Y-%m-%d")
finaloms_adms<-finaloms_adms[c("concatenate1","Approx_match","bic_zflt_no", "bic_z_zone" ,"bic_zgrid1" ,"bic_zfedr_nme" ,"bic_zopn_dte1","bic_zcls_dte1",
                               "bic_zstatus_1","bic_zevnt_ty","bic_zdurtn","bic_zfalt_typ")]
names(finaloms_adms)
names(finaloms_adms)[names(finaloms_adms)=="bic_zflt_no"]<-"reference_label"
names(finaloms_adms)[names(finaloms_adms)=="bic_z_zone"]<-"zone"
names(finaloms_adms)[names(finaloms_adms)=="bic_zgrid1"]<-"grid"
names(finaloms_adms)[names(finaloms_adms)=="bic_zfedr_nme"]<-"feeder"
names(finaloms_adms)[names(finaloms_adms)=="bic_zopn_dte1"]<-"start_time"
names(finaloms_adms)[names(finaloms_adms)=="bic_zcls_dte1"]<-"end_time"
names(finaloms_adms)[names(finaloms_adms)=="bic_zevnt_ty"]<-"tripping"
names(finaloms_adms)[names(finaloms_adms)=="bic_zdurtn"]<-"inter_dur"
names(finaloms_adms)[names(finaloms_adms)=="bic_zstatus_1"]<-"status"
names(finaloms_adms)[names(finaloms_adms)=="bic_zfalt_typ"]<-"fault_type"

finaloms_adms$year<-format(finaloms_adms$end_time,"%Y")
finaloms_adms$month<-format(finaloms_adms$end_time,"%b")
finaloms_adms$day<-format(finaloms_adms$end_time,"%d")
finaloms_adms$year<-format(finaloms_adms$end_time,"%Y")
finaloms_adms$month<-format(finaloms_adms$end_time,"%b")
finaloms_adms$day<-format(finaloms_adms$end_time,"%d")
finaloms_adms$start_time<-as.character(finaloms_adms$start_time)
finaloms_adms$end_time<-as.character(finaloms_adms$end_time)
table(finaloms_adms$year)
##BInding both data with columns: date,concatenate,type,duration of fault

omsc<-finaloms2[c("concatenate1" ,"reference_label" ,"type" ,"zone","subcause","grid_name" ,"feeder_name"  ,"cmi_start_time" ,"Approx_match" ,"year" ,"month" , "day","closing_time" ,"inter_dur" ,"oms_fault_type" )]
dic<-finaldi_1418[c("concatenate1" ,"sr_no","type","equipment_name","equipstatus" ,"interzonecode" ,"from_year"  ,"from_day" , "from_month"  , "grid_name"  ,"Approx_match" ,"fromdate" ,"todate","inter_dur","fault_type"  )]
names(omsc)
names(dic)
names(omsc)[3]<-"status"

names(omsc)[4]<-"zone"
names(omsc)[5]<-"tripping"
names(omsc)[6]<-"grid"
names(omsc)[7]<-"feeder"
names(omsc)[8]<-"start_time"
names(omsc)[13]<-"end_time"
names(omsc)[15]<-"fault_type"
names(dic)[2]<-"reference_label"
names(dic)[3]<-"tripping"
names(dic)[4]<-"feeder"
names(dic)[5]<-"status"
names(dic)[6]<-"zone"
names(dic)[7]<-"year"
names(dic)[8]<-"day"
names(dic)[9]<-"month"
names(dic)[10]<-"grid"

names(dic)[12]<-"start_time"
names(dic)[13]<-"end_time"

finaloms_adms$start_time<-as.character(finaloms_adms$start_time)
finaloms_adms$end_time<-as.character(finaloms_adms$end_time)

bind1<-rbind(omsc,dic,finaloms_adms)
#Here the start time has NA values :
table(bind1$tripping)
bind1$tripping[bind1$tripping=='ESD against Interruption']<-'TI'
bind1$tripping[bind1$tripping=='HV B/D']<-'BD'
bind1$tripping[bind1$tripping=='HV B/D SHUTDOWN']<-'BD'
bind1$tripping[bind1$tripping=='HV ESD']<-'ESD'
bind1$tripping[bind1$tripping=='HV ESD INT']<-'ESD'
bind1$tripping[bind1$tripping=='HV L/D']<-'LD'
bind1$tripping[bind1$tripping=='HV BACKFEEDING']<-'BD'
bind1$tripping[bind1$tripping=='HV DLR']<-'BD'
bind1$tripping[bind1$tripping=='HV L/R']<-'LR'
bind1$tripping[bind1$tripping=='HV PSD']<-'PSD'
bind1$tripping[bind1$tripping=='HV UPSD']<-'PSD'
bind1$tripping[bind1$tripping=='HV T/I']<-'TI'

### #NA or NA will be coming in column of grid and feeder but they have been imputed through master data and miss_oms data
fb<-subset(bind3,year=="2018")
table(fb$month)
bind2<-subset(bind1,tripping=='BD'|tripping=='LD'|tripping=='LR')
bind3<-bind2[!duplicated(bind2$reference_label), ]
###TRAINNG TYPE OF FAULT

bind3$fault_type<-as.character(bind3$fault_type)
bind3$fault_type=sapply(bind3$fault_type,toupper)
bind3$fault_type=gsub("(?!-)[[:punct:]]","",bind3$fault_type,perl=TRUE)
bind3$fault_type=gsub("$","",bind3$fault_type,fixed=TRUE)
bind3$fault_type=gsub("=","",bind3$fault_type,fixed=TRUE)
bind3$fault_type=gsub("+","",bind3$fault_type,fixed=TRUE)
bind3$fault_type=gsub("~","",bind3$fault_type,fixed=TRUE)
bind3$fault_type=gsub("^","",bind3$fault_type,fixed=TRUE)
bind3$fault_type=gsub("\\s+"," ",bind3$fault_type)
bind3$fault_type=gsub("  ","",bind3$fault_type,fixed=TRUE)
bind3$fault_type=gsub(" ","",bind3$fault_type,fixed=TRUE)
bind3$fault_type<-as.factor(bind3$fault_type)

levels(bind3$fault_type)[levels(bind3$fault_type)=="Transient Fault"|
                           levels(bind3$fault_type)=="TRANSIENTFAULT"|levels(bind3$fault_type)=="TRANSIENTTRIP"|levels(bind3$fault_type)=="TRANSIENTTRIPBIRDAGE"
                         |levels(bind3$fault_type)=="TRANSIENTTRIPPNG"|levels(bind3$fault_type)=="TXFAULT"]<-"Transien_fault"
levels(bind3$fault_type)[levels(bind3$fault_type)=="TREEFAULT"|
                           levels(bind3$fault_type)=="TRIPPINGDUETOTREE"]<-"Tree_fault"

levels(bind3$fault_type)[levels(bind3$fault_type)=="ANIMALELECTROCUTEDONTRF"|
                           levels(bind3$fault_type)=="CATRATSQUIRRELELECTROCUTION"]<-"Animal_Electroc"

levels(bind3$fault_type)[levels(bind3$fault_type)=="DDFUSE"|levels(bind3$fault_type)=="DDUNITFAULTY"|
                           levels(bind3$fault_type)=="DDFUSEBLOWN"]<-"DDFUSEFAULT"

levels(bind3$fault_type)[levels(bind3$fault_type)=="CONDUCTORSNAPPED"|
                           levels(bind3$fault_type)=="CONDUCTORSNAP"]<-"CONDUCTORSNAPPED"

levels(bind3$fault_type)[levels(bind3$fault_type)=="SERVICEPILLARFUSEBLOWN"|levels(bind3$fault_type)=="NOOUTAGE"|levels(bind3$fault_type)=="CONSUMERINTERNALCKTFAULT"|
                           levels(bind3$fault_type)=="SRPLRCABLEBURNT"|levels(bind3$fault_type)=="FEEDERPILLARFUSEBLOWN"|levels(bind3$fault_type)=="LTACBTRIPPING"|levels(bind3$fault_type)=="FPSPFUSEBLOWN"]<-"Fuse_blwn_out_mcb_trip"

levels(bind3$fault_type)[levels(bind3$fault_type)=="SLBROKEN"|levels(bind3$fault_type)=="TERMINALBURNTFROMBUSBAR"|levels(bind3$fault_type)=="TERMINALLOOSEFROMBUSBAR"|
                           levels(bind3$fault_type)=="WIRETOUCHINGROOF"|levels(bind3$fault_type)=="SLINEBURNT"|levels(bind3$fault_type)=="SLREPAIRED"|levels(bind3$fault_type)=="NEUTRALWIREBROKEN"]<-"SL_Broken"


levels(bind3$fault_type)[levels(bind3$fault_type)=="LTCABLEBURNT"|levels(bind3$fault_type)=="LVJUMPERBURNT"|levels(bind3$fault_type)=="LTACBBURNT"|levels(bind3$fault_type)=="LTABCBURNT"|levels(bind3$fault_type)=="LTACBMCCBBURNT"|
                           levels(bind3$fault_type)=="JUMPERLOOSEFROMPOLE"| levels(bind3$fault_type)=="FDRPLRSOCKETBURNT"| levels(bind3$fault_type)=="TYCOBOXCHANGEDBURNT"|levels(bind3$fault_type)=="LTMAINCABLEBURNT"|
                           levels(bind3$fault_type)=="SPFPCABLEBURNT" | levels(bind3$fault_type)=="LTLEADBURNTALSOCKETBURNT" |levels(bind3$fault_type)=="SOCKETBURNT"|levels(bind3$fault_type)=="FPSPSOCKETBURNT"]<-"Fault_Dist_Mains"

levels(bind3$fault_type)[levels(bind3$fault_type)=="DISTTRFFAILURE"|
                           levels(bind3$fault_type)=="TRANSFORMERFAILURE"|levels(bind3$fault_type)=="POWERTRANSFORMERFAILURE"]<-"Transformer_fault"

levels(bind3$fault_type)[levels(bind3$fault_type)=="CABLEFAULT"|
                           levels(bind3$fault_type)=="HTCABLEFAULTY "|levels(bind3$fault_type)=="LTCABLEBURNT"|levels(bind3$fault_type)=="LTABCBURNT"
                         |levels(bind3$fault_type)=="HTCABLETERMINATINFAULTY"]<-"Cable_Burnt_Fault"


levels(bind3$fault_type)[levels(bind3$fault_type)=="OHFAULT"|levels(bind3$fault_type)=="OHJUMPERBURNT"]<-"Overhead_fault"
levels(bind3$fault_type)[levels(bind3$fault_type)=="TRANSIENTTRIPBIRDAGE"|levels(bind3$fault_type)=="BIRDAGE"|levels(bind3$fault_type)=="BLASTONPOLEBIRDAGE"]<-"BIRDAGE"

levels(bind3$fault_type)[levels(bind3$fault_type)=="CTPTLACVTFAILURE"|levels(bind3$fault_type)=="CTPTLACVTCIRCUITBREAKER"|levels(bind3$fault_type)=="GRIDSWITCHGEARFAILURE"]<-"HT_Mains_Failed_3366"
levels(bind3$fault_type)[levels(bind3$fault_type)=="METERFAULTY"| levels(bind3$fault_type)=="CUSTOMERMTERBURNTCTMTR"|levels(bind3$fault_type)=="CUSTOMER3PHSMETERBURNT"| levels(bind3$fault_type)=="METERINGCUBICLEFAULT"]<-"Burnt_Meter"

levels(bind3$fault_type)[levels(bind3$fault_type)=="SHUTDOWN"|levels(bind3$fault_type)=="PLANNEDSHUTDOWN"|levels(bind3$fault_type)=="ESD"]<-"Shutdown"
levels(bind3$fault_type)[levels(bind3$fault_type)=="LOADSHEDDING"]<-"Load_shedding"
levels(bind3$fault_type)[levels(bind3$fault_type)=="SUPPLYFAIL"|levels(bind3$fault_type)=="TOWERPOLESTRUCTUREDAMAGED"]<-"Supply_fail"

levels(bind3$fault_type)[levels(bind3$fault_type)=="FEEDERTRIPSHUTDOWN"|levels(bind3$fault_type)=="FEEDERTRIP"]<-"FeederTrip"
levels(bind3$fault_type)[levels(bind3$fault_type)=="OTHER"|levels(bind3$fault_type)=="OTHERFAULT"|
                           levels(bind3$fault_type)=="FAULTINOTHERSECTION"|levels(bind3$fault_type)=="UNKNOWN"|levels(bind3$fault_type)=="FIREINFORATION"]<-"OTHERFAULT"


names(bind3)
fb<-subset(bind3,year=="2018")
table(fb$month)

#bind3 of final data of interruption.

############SCADA ADMS

#SCADA LOAD DATA CLEANING AND FORMING VARIABLES OF MAX AND AVG LOAD


names(loadadms)
loadadms<-loadadms[c("avg_date","load_mnth","voltage","grid", "feeder","element", "avg_load","max_load")]
names(loadadms)[names(loadadms)=="avg_date"]<-"calday"
names(loadadms)[names(loadadms)=="load_mnth"]<-"calmonth"
names(loadadms)[names(loadadms)=="voltage"]<-"bic_zb2text"
names(loadadms)[names(loadadms)=="grid"]<-"bic_zb1text"
names(loadadms)[names(loadadms)=="feeder"]<-"bic_zb3text"
names(loadadms)[names(loadadms)=="element"]<-"bic_zelmnttxt"
names(loadadms)[names(loadadms)=="avg_load"]<-"bic_zwert1"
names(loadadms)[names(loadadms)=="max_load"]<-"bic_zmaxwrt"
table(loadadms$bic_zelmnttxt)
#load<-rbind(loadd,loadadms)
table(load$bic_zelmnttxt)
load<-subset(load,bic_zelmnttxt=="R-Phase Current")
loadadms<-subset(loadadms,bic_zelmnttxt=="Rph Current")

######################SCADA LOAD############################################################

load$concatenate1<-paste(load$bic_zb1text,load$bic_zb3text)
load$concatenate1=sapply(load$concatenate1,toupper)
load$concatenate1=gsub("GRID","",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("(?!-)[[:punct:]]","",load$concatenate1,perl=TRUE)
load$concatenate1=gsub("$","",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("=","",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("+","",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("~","",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("^","",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("\\s+"," ",load$concatenate1)
load$concatenate1=gsub("  ","",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub(" ","",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("1","ONE",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("2","TWO",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("3","THREE",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("4","FOUR",load$concatenate1,fixed=TRUE)
load$concatenate1=gsub("-III{3}","-THREE",load$concatenate1)
load$concatenate1=gsub("III{3}$","-THREE",load$concatenate1)
load$concatenate1=gsub("-II{2}","-TWO",load$concatenate1)
load$concatenate1=gsub("-I{1}","-ONE",load$concatenate1)
load$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",load$concatenate1)
load$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",load$concatenate1)

load$feeder=sapply(load$bic_zb3text,toupper)
load$feeder=gsub("GRID","",load$feeder,fixed=TRUE)
load$feeder=gsub("(?!-)[[:punct:]]","",load$feeder,perl=TRUE)
load$feeder=gsub("$","",load$feeder,fixed=TRUE)
load$feeder=gsub("=","",load$feeder,fixed=TRUE)
load$feeder=gsub("+","",load$feeder,fixed=TRUE)
load$feeder=gsub("~","",load$feeder,fixed=TRUE)
load$feeder=gsub("^","",load$feeder,fixed=TRUE)
load$feeder=gsub("\\s+"," ",load$feeder)
load$feeder=gsub("  ","",load$feeder,fixed=TRUE)
load$feeder=gsub(" ","",load$feeder,fixed=TRUE)
load$feeder=gsub("1","ONE",load$feeder,fixed=TRUE)
load$feeder=gsub("2","TWO",load$feeder,fixed=TRUE)
load$feeder=gsub("3","THREE",load$feeder,fixed=TRUE)
load$feeder=gsub("4","FOUR",load$feeder,fixed=TRUE)

load$feeder=gsub("-III{3}","-THREE",load$feeder)
load$feeder=gsub("III{3}$","-THREE",load$feeder)
load$feeder=gsub("-II{2}","-TWO",load$feeder)
load$feeder=gsub("-I{1}","-ONE",load$feeder)
load$feeder<-gsub("FEEDER-1|FEEDER1","FDR-ONE",load$feeder)
load$feeder<-gsub("FEEDER-2|FEEDER2","FDR-TWO",load$feeder)

load$feeder[load$feeder==""]<-"MISS"
load$feeder[is.na(load$feeder)]<-"MISS"

#delting values which are not feeder in scada load data
load1<-load[!grepl("MVA",load$concatenate1)&!grepl("BUSSECTION",load$concatenate1)&!grepl("INCOMER",load$concatenate1)&
              !grepl("^OG",load$feeder)&!grepl("^SHUNTCAP",load$feeder)&!grepl("STNTRAFO",load$feeder)&!grepl("STNTRAFO",load$feeder)&
              !grepl("^TR0",load$feeder)&!grepl("^PTR",load$feeder)&!grepl("^STN",load$feeder)&!grepl("BC1A",load$feeder)&
              !grepl("^NALOAD",load$feeder)&!grepl("^B220P",load$feeder)&!grepl("^IC",load$feeder)&!grepl("^NALOAD",load$feeder)&
              !grepl("^BHLP",load$feeder)&!grepl("^DUP02F",load$feeder)&!grepl("^33KV",load$feeder)&!grepl("BATTERY",load$feeder)&
              !grepl("^TRNO",load$feeder)&!grepl("^TR[0-9]",load$feeder)&!grepl("^NA7P",load$feeder)&!grepl("CKT",load$feeder)&
              !grepl("TRF",load$feeder)&!grepl("^IC",load$feeder)&!grepl("CAPBANK",load$feeder)&!grepl("BUSCOUPL",load$feeder)&
              !grepl("^SPARE",load$feeder)&!grepl("^BHLP",load$feeder)&!grepl("^66KV",load$feeder)& !grepl("^11KVIC",load$feeder)
            &!grepl("RTU560",load$feeder)&!grepl("INTERCONNEC",load$feeder)&!grepl("BWBUS",load$feeder)&!grepl("SNTCP",load$feeder)
            &!grepl("LOCALTR",load$feeder)&!grepl("SEC3UG",load$feeder)&!grepl("VOLTAMP",load$feeder)&!grepl("SNTCP",load$feeder)
            &!grepl("OG0[0-9]",load$feeder)&!grepl("OG[0-9]",load$feeder)&!grepl("OUTSIDER",load$feeder)&!grepl("RMUNEAR",load$feeder)
            &!grepl("BACKUPCAPBANK",load$feeder)&!grepl("BUSBAR",load$feeder)&!grepl("POWERTRAF",load$feeder)
            &!grepl("DDA21B",load$feeder)&!grepl("FFSH",load$feeder)&!grepl("1220BWN",load$feeder)&!grepl("BWN1P12B",load$feeder)
            &!grepl("BWN1P23L",load$feeder)&!grepl("BAWANA1FGBLKSEC3",load$feeder)&!grepl("KNJ",load$feeder)&!grepl("BAWANA1L258SEC3",load$feeder)
            &!grepl("GULP",load$feeder),]

# dfff<-ccf[grepl("^[0-9KVIC]",load$feeder),]
# head(dfff)
load_<-data.frame(unique(load1$concatenate1))
names(load_)<-"concatenate1"
load_$Approx_match<-c()
load_$Approx_match1<-c()
load_$Approx_match2<-c()
load_$Approx_match3<-c()

for( i in 1: NROW(load_))
{
  load_$Approx_match[i]<-master$concatenate1[which.min(stringdist(load_$concatenate1[i],
                                                                  master$concatenate1,method='jaccard',q=2,nthread=100))]
  load_$Approx_match1[i]<-master$concatenate1[which.min(stringdist(load_$concatenate1[i],master$concatenate1,method='dl',nthread=100))]
  load_$Approx_match2[i]<-master$concatenate1[which.min(stringdist(load_$concatenate1[i],master$concatenate1,method='jw',nthread=100))]
  load_$Approx_match3[i]<-master$concatenate1[which.min(stringdist(load_$concatenate1[i],master$concatenate1,method='hamming',nthread=100))]
  
}

final_loadd<-merge(load1,load_,by="concatenate1",all.x=TRUE)

####################################ADMS LOAD################################################

loadadms$concatenate1<-paste(loadadms$bic_zb1text,loadadms$bic_zb3text)
loadadms$concatenate1=sapply(loadadms$concatenate1,toupper)
loadadms$concatenate1=gsub("GRID","",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("(?!-)[[:punct:]]","",loadadms$concatenate1,perl=TRUE)
loadadms$concatenate1=gsub("$","",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("=","",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("+","",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("~","",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("^","",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("\\s+"," ",loadadms$concatenate1)
loadadms$concatenate1=gsub("  ","",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub(" ","",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("1","ONE",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("2","TWO",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("3","THREE",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("4","FOUR",loadadms$concatenate1,fixed=TRUE)
loadadms$concatenate1=gsub("-III{3}","-THREE",loadadms$concatenate1)
loadadms$concatenate1=gsub("III{3}$","-THREE",loadadms$concatenate1)
loadadms$concatenate1=gsub("-II{2}","-TWO",loadadms$concatenate1)
loadadms$concatenate1=gsub("-I{1}","-ONE",loadadms$concatenate1)
loadadms$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",loadadms$concatenate1)
loadadms$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",loadadms$concatenate1)

loadadms$feeder=sapply(loadadms$bic_zb3text,toupper)
loadadms$feeder=gsub("GRID","",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("(?!-)[[:punct:]]","",loadadms$feeder,perl=TRUE)
loadadms$feeder=gsub("$","",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("=","",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("+","",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("~","",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("^","",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("\\s+"," ",loadadms$feeder)
loadadms$feeder=gsub("  ","",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub(" ","",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("1","ONE",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("2","TWO",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("3","THREE",loadadms$feeder,fixed=TRUE)
loadadms$feeder=gsub("4","FOUR",loadadms$feeder,fixed=TRUE)

loadadms$feeder=gsub("-III{3}","-THREE",loadadms$feeder)
loadadms$feeder=gsub("III{3}$","-THREE",loadadms$feeder)
loadadms$feeder=gsub("-II{2}","-TWO",loadadms$feeder)
loadadms$feeder=gsub("-I{1}","-ONE",loadadms$feeder)
loadadms$feeder<-gsub("FEEDER-1|FEEDER1","FDR-ONE",loadadms$feeder)
loadadms$feeder<-gsub("FEEDER-2|FEEDER2","FDR-TWO",loadadms$feeder)


loadadms$feeder[loadadms$feeder==""]<-"MISS"
loadadms$feeder[is.na(loadadms$feeder)]<-"MISS"

#delting values which are not feeder in scada loadadms data
loadadms1<-loadadms[!grepl("MVA",loadadms$concatenate1)&!grepl("BUSSECTION",loadadms$concatenate1)&!grepl("INCOMER",loadadms$concatenate1)&
                      !grepl("^OG",loadadms$feeder)&!grepl("^SHUNTCAP",loadadms$feeder)&!grepl("STNTRAFO",loadadms$feeder)&!grepl("STNTRAFO",loadadms$feeder)&
                      !grepl("^TR0",loadadms$feeder)&!grepl("^PTR",loadadms$feeder)&!grepl("^STN",loadadms$feeder)&!grepl("BC1A",loadadms$feeder)&
                      !grepl("^NAloadadms",loadadms$feeder)&!grepl("^B220P",loadadms$feeder)&!grepl("^IC",loadadms$feeder)&!grepl("^NAloadadms",loadadms$feeder)&
                      !grepl("^BHLP",loadadms$feeder)&!grepl("^DUP02F",loadadms$feeder)&!grepl("^33KV",loadadms$feeder)&!grepl("BATTERY",loadadms$feeder)&
                      !grepl("^TRNO",loadadms$feeder)&!grepl("^TR[0-9]",loadadms$feeder)&!grepl("^NA7P",loadadms$feeder)&!grepl("CKT",loadadms$feeder)&
                      !grepl("TRF",loadadms$feeder)&!grepl("^IC",loadadms$feeder)&!grepl("CAPBANK",loadadms$feeder)&!grepl("BUSCOUPL",loadadms$feeder)&
                      !grepl("^SPARE",loadadms$feeder)&!grepl("^BHLP",loadadms$feeder)&!grepl("^66KV",loadadms$feeder)& !grepl("^11KVIC",loadadms$feeder)
                    &!grepl("RTU560",loadadms$feeder)&!grepl("INTERCONNEC",loadadms$feeder)&!grepl("BWBUS",loadadms$feeder)&!grepl("SNTCP",loadadms$feeder)
                    &!grepl("LOCALTR",loadadms$feeder)&!grepl("SEC3UG",loadadms$feeder)&!grepl("VOLTAMP",loadadms$feeder)&!grepl("SNTCP",loadadms$feeder)
                    &!grepl("OG0[0-9]",loadadms$feeder)&!grepl("OG[0-9]",loadadms$feeder)&!grepl("OUTSIDER",loadadms$feeder)&!grepl("RMUNEAR",loadadms$feeder)
                    &!grepl("BACKUPCAPBANK",loadadms$feeder)&!grepl("BUSBAR",loadadms$feeder)&!grepl("POWERTRAF",loadadms$feeder)
                    &!grepl("DDA21B",loadadms$feeder)&!grepl("FFSH",loadadms$feeder)&!grepl("1220BWN",loadadms$feeder)&!grepl("BWN1P12B",loadadms$feeder)
                    &!grepl("BWN1P23L",loadadms$feeder)&!grepl("BAWANA1FGBLKSEC3",loadadms$feeder)&!grepl("KNJ",loadadms$feeder)&!grepl("BAWANA1L258SEC3",loadadms$feeder)
                    &!grepl("GULP",loadadms$feeder),]

loadadms_<-data.frame(unique(loadadms1$concatenate1))
names(loadadms_)<-"concatenate1"
loadadms_$Approx_match<-c()
loadadms_$Approx_match1<-c()
loadadms_$Approx_match2<-c()
loadadms_$Approx_match3<-c()

#####concatenate_adms column should be cleaned

for( i in 1: NROW(loadadms_))
{
  loadadms_$Approx_match[i]<-master$concatenate1[which.min(stringdist(loadadms_$concatenate1[i],
                                                                      master$concatenate_adms,method='jaccard',q=2,nthread=100))]
  loadadms_$Approx_match1[i]<-master$concatenate1[which.min(stringdist(loadadms_$concatenate1[i],master$concatenate_adms,method='dl',nthread=100))]
  loadadms_$Approx_match2[i]<-master$concatenate1[which.min(stringdist(loadadms_$concatenate1[i],master$concatenate_adms,method='jw',nthread=100))]
  loadadms_$Approx_match3[i]<-master$concatenate1[which.min(stringdist(loadadms_$concatenate1[i],master$concatenate_adms,method='hamming',nthread=100))]
  
}

final_loadadms<-merge(loadadms1,loadadms_,by="concatenate1",all.x=TRUE)


########binding scada old and adms data
final_load<-rbind(final_loadd,final_loadadms)
final_load$year<-format(final_load$calday,"%Y")
final_load$month<-format(final_load$calday,"%m")
final_load$bic_zwert1<-as.numeric(final_load$bic_zwert1)
final_load$bic_zmaxwrt<-as.numeric(final_load$bic_zmaxwrt)

final_load2<-aggregate(final_load[c("bic_zwert1","bic_zmaxwrt")],by=list(final_load$Approx_match,final_load$year,final_load$month),FUN="mean")
names(final_load2)[1]<-"Approx_match"
names(final_load2)[2]<-"year"
names(final_load2)[3]<-"month"
names(final_load2)[4]<-"avg_wert"
names(final_load2)[5]<-"max_avg"
names(final_load2)
final_load2$time<-paste(final_load2$month,final_load2$year)

#######################GIS
names(gis)
gisw<-gis[c("id","name","mounting" , "length","specification","lon2","lat2")]
names(gisw)[2]<-"concatenate1"
gisw$concatenate1[gisw$concatenate1=="" |gisw$concatenate1=="28.75"|gisw$concatenate1=="28.74"|gisw$concatenate1=="28.76"|is.na(gisw$concatenate1)]<-"MISS"

ind <- which(with(gisw,concatenate1=="MISS"))
gisw <- gisw[ -ind, ]

#GIS FEEDER COLUMN IN MASTER DATA
master$gis_fdr_name<-paste(master$gis_fdr_name)
master$gis_fdr_name=sapply(master$gis_fdr_name,toupper)
master$gis_fdr_name=gsub("GRID","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("OUTDOOR","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("(?!-)[[:punct:]]","",master$gis_fdr_name,perl=TRUE)
master$gis_fdr_name=gsub("$","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("=","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("+","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("~","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("^","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("\\s+"," ",master$gis_fdr_name)
master$gis_fdr_name=gsub("  ","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub(" ","",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("1","ONE",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("2","TWO",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("3","THREE",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("4","four",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("-III{1}","-THREE",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("III{1}$","-THREE",master$gis_fdr_name)
master$gis_fdr_name=gsub("-II{1}","-TWO",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name=gsub("-I{1}","-ONE",master$gis_fdr_name,fixed=TRUE)
master$gis_fdr_name<-gsub("FEEDER-1|FEEDER1","FDR-ONE",master$gis_fdr_name)
master$gis_fdr_name<-gsub("FEEDER-2|FEEDER2","FDR-TWO",master$gis_fdr_name)
master$gis_fdr_name[master$gis_fdr_name==""]<-"MISS"
master$gis_fdr_name[is.na(master$gis_fdr_name)]<-"MISS"

#grid and feeder concatenate.
gisw$concatenate1=sapply(gisw$concatenate1,toupper)
gisw$concatenate1=gsub("GRID","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("OUTDOOR","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("(?!-)[[:punct:]]","",gisw$concatenate1,perl=TRUE)
gisw$concatenate1=gsub("$","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("=","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("+","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("~","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("^","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("\\s+"," ",gisw$concatenate1)
gisw$concatenate1=gsub("  ","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub(" ","",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("1","ONE",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("2","TWO",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("3","THREE",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("4","FOUR",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("-III{1}","-THREE",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("III{1}$","-THREE",gisw$concatenate1)
gisw$concatenate1=gsub("-II{1}","-TWO",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1=gsub("-I{1}","-ONE",gisw$concatenate1,fixed=TRUE)
gisw$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",gisw$concatenate1)
gisw$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",gisw$concatenate1)
gisw$concatenate1[gisw$concatenate1==""]<-"MISS"
gisw$concatenate1[is.na(gisw$concatenate1)]<-"MISS"
gis2<-data.frame(unique(gisw$concatenate1))
names(gis2)<-"concatenate1"
gis2$Approx_match<-c()
gis2$Approx_match1<-c()
gis2$Approx_match2<-c()
gis2$Approx_match3<-c()

for( i in 1: NROW(gis2))
{
  gis2$Approx_match[i]<-master$concatenate1[which.min(stringdist(gis2$concatenate1[i],master$gis_fdr_name,method='jaccard',q=2,nthread=100))]
  gis2$Approx_match1[i]<-master$concatenate1[which.min(stringdist(gis2$concatenate1[i],master$gis_fdr_name,method='jaccard',nthread=100))]
  gis2$Approx_match2[i]<-master$concatenate1[which.min(stringdist(gis2$concatenate1[i],master$gis_fdr_name,method='jw',nthread=100))]
  gis2$Approx_match3[i]<-master$concatenate1[which.min(stringdist(gis2$concatenate1[i],master$gis_fdr_name,method='hamming',nthread=100))]
  
}

#merging with original data of omss with gis1
finalgis<-merge(gisw,gis2,by="concatenate1",all.x=TRUE)
finalgis<-subset(finalgis,select=-c( id ,Approx_match1,Approx_match2,Approx_match3))
finalgis$specification[grep("ABC",finalgis$specification)]<-"ABC"
finalgis$specification[grep("XLPE",finalgis$specification)]<-"XLPE"
finalgis$specification[grep("PILCA",finalgis$specification)]<-"PILCA"
finalgis$specification[grep("ACSR",finalgis$specification)]<-"ACSR"
finalgis$mounting[finalgis$mounting==""|is.na(finalgis$mounting)]<-"MISS"
ind1<- which(with(finalgis,mounting=="MISS"|specification=="NO SPEC"))
finalgis <- finalgis[ -ind1, ]
finalgis$length<-gsub("mm{1}$",":mili",finalgis$length)
grep("m$",finalgis$length,value=TRUE)
finalgis$length<-gsub("m{1}$",":metre",finalgis$length)
finalgis1<-setDT(finalgis)[,c("len","dim"):=tstrsplit(length,":")]
finalgis$len<-as.numeric(finalgis$len)
finalgis$len<-ifelse(finalgis$dim=="mili",finalgis$len/1000,finalgis$len)
a<-dummy(finalgis$mounting)
b<-dummy(finalgis$specification)
finalgis<-cbind(finalgis,a,b)
finalgis$num_sections<-1
finalgis<-data.frame(finalgis)
finalgis[c("mountingOverhead","mountingUnderground","specificationABC","specificationACSR","specificationPILCA","specificationXLPE")]<-finalgis[["len"]]*finalgis[c("mountingOverhead","mountingUnderground","specificationABC","specificationACSR","specificationPILCA","specificationXLPE")]
gis_prop<-aggregate(finalgis[c("mountingOverhead","mountingUnderground","specificationABC","specificationACSR","specificationPILCA","specificationXLPE", "num_sections")],by=list(finalgis$Approx_match),sum)
names(gis_prop)[1]<-"Approx_match"
finalgis1<-finalgis[c("concatenate1","Approx_match","lon2","lat2")]
finalgis1<-unique(finalgis1)

bind4<-bind3
fb<-subset(bind4,year=="2018")
table(fb$month)
g<-dummy(bind4$tripping)
bind44<-cbind(bind4,g)
fb<-subset(bind44,year=="2018")
table(fb$month)
h<-dummy(bind44$fault_type)
bind444<-cbind(bind44,h)
str(bind444)
fb<-subset(bind444,year=="2018")
table(fb$month)

bind444$trippingBD<-as.numeric(bind444$trippingBD)
bind444$trippingLD<-as.numeric(bind444$trippingLD)
bind444$trippingLR<-as.numeric(bind444$trippingLR)
bind444$trippingLR<-as.numeric(bind444$trippingLR)
bind444$trippingLR<-as.numeric(bind444$trippingLR)
bind444$fault_typeTransien_fault<-as.numeric(bind444$fault_typeTransien_fault)
bind444$fault_typeTree_fault<-as.numeric(bind444$fault_typeTree_fault)
bind444$fault_typeAnimal_Electroc<-as.numeric(bind444$fault_typeAnimal_Electroc)
bind444$fault_typeCONDUCTORSNAPPED<-as.numeric(bind444$fault_typeCONDUCTORSNAPPED)
bind444$fault_typeFuse_blwn_out_mcb_trip<-as.numeric(bind444$fault_typeFuse_blwn_out_mcb_trip)
bind444$fault_typeOverhead_fault<-as.numeric(bind444$fault_typeOverhead_fault)
bind444$fault_typeSL_Broken<-as.numeric(bind444$fault_typeSL_Broken)
bind444$fault_typeFault_Dist_Mains<-as.numeric(bind444$fault_typeFault_Dist_Mains)
bind444$fault_typeTransformer_fault<-as.numeric(bind444$fault_typeTransformer_fault)
bind444$fault_typeCable_Burnt_Fault<-as.numeric(bind444$fault_typeCable_Burnt_Fault)
bind444$fault_typeBIRDAGE<-as.numeric(bind444$fault_typeBIRDAGE)
bind444$fault_typeHT_Mains_Failed_3366<-as.numeric(bind444$fault_typeHT_Mains_Failed_3366)
bind444$fault_typeBurnt_Meter<-as.numeric(bind444$fault_typeBurnt_Meter)
bind444$fault_typeShutdown<-as.numeric(bind444$fault_typeShutdown)
bind444$fault_typeLoad_shedding<-as.numeric(bind444$fault_typeLoad_shedding)
bind444$fault_typeSupply_fail<-as.numeric(bind444$fault_typeSupply_fail)
bind444$fault_typeFeederTrip<-as.numeric(bind444$fault_typeFeederTrip)
bind444$fault_typeOTHERFAULT<-as.numeric(bind444$fault_typeOTHERFAULT)

bind5<-aggregate(bind444[,c("trippingBD","trippingLD","trippingLR","fault_typeTransien_fault" ,"fault_typeTree_fault" , "fault_typeAnimal_Electroc","fault_typeCONDUCTORSNAPPED",
                            "fault_typeFuse_blwn_out_mcb_trip", "fault_typeOverhead_fault","fault_typeSL_Broken",
                            "fault_typeFault_Dist_Mains","fault_typeTransformer_fault","fault_typeCable_Burnt_Fault",
                            "fault_typeBIRDAGE","fault_typeHT_Mains_Failed_3366","fault_typeBurnt_Meter",
                            "fault_typeShutdown","fault_typeLoad_shedding","fault_typeSupply_fail",
                            "fault_typeFeederTrip","fault_typeOTHERFAULT"
)],by=list(bind444$Approx_match,bind444$month,bind444$year),FUN="sum")


names(bind5)[1]<-"Approx_match"
names(bind5)[2]<-"month"
names(bind5)[3]<-"year"
fb<-subset(bind5,year=="2018")
table(fb$month)


bind5<-subset(bind5,year>=as.integer(format(Sys.Date(),"%Y"))-3)
fb<-subset(bind5,year=="2018")
table(fb$month)

bind5$month<-as.factor(bind5$month)
levels(bind5$month)[levels(bind5$month)=="Apr"]<-"4"
levels(bind5$month)[levels(bind5$month)=="Aug"]<-"8"
levels(bind5$month)[levels(bind5$month)=="Dec"]<-"12"
levels(bind5$month)[levels(bind5$month)=="Feb"]<-"2"
levels(bind5$month)[levels(bind5$month)=="Jan"]<-"1"
levels(bind5$month)[levels(bind5$month)=="Jul"]<-"7"
levels(bind5$month)[levels(bind5$month)=="Jun"]<-"6"
levels(bind5$month)[levels(bind5$month)=="Mar"]<-"3"
levels(bind5$month)[levels(bind5$month)=="May"]<-"5"
levels(bind5$month)[levels(bind5$month)=="Nov"]<-"11"
levels(bind5$month)[levels(bind5$month)=="Oct"]<-"10"
levels(bind5$month)[levels(bind5$month)=="Sep"]<-"9"
fb<-subset(bind5,year=="2018")
table(fb$month)
str(bind5$year)
bind5$month<-as.numeric(levels(bind5$month))[bind5$month]
bind5$year<-as.numeric(bind5$year)
bind5<-bind5[order(bind5$Approx_match,bind5$year,bind5$month),]
names(bind5)
fb<-subset(bind5,year=="2018")
table(fb$month)

bind5<-data.table(bind5)
names(bind5)
bind6<-bind5[, shift(.SD, c(1,12,24), NA, "lag",TRUE),.SDcols=c("trippingBD","trippingLD","trippingLR"),by=c("Approx_match")]

bind66<-bind5[, shift(.SD, c(1:6), NA, "lag",TRUE),.SDcols=c("fault_typeTransien_fault" ,"fault_typeTree_fault",
                                                             "fault_typeAnimal_Electroc","fault_typeCONDUCTORSNAPPED",
                                                             "fault_typeFuse_blwn_out_mcb_trip","fault_typeSL_Broken",
                                                             "fault_typeFault_Dist_Mains","fault_typeTransformer_fault",
                                                             "fault_typeCable_Burnt_Fault","fault_typeBIRDAGE","fault_typeOverhead_fault",
                                                             "fault_typeHT_Mains_Failed_3366","fault_typeBurnt_Meter",
                                                             "fault_typeShutdown","fault_typeLoad_shedding",
                                                             "fault_typeSupply_fail","fault_typeFeederTrip",
                                                             "fault_typeOTHERFAULT"
),by=c("Approx_match")]

bind7<-cbind(bind5,bind6,bind66)
names(bind7)
bind7<-bind7[,-c(25,35)]
names(bind7)
bind7<-bind7[order(bind7$Approx_match,bind7$year,bind7$month),]
str(bind7)
##making standarad data

##making 3 years of standard data 2 years for prediction and rest for testing
#number of feeders in master data
nosfeeders<-NROW(master$concatenate1)

#a year should come 12 times for each feeder in a year 1112*12 for each year
l<-as.integer(format(Sys.Date(),"%Y"))-2
u<-as.integer(format(Sys.Date(),"%Y"))

year<-rep(l:u,each=(nosfeeders*12))
#year<-rep(2016:2018,each=13344)

# a month should come 2 times for each feeder in a year 3*1112 
month<-rep(1:12,(3*nosfeeders))

##feeder list 12 times each feeder
Approx_match<-rep(unique(master$concatenate1),each=12)

stand<-data.frame(cbind(year,month,Approx_match))
stand$date_<-paste(stand$year,stand$month,sep="-")

bind7$month<-as.factor(bind7$month)
levels(bind7$month)[levels(bind7$month)=="01"]<-"1"
levels(bind7$month)[levels(bind7$month)=="02"]<-"2"
levels(bind7$month)[levels(bind7$month)=="03"]<-"3"
levels(bind7$month)[levels(bind7$month)=="04"]<-"4"
levels(bind7$month)[levels(bind7$month)=="05"]<-"5"
levels(bind7$month)[levels(bind7$month)=="06"]<-"6"
levels(bind7$month)[levels(bind7$month)=="07"]<-"7"
levels(bind7$month)[levels(bind7$month)=="08"]<-"8"
levels(bind7$month)[levels(bind7$month)=="09"]<-"9"

bind7$date_<-paste(bind7$year,bind7$month,sep="-")
bind7<-data.frame(bind7)

#binding with standard data of three years

bind77<-merge(bind7,stand,by=c("Approx_match","date_"),all=TRUE)
summary(bind7$trippingBD)

####load variables preparation
names(final_load2)
summary(final_load2$avg_wert)
summary(final_load2$max_avg)
final_load2$max_avg<-ifelse(final_load2$max_avg<=0,quantile(final_load2$max_avg, 0.25 ,na.rm=TRUE ),final_load2$max_avg)
final_load2$avg_wert<-ifelse(final_load2$avg_wert<=0,quantile(final_load2$avg_wert, 0.25 ,na.rm=TRUE ),final_load2$avg_wert)
final_load2<-final_load2[order(final_load2$Approx_match,final_load2$year,final_load2$month),]

final_load2<-data.table(final_load2)

bind8<-final_load2[, shift(.SD, c(1:6), NA, "lag",TRUE),.SDcols=c("avg_wert","max_avg"),by=c("Approx_match")]


bind9<-cbind(final_load2,bind8)
bind9$month<-as.factor(bind9$month)
levels(bind9$month)[levels(bind9$month)=="01"]<-"1"
levels(bind9$month)[levels(bind9$month)=="02"]<-"2"
levels(bind9$month)[levels(bind9$month)=="03"]<-"3"
levels(bind9$month)[levels(bind9$month)=="04"]<-"4"
levels(bind9$month)[levels(bind9$month)=="05"]<-"5"
levels(bind9$month)[levels(bind9$month)=="06"]<-"6"
levels(bind9$month)[levels(bind9$month)=="07"]<-"7"
levels(bind9$month)[levels(bind9$month)=="08"]<-"8"
levels(bind9$month)[levels(bind9$month)=="09"]<-"9"

bind9$date_<-paste(bind9$year,bind9$month,sep="-")
table(bind9$date_)

###combining load,tripping and fault variables
bind77<-data.frame(bind77)
bind77<-setDT(bind77)[,c("year","month"):=tstrsplit(date_,"-")]

bind9<-data.frame(bind9)
names(bind9)<-gsub(".","_",names(bind9),fixed=TRUE)
bind9<-setDT(bind9)[,c("year","month"):=tstrsplit(date_,"-")]

bind9<-subset(bind9,select=-c(Approx_match_1))

# bind9<-bind9[,-c("Approx_match.1")]

###merging with load data
names(bind9)
bind77<-subset(bind77,select=-c(month.y,year.y,month.x,year.x))

#c1<-subset(bind77,Approx_match=="A-7NARELAAIRKHAMPUR")
finaldata<-merge(bind77,bind9,by=c("Approx_match","date_"),all=TRUE)

##taking data from april 2016 as load data is available from that year only
table(finaldata$date_)

##making dependent variable
str(finaldata$trippingBD)
str(finaldata$trippingLD)
str(finaldata$trippingLR)
str(finaldata$tripping)

finaldata[,c("trippingBD","trippingLD","trippingLR")][is.na(finaldata[,c("trippingBD","trippingLD","trippingLR")])]<-0

finaldata$tripping<-(finaldata$trippingBD+finaldata$trippingLD+finaldata$trippingLR)
finaldata$tripping1<-ifelse(finaldata$tripping>=1,1,0)
finaldata$tripping1<-ifelse(is.na(finaldata$tripping1),0,finaldata$tripping1)
table(finaldata$tripping1)

#merging with GIS data

finaldata2<-merge(finaldata,gis_prop,by="Approx_match")
finaldata2<-subset(finaldata2,select=-c(month.x,month.y,year.x,year.y))
table(finaldata2$date_)
str(finaldata2$date_)
finaldata2<-filter(finaldata2,!grepl("^2015",date_))
finaldata2<-setDT(finaldata2)[,c("year","month"):=tstrsplit(date_,"-")]

table(finaldata2$year)
table(finaldata2$month)
table(finaldata2$date_)


summary(finaldata2$avg_wert)
summary(finaldata2$max_avg)
summary(finaldata2$trippingBD)
names(finaldata2)
finaldata2[,c(6:140,157)][is.na(finaldata2[,c(6:140,157)])]<-0
table(is.na(finaldata2$tripping1))
table(finaldata2$tripping1)
table(finaldata2$month)

M1_fun1 <- function(x){
  
  x<-ifelse(is.na(x),mean(x,na.rm=TRUE),x)
  
}

M1_funout<- function(x){
  quantiles <- quantile( x, c(.05, .95),na.rm=TRUE )
  x<-ifelse(x<quantiles[1],quantiles[1],x)
  x<-ifelse(x>quantiles[2],quantiles[2],x)
}

str(finaldata2$year)

#train<-subset(finaldata2,year=="2016" | year=="2017")
finaldata2$day<-1
finaldata2$date_<-paste(finaldata2$date_,finaldata2$day,sep="-")
finaldata2$date_<-as.Date(finaldata2$date_,format="%Y-%m-%d")
finaldata2$month<-as.numeric(finaldata2$month)

#finaldata2<-subset(finaldata2,select=-c(month.x,month.y,year.x,year.y))
str(finaldata2$month)
#train<-subset(finaldata2,date_>as.Date(format(Sys.Date(),"%Y-%m-01"))-months(25)& date_<as.Date(format(Sys.Date(),"%Y-%m-01")))
train<-subset(finaldata2,date_==as.Date(format(Sys.Date(),"%Y-%m-01"))-months(1)|date_==as.Date(format(Sys.Date(),"%Y-%m-01"))-
                months(12)|date_==as.Date(format(Sys.Date(),"%Y-%m-01"))-months(24))

table(train$date_)
table(train$tripping1)
test<-subset(finaldata2,date_>=as.Date(format(Sys.Date(),"%Y-%m-01"))& date_<as.Date(format(Sys.Date(),"%Y-%m-01"))+months(1))
table(test$date_)
train$month<-as.factor(train$month)
test$month<-as.factor(test$month)

str(train$month)
str(test$month)

#Data preprocess on training data

num_var=sapply(train,is.numeric)
train<-data.frame(train)
train[num_var]<-sapply(train[num_var],function(x){M1_fun1(x)})
train1<-train##for getting unscaled values
train[num_var]<-sapply(train[num_var],function(x){M1_funout(x)})
train[num_var]<-sapply(train[num_var],function(x){(x-min(x))/(max(x)-min(x))})

#REPLACING NAN VALUES
train[is.na(train)] <- 0
#Data preprocess on testing data
num_var=sapply(test,is.numeric)
test<-data.frame(test)
test[num_var]<-sapply(test[num_var],function(x){M1_fun1(x)})
test1<-test##for getting unscaled values
test[num_var]<-sapply(test[num_var],function(x){M1_funout(x)})
test[num_var]<-sapply(test[num_var],function(x){(x-min(x))/(max(x)-min(x))})
#REPLACING NAN VALUES
test[is.na(test)] <- 0
table(test$month)
levels(test$month)

train$tripping1<-as.factor(train$tripping1)
test$tripping1<-as.factor(test$tripping1)
train$Approx_match<-as.factor(train$Approx_match)
test$Approx_match<-as.factor(test$Approx_match)

##################Last month sensitivity analysis###############################


#tripping_nextmonth is the data where the trippings are of last one month is already stored in HIVE
ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='default',all=TRUE) 

pred_lastmonth<-ore.pull(tripping_analysis_table)
lastm<-subset(finaldata2,date_>=as.Date(format(Sys.Date(),"%Y-%m-01"))-months(1)& 
                date_<as.Date(format(Sys.Date(),"%Y-%m-01")))
names(lastm)
lastm<-lastm[,c("Approx_match","date_","tripping1")]
pred_lastmonth<-pred_lastmonth[c("approx_match","sno1","pred_tripping")]
names(pred_lastmonth)[names(pred_lastmonth)=="approx_match"]<-"Approx_match"
fd1<-merge(pred_lastmonth,lastm,by="Approx_match",all.x=TRUE)

fd1<-fd1[!duplicated(fd1$Approx_match), ]

names(fd1)[names(fd1)=="tripping1"]<-"Current_monthActual_tripping"
fd1$correct_classification<-ifelse(fd1$Current_monthActual_tripping==fd1$pred_tripping,"correct","incorrect")
ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='default',all=TRUE) 

ore.drop("tripping_actual_pred0")
ore.create(fd1,"tripping_actual_pred0")

##############MODELLING FOR NEXT MONTH##########################
h2o.init(
  nthreads=-1,            ## -1: use all available threads
  max_mem_size = "300G")    ## specify the memory size for the H2O cloud
h2o.removeAll()           ## Clean slate - just in case the cluster was already running

datamodel_ <- as.h2o(train,destination_frame="datamodel_.hex")
test_ <- as.h2o(test,destination_frame="test_.hex")

# ntrees_opt <- c(200,300,400,500,600,700,800,900,1000) #nos of trees
ntrees_opt <- c(50,100,150,200,300,400,500,600,700,800,900,1000) #nos of trees
maxdepth_opt <- c(3,4,5) #size of tree
learnrate_opt <- c(0.1,0.15,0.2) #shrinkage value/learning rate
hypeper_parameters <- list(ntrees=ntrees_opt, max_depth=maxdepth_opt, learn_rate=learnrate_opt,stopping_rounds = 10,stopping_metric = "AUC",
                           stopping_tolerance = 0.0001,col_sample_rate=0.8)

names(datamodel_)

####GBM

grid_gbm_Jun<- h2o.grid("gbm", hyper_params = hypeper_parameters, y = "tripping1", x = c("Approx_match","trippingBD_lag_1"    ,                   "trippingBD_lag_12"  ,                   
                                                                                         "trippingBD_lag_24"     ,                 "trippingLD_lag_1"  ,                    
                                                                                         "trippingLD_lag_12"    ,                  "trippingLD_lag_24"    ,                 
                                                                                         "trippingLR_lag_1"    ,                   "trippingLR_lag_12"       ,              
                                                                                         "trippingLR_lag_24"       ,               "fault_typeTransien_fault_lag_1"   ,     
                                                                                         "fault_typeTransien_fault_lag_2"   ,      "fault_typeTransien_fault_lag_3"   ,     
                                                                                         "fault_typeTransien_fault_lag_4"    ,     "fault_typeTransien_fault_lag_5"  ,      
                                                                                         "fault_typeTransien_fault_lag_6"     ,    "fault_typeTree_fault_lag_1"    ,        
                                                                                         "fault_typeTree_fault_lag_2"    ,         "fault_typeTree_fault_lag_3"   ,         
                                                                                         "fault_typeTree_fault_lag_4"  ,           "fault_typeTree_fault_lag_5"   ,         
                                                                                         "fault_typeTree_fault_lag_6"   ,          "fault_typeAnimal_Electroc_lag_1"  ,     
                                                                                         "fault_typeAnimal_Electroc_lag_2" ,       "fault_typeAnimal_Electroc_lag_3" ,      
                                                                                         "fault_typeAnimal_Electroc_lag_4" ,       "fault_typeAnimal_Electroc_lag_5"  ,     
                                                                                         "fault_typeAnimal_Electroc_lag_6"   ,     "fault_typeCONDUCTORSNAPPED_lag_1"  ,    
                                                                                         "fault_typeCONDUCTORSNAPPED_lag_2"  ,     "fault_typeCONDUCTORSNAPPED_lag_3"   ,   
                                                                                         "fault_typeCONDUCTORSNAPPED_lag_4"   ,    "fault_typeCONDUCTORSNAPPED_lag_5"    ,  
                                                                                         "fault_typeCONDUCTORSNAPPED_lag_6" ,      "fault_typeFuse_blwn_out_mcb_trip_lag_1",
                                                                                         "fault_typeFuse_blwn_out_mcb_trip_lag_2","fault_typeFuse_blwn_out_mcb_trip_lag_3",
                                                                                         "fault_typeFuse_blwn_out_mcb_trip_lag_4","fault_typeFuse_blwn_out_mcb_trip_lag_5",
                                                                                         "fault_typeFuse_blwn_out_mcb_trip_lag_6","fault_typeSL_Broken_lag_1"   ,          
                                                                                         "fault_typeSL_Broken_lag_2",              "fault_typeSL_Broken_lag_3" ,            
                                                                                         "fault_typeSL_Broken_lag_4"       ,       "fault_typeSL_Broken_lag_5"    ,         
                                                                                         "fault_typeSL_Broken_lag_6"        ,      "fault_typeFault_Dist_Mains_lag_1"  ,    
                                                                                         "fault_typeFault_Dist_Mains_lag_2"  ,     "fault_typeFault_Dist_Mains_lag_3" ,     
                                                                                         "fault_typeFault_Dist_Mains_lag_4"   ,    "fault_typeFault_Dist_Mains_lag_5" ,     
                                                                                         "fault_typeFault_Dist_Mains_lag_6"     ,  "fault_typeTransformer_fault_lag_1"  ,   
                                                                                         "fault_typeTransformer_fault_lag_2"  ,    "fault_typeTransformer_fault_lag_3"   ,  
                                                                                         "fault_typeTransformer_fault_lag_4",      "fault_typeTransformer_fault_lag_5"  ,   
                                                                                         "fault_typeTransformer_fault_lag_6" ,     "fault_typeCable_Burnt_Fault_lag_1" ,    
                                                                                         "fault_typeCable_Burnt_Fault_lag_2",      "fault_typeCable_Burnt_Fault_lag_3"   ,  
                                                                                         "fault_typeCable_Burnt_Fault_lag_4" ,     "fault_typeCable_Burnt_Fault_lag_5"  ,   
                                                                                         "fault_typeCable_Burnt_Fault_lag_6" ,     "fault_typeBIRDAGE_lag_1"     ,          
                                                                                         "fault_typeBIRDAGE_lag_2"   ,             "fault_typeBIRDAGE_lag_3"  ,             
                                                                                         "fault_typeBIRDAGE_lag_4"        ,        "fault_typeBIRDAGE_lag_5" ,              
                                                                                         "fault_typeBIRDAGE_lag_6"    ,            "fault_typeHT_Mains_Failed_3366_lag_1"  ,
                                                                                         "fault_typeHT_Mains_Failed_3366_lag_2" ,  "fault_typeHT_Mains_Failed_3366_lag_3"  ,
                                                                                         "fault_typeHT_Mains_Failed_3366_lag_4" ,  "fault_typeHT_Mains_Failed_3366_lag_5",  
                                                                                         "fault_typeHT_Mains_Failed_3366_lag_6"   ,"fault_typeBurnt_Meter_lag_1"  ,         
                                                                                         "fault_typeBurnt_Meter_lag_2"    ,        "fault_typeBurnt_Meter_lag_3"    ,       
                                                                                         "fault_typeBurnt_Meter_lag_4"    ,        "fault_typeBurnt_Meter_lag_5"  ,         
                                                                                         "fault_typeBurnt_Meter_lag_6"      ,      "fault_typeShutdown_lag_1",              
                                                                                         "fault_typeShutdown_lag_2"      ,         "fault_typeShutdown_lag_3"   ,           
                                                                                         "fault_typeShutdown_lag_4"     ,          "fault_typeShutdown_lag_5" ,             
                                                                                         "fault_typeShutdown_lag_6"   ,            "fault_typeLoad_shedding_lag_1"  ,       
                                                                                         "fault_typeLoad_shedding_lag_2" ,         "fault_typeLoad_shedding_lag_3"   ,      
                                                                                         "fault_typeLoad_shedding_lag_4"    ,      "fault_typeLoad_shedding_lag_5"   ,      
                                                                                         "fault_typeLoad_shedding_lag_6"    ,      "fault_typeSupply_fail_lag_1"   ,        
                                                                                         "fault_typeSupply_fail_lag_2" ,           "fault_typeSupply_fail_lag_3" ,          
                                                                                         "fault_typeSupply_fail_lag_4"  ,          "fault_typeSupply_fail_lag_5"   ,        
                                                                                         "fault_typeSupply_fail_lag_6"   ,         "fault_typeFeederTrip_lag_1" ,           
                                                                                         "fault_typeFeederTrip_lag_2"  ,           "fault_typeFeederTrip_lag_3"  ,          
                                                                                         "fault_typeFeederTrip_lag_4"   ,          "fault_typeFeederTrip_lag_5"    ,        
                                                                                         "fault_typeFeederTrip_lag_6"   ,          "fault_typeOTHERFAULT_lag_1"   ,         
                                                                                         "fault_typeOTHERFAULT_lag_2" ,            "fault_typeOTHERFAULT_lag_3" ,           
                                                                                         "fault_typeOTHERFAULT_lag_4"  ,           "fault_typeOTHERFAULT_lag_5",
                                                                                         "avg_wert_lag_1"   ,                      "avg_wert_lag_2"  ,                      
                                                                                         "avg_wert_lag_3"  ,                       "avg_wert_lag_4"   ,                     
                                                                                         "avg_wert_lag_5"    ,                     "avg_wert_lag_6"  ,                      
                                                                                         "max_avg_lag_1"      ,                    "max_avg_lag_2"    ,                     
                                                                                         "max_avg_lag_3"      ,                    "max_avg_lag_4"    ,                     
                                                                                         "max_avg_lag_5"     ,                     "max_avg_lag_6"    ,                     
                                                                                         "mountingOverhead"         ,              "mountingUnderground"     ,              
                                                                                         "specificationABC"             ,          "specificationACSR"     ,                
                                                                                         "specificationPILCA"      ,               "specificationXLPE" ,                    
                                                                                         "num_sections"   )
                        , distribution="bernoulli", training_frame =
                          datamodel_,balance_classes = TRUE,nfolds=5,keep_cross_validation_predictions=TRUE,seed=23456)
grid_models_gbm_Jun <- lapply(grid_gbm_Jun@model_ids, function(model_id) { model = h2o.getModel(model_id) })

auc11<-c()
for (i in 1:length(grid_models_gbm_Jun)) {
  auc11[i]<-h2o.auc(grid_models_gbm_Jun[[i]])
}

Junvar4<-h2o.varimp(grid_models_gbm_Jun[[which.max(auc11)]])
Junvar5<-as.data.frame(Junvar4)

final_predictions_gbm_Jun_t<-h2o.predict(
  object = grid_models_gbm_Jun[[which.max(auc11)]],
  newdata = datamodel_)

Jun_train<-(final_predictions_gbm_Jun_t$p1)
Jun_train<-as.data.frame(Jun_train)

traind_Jun<-cbind(train,Prob=Jun_train$p1)

##ROC 
pred_Jun<-prediction(traind_Jun$Prob,traind_Jun$tripping1)
perf_Jun<-performance(pred_Jun,"tpr","fpr")
plot(perf_Jun,main="TRAINING SET",col="blue",lty=3, lwd=3)
abline(0, 1,col="red")
performance(pred_Jun, "auc")@y.values

cost.perf = performance(pred_Jun, "cost", cost.fp = 1, cost.fn = 1)
Junp<-pred_Jun@cutoffs[[1]][which.min(cost.perf@y.values[[1]])]

# Confusion matrix
table(actual=traind_Jun$tripping1,predicted=traind_Jun$Prob>=Junp)
traind_Jun$predict<-ifelse(traind_Jun$Prob>=Junp,1,0)


###TESTING DATA for current month prediction

finaltest<-h2o.predict(
  object =grid_models_gbm_Jun[[which.max(auc11)]],
  newdata = test_)

Jun_valid<-(finaltest$p1)

Jun_valid<-as.data.frame(Jun_valid)
Jun_valid<-cbind(test1,Prob=Jun_valid$p1)
Jun_valid$pred_tripping<-ifelse(Jun_valid$Prob>=Junp,1,0)
table(Jun_valid$pred_tripping)

names(Jun_valid)
Jun_valid<-merge(Jun_valid,master[c("s_no","concatenate1","adms_grid","adms_feeder","grid","feeder_name",
                                    "abr","feeder_indexing" ,"zone1_code","flash_consumer","district"
)],by.x="Approx_match",by.y="concatenate1",all.x=TRUE)

Jun_valid$Last_month_tripping<-rowSums(Jun_valid[c("trippingBD_lag_1","trippingLD_lag_1","trippingLR_lag_1")])
Jun_valid$Count_Overhead_Fault<-rowSums(Jun_valid[c( "fault_typeOverhead_fault_lag_1","fault_typeOverhead_fault_lag_2","fault_typeOverhead_fault_lag_3",  "fault_typeOverhead_fault_lag_4",         
                                                     "fault_typeOverhead_fault_lag_5","fault_typeOverhead_fault_lag_6")])

Jun_valid$Count_Transient_Fault<-rowSums(Jun_valid[c( "fault_typeTransien_fault_lag_1" , "fault_typeTransien_fault_lag_2" ,"fault_typeTransien_fault_lag_3","fault_typeTransien_fault_lag_4"            
                                                      ,"fault_typeTransien_fault_lag_5", "fault_typeTransien_fault_lag_6")])

Jun_valid$Count_AnimalElec_Fault<-rowSums(Jun_valid[c("fault_typeAnimal_Electroc_lag_1", "fault_typeAnimal_Electroc_lag_2"           
                                                      ,"fault_typeAnimal_Electroc_lag_3","fault_typeAnimal_Electroc_lag_4"           
                                                      ,"fault_typeAnimal_Electroc_lag_5", "fault_typeAnimal_Electroc_lag_6")])

Jun_valid$Count_Birdage_Fault<-rowSums(Jun_valid[c("fault_typeBIRDAGE_lag_1", "fault_typeBIRDAGE_lag_2" , "fault_typeBIRDAGE_lag_3","fault_typeBIRDAGE_lag_4",                 
                                                   "fault_typeBIRDAGE_lag_5" ,"fault_typeBIRDAGE_lag_6")])

Jun_valid$Count_Cable_Fault<-rowSums(Jun_valid[c("fault_typeCable_Burnt_Fault_lag_1" , "fault_typeCable_Burnt_Fault_lag_2"         
                                                 ,"fault_typeCable_Burnt_Fault_lag_3" ,"fault_typeCable_Burnt_Fault_lag_4", "fault_typeCable_Burnt_Fault_lag_5","fault_typeCable_Burnt_Fault_lag_6")])

Jun_valid$Count_Tree_Fault<-rowSums(Jun_valid[c("fault_typeTree_fault_lag_1","fault_typeTree_fault_lag_2","fault_typeTree_fault_lag_3" ,"fault_typeTree_fault_lag_4"                
                                                , "fault_typeTree_fault_lag_5","fault_typeTree_fault_lag_6"  )])

Jun_valid$Count_fault_typeFuse_blwn_out_mcb_trip<-rowSums(Jun_valid[c( "fault_typeFuse_blwn_out_mcb_trip_lag_1" ,"fault_typeFuse_blwn_out_mcb_trip_lag_2"    
                                                                       ,"fault_typeFuse_blwn_out_mcb_trip_lag_3" ,"fault_typeFuse_blwn_out_mcb_trip_lag_4"    
                                                                       ,"fault_typeFuse_blwn_out_mcb_trip_lag_5","fault_typeFuse_blwn_out_mcb_trip_lag_6" )])

names(Jun_valid)
Jun_valid<-data.frame(Jun_valid)
predicted<-Jun_valid[c("Approx_match" ,"s_no","year", "month", "date_" ,"adms_grid", "adms_feeder", "grid","feeder_name" , "abr", "feeder_indexing","flash_consumer" ,"Prob","avg_wert_lag_1" , "district","Count_Overhead_Fault", "Count_Transient_Fault",
                       "max_avg_lag_1", "tripping1","Count_AnimalElec_Fault","Count_Birdage_Fault","pred_tripping",
                       "Count_Cable_Fault","Count_Tree_Fault","Count_fault_typeFuse_blwn_out_mcb_trip","Last_month_tripping" )]

names(predicted)
#names(predicted)[names(predicted)=="s_no"]<-"sno1"
names(predicted)[names(predicted)=="Prob"]<-"Prob_Tripping"
names(predicted)[names(predicted)=="max_avg_lag_1"]<-"Max_load_Last_month"
names(predicted)[names(predicted)=="avg_wert_lag_1"]<-"Avg_load_Last_month"
predicted1<-predicted[!duplicated(predicted$Approx_match), ]

grid_operations_list1$concatenate1<-paste(grid_operations_list1$grid,grid_operations_list1$feeder_name)
grid_operations_list1$concatenate1=sapply(grid_operations_list1$concatenate1,toupper)
#grid_operations_list1$concatenate1=gsub("TO","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("GRID","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("(?!-)[[:punct:]]","",grid_operations_list1$concatenate1,perl=TRUE)
grid_operations_list1$concatenate1=gsub("$","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("=","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("+","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("~","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("^","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("\\s+"," ",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("  ","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub(" ","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("1","ONE",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("2","TWO",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("3","THREE",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("4","FOUR",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("-III{1}","-THREE",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("III{1}$","-THREE",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("-II{1}","-TWO",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("-I{1}","-ONE",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("10{1}","TEN",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1[grid_operations_list1$concatenate1==""]<-"MISS"
grid_operations_list1$concatenate1[is.na(grid_operations_list1$concatenate1)]<-"MISS"

grid_operations_list2<-data.frame(unique(grid_operations_list1$concatenate1))
names(grid_operations_list2)<-"concatenate1"
grid_operations_list2<-subset(grid_operations_list2,concatenate1!='MISS')

grid_operations_list2$Approx_match<-c()
grid_operations_list2$Approx_match1<-c()
grid_operations_list2$Approx_match2<-c()
grid_operations_list2$Approx_match3<-c()

for( i in 1: NROW(grid_operations_list2))
{
  grid_operations_list2$Approx_match[i]<-master$concatenate1[which.min(stringdist(grid_operations_list2$concatenate1[i],master$concatenate_adms,method='jaccard',q=2,nthread=100))]
  grid_operations_list2$Approx_match1[i]<-master$concatenate1[which.min(stringdist(grid_operations_list2$concatenate1[i],master$concatenate_adms,method='cosine',q=2,nthread=100))]
  grid_operations_list2$Approx_match2[i]<-master$concatenate1[which.min(stringdist(grid_operations_list2$concatenate1[i],master$concatenate_adms,method='osa',q=2,nthread=100))]
  grid_operations_list2$Approx_match3[i]<-master$concatenate1[which.min(stringdist(grid_operations_list2$concatenate1[i],master$concatenate_adms,method='dl',nthread=100))]
  
}

finalgrid_operations_list<-merge(grid_operations_list1,grid_operations_list2,by="concatenate1",all.x=TRUE)
finalgrid_operations_list$Auto_reclosureAt_Grid<-"YES"

Jun_valid1<-merge(predicted1,finalgrid_operations_list[c("Approx_match","Auto_reclosureAt_Grid")],by=c("Approx_match"),all.x=TRUE)
Jun_valid1$Auto_reclosureAt_Grid<-ifelse(is.na(Jun_valid1$Auto_reclosureAt_Grid),"NO",Jun_valid1$Auto_reclosureAt_Grid)

table(is.na(Jun_valid1$Auto_reclosureAt_Grid))

names(pole_mounted1)
pole_mounted1$concatenate1<-paste(pole_mounted1$name_of_grid_11_kv_sub_stn,pole_mounted1$name_of_feeder_equipment)
pole_mounted1$concatenate1=sapply(pole_mounted1$concatenate1,toupper)
#pole_mounted1$concatenate1=gsub("TO","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("GRID","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("(?!-)[[:punct:]]","",pole_mounted1$concatenate1,perl=TRUE)
pole_mounted1$concatenate1=gsub("$","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("=","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("+","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("~","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("^","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("\\s+"," ",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("  ","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub(" ","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("1","ONE",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("2","TWO",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("3","THREE",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("4","FOUR",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("-III{1}","-THREE",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("III{1}$","-THREE",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("-II{1}","-TWO",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("-I{1}","-ONE",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("10{1}","TEN",pole_mounted1$concatenate1)
pole_mounted1$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",pole_mounted1$concatenate1)
pole_mounted1$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",pole_mounted1$concatenate1)
pole_mounted1$concatenate1[pole_mounted1$concatenate1==""]<-"MISS"
pole_mounted1$concatenate1[is.na(pole_mounted1$concatenate1)]<-"MISS"

pole_mounted2<-data.frame(unique(pole_mounted1$concatenate1))
names(pole_mounted2)<-"concatenate1"
pole_mounted2<-subset(pole_mounted2,concatenate1!='MISS')

pole_mounted2$Approx_match<-c()
pole_mounted2$Approx_match1<-c()
pole_mounted2$Approx_match2<-c()
pole_mounted2$Approx_match3<-c()

for( i in 1: NROW(pole_mounted2))
{
  pole_mounted2$Approx_match[i]<-master$concatenate1[which.min(stringdist(pole_mounted2$concatenate1[i],master$concatenate_adms,method='jaccard',q=2,nthread=100))]
  pole_mounted2$Approx_match1[i]<-master$concatenate1[which.min(stringdist(pole_mounted2$concatenate1[i],master$concatenate_adms,method='cosine',q=2,nthread=100))]
  pole_mounted2$Approx_match2[i]<-master$concatenate1[which.min(stringdist(pole_mounted2$concatenate1[i],master$concatenate_adms,method='osa',q=2,nthread=100))]
  pole_mounted2$Approx_match3[i]<-master$concatenate1[which.min(stringdist(pole_mounted2$concatenate1[i],master$concatenate_adms,method='dl',nthread=100))]
  
}

finalpole_mounted<-merge(pole_mounted1,pole_mounted2,by="concatenate1",all.x=TRUE)

table(finalpole_mounted$type_of_autorecloser)
finalpole_mounted<-subset(finalpole_mounted,type_of_autorecloser=="Pole Mounted AR")
finalpole_mounted<-finalpole_mounted[c("concatenate1" ,"name_of_grid_11_kv_sub_stn","name_of_feeder_equipment" ,
                                       "Approx_match", "Approx_match1","Approx_match2" , "Approx_match3" )]

finalpole_mounted<-unique(finalpole_mounted)
finalpole_mounted$Pole_mounted_Reclosure<-"YES"

Jun_valid2<-merge(Jun_valid1,finalpole_mounted[c("Approx_match","Pole_mounted_Reclosure")],by=c("Approx_match"),all.x=TRUE)
Jun_valid2$Pole_mounted_Reclosure<-ifelse(is.na(Jun_valid2$Pole_mounted_Reclosure),"NO",Jun_valid2$Pole_mounted_Reclosure)

table(is.na(Jun_valid2$Pole_mounted_Reclosure))

ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='default',all=TRUE) 

Jun_valid2<-Jun_valid2[!duplicated(Jun_valid2$Approx_match), ]
names(Jun_valid2)[names(Jun_valid2)=="s_no"]<-"sno1"
ore.drop("tripping_analysis_table0")
ore.create(Jun_valid2[c("Approx_match","sno1","year" ,"date_","Prob_Tripping", "Avg_load_Last_month"                   
                        , "Count_Overhead_Fault" ,"Count_Transient_Fault","Max_load_Last_month","tripping1", "Count_AnimalElec_Fault"                
                        ,"Count_Birdage_Fault" , "pred_tripping" ,"Count_Cable_Fault" ,"Count_Tree_Fault" ,"Count_fault_typeFuse_blwn_out_mcb_trip","Last_month_tripping"                   
                        ,"Auto_reclosureAt_Grid" ,"Pole_mounted_Reclosure")],"tripping_analysis_table0")

#############NORMAL ABNORAMAL#############################################################################
dat1<-bind3[!bind3$Approx_match=='N'|!bind3$Approx_match=='NA',]
uniq_bind<-dat1[!duplicated(dat1[,'reference_label']),]
table(uniq_bind$status)
table(uniq_bind$tripping)
uniq_bind$end_time<-as.Date(uniq_bind$end_time,"%Y-%m-%d")
uniq_bind$month<-format(uniq_bind$end_time,"%m")
uniq_bind1<-subset(uniq_bind,end_time>=as.Date(format(Sys.Date(),"%Y-%m-01"))-months(1)& 
                     end_time<as.Date(format(Sys.Date(),"%Y-%m-01")))
uniq_bind1$day<-as.numeric(uniq_bind1$day)
BD1<-uniq_bind1[uniq_bind1$tripping=='BD',]
names(BD1)
########Classifying trunk section of feeder with Underground or Overhead#######

trunk1$concatenate1<-paste(trunk1$grid,trunk1$feeder)
trunk1$concatenate1=sapply(trunk1$concatenate1,toupper)
#trunk1$concatenate1=gsub("TO","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("GRID","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("(?!-)[[:punct:]]","",trunk1$concatenate1,perl=TRUE)
trunk1$concatenate1=gsub("$","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("=","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("+","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("~","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("^","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("\\s+"," ",trunk1$concatenate1)
trunk1$concatenate1=gsub("  ","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub(" ","",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("1","ONE",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("2","TWO",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("3","THREE",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("4","FOUR",trunk1$concatenate1,fixed=TRUE)
trunk1$concatenate1=gsub("-III{1}","-THREE",trunk1$concatenate1)
trunk1$concatenate1=gsub("III{1}$","-THREE",trunk1$concatenate1)
trunk1$concatenate1=gsub("-II{1}","-TWO",trunk1$concatenate1)
trunk1$concatenate1=gsub("-I{1}","-ONE",trunk1$concatenate1)
trunk1$concatenate1=gsub("10{1}","TEN",trunk1$concatenate1)
trunk1$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",trunk1$concatenate1)
trunk1$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",trunk1$concatenate1)
trunk1$concatenate1[trunk1$concatenate1==""]<-"MISS"
trunk1$concatenate1[is.na(trunk1$concatenate1)]<-"MISS"

trunk2<-data.frame(unique(trunk1$concatenate1))
names(trunk2)<-"concatenate1"
trunk2<-subset(trunk2,concatenate1!='MISS')

trunk2$Approx_match<-c()
trunk2$Approx_match1<-c()
trunk2$Approx_match2<-c()
trunk2$Approx_match3<-c()

for( i in 1: NROW(trunk2))
{
  trunk2$Approx_match[i]<-master$concatenate1[which.min(stringdist(trunk2$concatenate1[i],master$concatenate_adms,method='jaccard',q=2,nthread=100))]
  trunk2$Approx_match1[i]<-master$concatenate1[which.min(stringdist(trunk2$concatenate1[i],master$concatenate_adms,method='cosine',q=2,nthread=100))]
  trunk2$Approx_match2[i]<-master$concatenate1[which.min(stringdist(trunk2$concatenate1[i],master$concatenate_adms,method='osa',q=2,nthread=100))]
  trunk2$Approx_match3[i]<-master$concatenate1[which.min(stringdist(trunk2$concatenate1[i],master$concatenate_adms,method='dl',nthread=100))]
  
}

finaltrunk<-merge(trunk1,trunk2,by="concatenate1",all.x=TRUE)

colnames(finaltrunk)[colnames(finaltrunk)=="fss_sub_stn_name"]<-"Trunk_Feeder"
finaltrunk<-setDT(finaltrunk)[,c("type1","type2","type3"):=tstrsplit(network_type,")")]
finaltrunk<-data.frame(finaltrunk)
#checking over each 3 column if A1 or Dog then put 1 else put 0
finaltrunk[c("Trunk_Network_Overhead_type1","Trunk_Network_Overhead_type2","Trunk_Network_Overhead_type3")]<-ifelse(sapply(finaltrunk[c("type1","type2","type3")],
                                                                                                                           function(x){grepl("A1",x,fixed=TRUE)|grepl("DOG",x,fixed=TRUE)}),1,0)

finaltrunk[c("Trunk_Network_Ug_type1","Trunk_Network_Ug_type2","Trunk_Network_Ug_type3")]<-ifelse(sapply(finaltrunk[c("type1","type2","type3")],
                                                                                                         function(x){!grepl("A1",x,fixed=TRUE)|!grepl("DOG",x,fixed=TRUE)}),1,0)
#str_extract(data, regexp)
finaltrunk[c("Type1_p","Type2_p","Type3_p")]<-sapply(finaltrunk[c("type1","type2","type3")],function(x){str_extract(x,"\\d+%")})

finaltrunk[c("Type1_p","Type2_p","Type3_p")]<-sapply(finaltrunk[c("Type1_p","Type2_p","Type3_p")],function(x){gsub("%","",x)})
finaltrunk[c("Type1_p","Type2_p","Type3_p")]<-sapply(finaltrunk[c("Type1_p","Type2_p","Type3_p")],function(x){x<-as.numeric(x)})

finaltrunk[c("Trunk_Network_Overhead_type1","Trunk_Network_Ug_type1")]<-finaltrunk[c("Trunk_Network_Overhead_type1","Trunk_Network_Ug_type1")]*finaltrunk[["Type1_p"]]
finaltrunk[c("Trunk_Network_Overhead_type2","Trunk_Network_Ug_type2")]<-finaltrunk[c("Trunk_Network_Overhead_type2","Trunk_Network_Ug_type2")]*finaltrunk[["Type2_p"]]
finaltrunk[c("Trunk_Network_Overhead_type3","Trunk_Network_Ug_type3")]<-finaltrunk[c("Trunk_Network_Overhead_type3","Trunk_Network_Ug_type3")]*finaltrunk[["Type3_p"]]

names(finaltrunk)
finaltrunk1<-finaltrunk[c("Trunk_Feeder","network_type","length_in_metres","Trunk_Network_Overhead_type1" ,"Trunk_Network_Overhead_type2",
                          "Trunk_Network_Overhead_type3","Trunk_Network_Ug_type1", "Trunk_Network_Ug_type2","Trunk_Network_Ug_type3","Approx_match")]

########## LOGIC 1 : UNDERGORUND AND BD is happening and fault type is Birdage,Animal Elect,Transient is  thus Abnormal Tripping##################

logic1<-merge(BD1,finaltrunk1,by="Approx_match",all.x=TRUE)
#dfi[,c(2:109,174:185)][is.na(dfi[,c(2:109,174:185)])]<-0
# logic1[,][is.na(logic1[,])]<-"missing"
# logic1<-logic1[[is.na[logic1)]]<-"missing"
table(logic1$fault_type)
logic1$HighIntensityFault<-ifelse(logic1$fault_type %in% c("Animal_Electroc","BIRDAGE","Transien_fault") & (logic1$Trunk_Network_Ug_type1>=50|logic1$Trunk_Network_Ug_type2>=50|
                                                                                                              logic1$Trunk_Network_Ug_type3>=50),"HighIntensityFault","Normal")



#########LOGIC 2 : If Trunk Section of Feeder is underground and the fault type is other than Cable Fault HT ABC conductor snapped then classify as Abnormal otherwise Normal
##then Abnormal Tripping############


logic1$NormalAbnormal_Flag<-ifelse(!logic1$fault_type %in% c("CONDUCTORSNAPPED","Cable_Burnt_Fault","HTABCFAULTY","HTCABLEFAULTY") & (logic1$Trunk_Network_Ug_type1>=50|logic1$Trunk_Network_Ug_type2>=50|
                                                                                                                                        logic1$Trunk_Network_Ug_type3>=50),"Abnormal","Normal")

##########LOGIC 3 : AUTO RECLOSURE FEEDER LIST on Grid########

names(grid_operations_list1)
grid_operations_list1$concatenate1<-paste(grid_operations_list1$grid,grid_operations_list1$feeder_name)
grid_operations_list1$concatenate1=sapply(grid_operations_list1$concatenate1,toupper)
#grid_operations_list1$concatenate1=gsub("TO","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("GRID","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("(?!-)[[:punct:]]","",grid_operations_list1$concatenate1,perl=TRUE)
grid_operations_list1$concatenate1=gsub("$","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("=","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("+","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("~","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("^","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("\\s+"," ",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("  ","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub(" ","",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("1","ONE",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("2","TWO",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("3","THREE",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("4","FOUR",grid_operations_list1$concatenate1,fixed=TRUE)
grid_operations_list1$concatenate1=gsub("-III{1}","-THREE",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("III{1}$","-THREE",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("-II{1}","-TWO",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("-I{1}","-ONE",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1=gsub("10{1}","TEN",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",grid_operations_list1$concatenate1)
grid_operations_list1$concatenate1[grid_operations_list1$concatenate1==""]<-"MISS"
grid_operations_list1$concatenate1[is.na(grid_operations_list1$concatenate1)]<-"MISS"

grid_operations_list2<-data.frame(unique(grid_operations_list1$concatenate1))
names(grid_operations_list2)<-"concatenate1"
grid_operations_list2<-subset(grid_operations_list2,concatenate1!='MISS')

grid_operations_list2$Approx_match<-c()
grid_operations_list2$Approx_match1<-c()
grid_operations_list2$Approx_match2<-c()
grid_operations_list2$Approx_match3<-c()

for( i in 1: NROW(grid_operations_list2))
{
  grid_operations_list2$Approx_match[i]<-master$concatenate1[which.min(stringdist(grid_operations_list2$concatenate1[i],master$concatenate_adms,method='jaccard',q=2,nthread=100))]
  grid_operations_list2$Approx_match1[i]<-master$concatenate1[which.min(stringdist(grid_operations_list2$concatenate1[i],master$concatenate_adms,method='cosine',q=2,nthread=100))]
  grid_operations_list2$Approx_match2[i]<-master$concatenate1[which.min(stringdist(grid_operations_list2$concatenate1[i],master$concatenate_adms,method='osa',q=2,nthread=100))]
  grid_operations_list2$Approx_match3[i]<-master$concatenate1[which.min(stringdist(grid_operations_list2$concatenate1[i],master$concatenate_adms,method='dl',nthread=100))]
  
}

finalgrid_operations_list<-merge(grid_operations_list1,grid_operations_list2,by="concatenate1",all.x=TRUE)
finalgrid_operations_list$Auto_reclosureAt_Grid<-"YES"
finalgrid_operations_list<-unique(finalgrid_operations_list)
BD1<-unique(BD1)
#merging this feeder list with the feeder list of the current month where BD has happened.
logic3<-merge(BD1,finalgrid_operations_list[c("Approx_match","Auto_reclosureAt_Grid")],by="Approx_match",all.x=TRUE)
logic3$Auto_reclosureAt_Grid<-ifelse(is.na(logic3$Auto_reclosureAt_Grid),"NO",logic3$Auto_reclosureAt_Grid)
logic3$Urgent_Maintenance_Flag1<-ifelse(logic3$Auto_reclosureAt_Grid=="YES","YES","NO")


###############POLE MOUNTED RECLOSURE FEEDER LIST
names(pole_mounted1)
pole_mounted1$concatenate1<-paste(pole_mounted1$name_of_grid_11_kv_sub_stn,pole_mounted1$name_of_feeder_equipment)
pole_mounted1$concatenate1=sapply(pole_mounted1$concatenate1,toupper)
#pole_mounted1$concatenate1=gsub("TO","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("GRID","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("(?!-)[[:punct:]]","",pole_mounted1$concatenate1,perl=TRUE)
pole_mounted1$concatenate1=gsub("$","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("=","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("+","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("~","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("^","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("\\s+"," ",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("  ","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub(" ","",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("1","ONE",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("2","TWO",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("3","THREE",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("4","FOUR",pole_mounted1$concatenate1,fixed=TRUE)
pole_mounted1$concatenate1=gsub("-III{1}","-THREE",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("III{1}$","-THREE",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("-II{1}","-TWO",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("-I{1}","-ONE",pole_mounted1$concatenate1)
pole_mounted1$concatenate1=gsub("10{1}","TEN",pole_mounted1$concatenate1)
pole_mounted1$concatenate1<-gsub("FEEDER-1|FEEDER1","FDR-ONE",pole_mounted1$concatenate1)
pole_mounted1$concatenate1<-gsub("FEEDER-2|FEEDER2","FDR-TWO",pole_mounted1$concatenate1)
pole_mounted1$concatenate1[pole_mounted1$concatenate1==""]<-"MISS"
pole_mounted1$concatenate1[is.na(pole_mounted1$concatenate1)]<-"MISS"

pole_mounted2<-data.frame(unique(pole_mounted1$concatenate1))
names(pole_mounted2)<-"concatenate1"
pole_mounted2<-subset(pole_mounted2,concatenate1!='MISS')

pole_mounted2$Approx_match<-c()
pole_mounted2$Approx_match1<-c()
pole_mounted2$Approx_match2<-c()
pole_mounted2$Approx_match3<-c()

for( i in 1: NROW(pole_mounted2))
{
  pole_mounted2$Approx_match[i]<-master$concatenate1[which.min(stringdist(pole_mounted2$concatenate1[i],master$concatenate_adms,method='jaccard',q=2,nthread=100))]
  pole_mounted2$Approx_match1[i]<-master$concatenate1[which.min(stringdist(pole_mounted2$concatenate1[i],master$concatenate_adms,method='cosine',q=2,nthread=100))]
  pole_mounted2$Approx_match2[i]<-master$concatenate1[which.min(stringdist(pole_mounted2$concatenate1[i],master$concatenate_adms,method='osa',q=2,nthread=100))]
  pole_mounted2$Approx_match3[i]<-master$concatenate1[which.min(stringdist(pole_mounted2$concatenate1[i],master$concatenate_adms,method='dl',nthread=100))]
  
}

finalpole_mounted<-merge(pole_mounted1,pole_mounted2,by="concatenate1",all.x=TRUE)

table(finalpole_mounted$type_of_autorecloser)
finalpole_mounted<-subset(finalpole_mounted,type_of_autorecloser=="Pole Mounted AR")
finalpole_mounted<-finalpole_mounted[c("concatenate1" ,"name_of_grid_11_kv_sub_stn","name_of_feeder_equipment" ,
                                       "Approx_match", "Approx_match1","Approx_match2" , "Approx_match3" )]

finalpole_mounted<-unique(finalpole_mounted)
finalpole_mounted$Pole_mounted_Reclosure<-"YES"
#merging this feeder list with the feeder list of the current month where BD has happened.
logic4<-merge(BD1,finalpole_mounted[c("Approx_match","Pole_mounted_Reclosure")],by="Approx_match",all.x=TRUE)
logic4$Pole_mounted_Reclosure<-ifelse(is.na(logic4$Pole_mounted_Reclosure),"NO",logic4$Pole_mounted_Reclosure)
logic4$Urgent_Maintenance_Flag2<-ifelse(logic4$Pole_mounted_Reclosure=="YES","YES","NO")



######################### LOGIC 5: Abnormal Rate of Tripping from last year then Abnormal Tripping#####################
###########################################CHANGEPOINT ANALYSIS#################################################################
#########FEEDER WISE COUNT####

uniq_bind2<-subset(uniq_bind,end_time>=(as.Date(format(Sys.Date(),"%Y-%m-01"))-years(1))-months(1)& 
                     end_time<as.Date(format(Sys.Date(),"%Y-%m-01")))
# gh<-subset(uniq_bind1,year==2016&month=="12")
# table(gh$month)
uniq_bind2$day<-as.numeric(uniq_bind2$day)
BD2<-uniq_bind2[uniq_bind2$tripping=='BD',]
names(BD1)

BD2<-data.frame(BD2)
BD2$type<-1
BD2$type<-as.numeric(BD2$type)
BD2$month<-as.numeric(BD2$month)
BD2$year<-as.numeric(BD2$year)
BD2$day<-as.numeric(BD2$day)

bdcnt2<-aggregate(BD2$type,by=list(BD2$Approx_match,BD2$year,BD2$month),sum)
names(bdcnt2)<-gsub(" ", "_",names(bdcnt2),fixed=T)
names(bdcnt2)<-gsub(".", "_",names(bdcnt2),fixed=T)
bdcntt2<-arrange(bdcnt2,Group_2,Group_3)
bdcntt2$time<-paste(bdcntt2$Group_2,bdcntt2$Group_3)

bdcntt2$time<-gsub(" ","_",bdcntt2$time,fixed=TRUE)
d<-dummy_cols(bdcntt2$time)
bdcnt3<-cbind(bdcntt2,d)
names(bdcnt3)<-gsub(".","",names(bdcnt3),fixed=TRUE)
names(bdcnt3)
#CHANGE POINT ANALYSIS FOR DETECTING CHANGE IN PATTERN OF TRIPPING
bdcnt3[-(1:6)]<-bdcnt3[["x"]]*bdcnt3[-(1:6)]
names(bdcnt3)
#bdcnt1<-aggregate(bdcnt3[,c(8:ncol(bdcnt3))],by=list(bdcnt3$Group_1),sum)

##CHECK INDICES last 1 year tripping from current month
bdcnt1<-aggregate(bdcnt3[,c(10:(ncol(bdcnt3)))],by=list(bdcnt3$Group_1),sum)
names(bdcnt1)[ncol(bdcnt1)]
str(bdcnt1$data_2018_7)
bdcnt11<-subset(bdcnt1,bdcnt1[ncol(bdcnt1)]>=1)
names(bdcnt11)
bdcnt11$BD_cnt_oneYear<-rowSums(bdcnt11[c(2:NCOL(bdcnt11))])
names(bdcnt11)
bdcnt2<-as.matrix(bdcnt11[2:(NCOL(bdcnt11)-1)])
#METHOD 1
m.pelt <- cpt.meanvar(bdcnt2, method = "PELT",penalty='Manual',pen.value='2*log(n)',minseglen=2)
#summary(m.pelt)
xx<-unlist(m.pelt)
# plot(xx[[1]], type = "l", cpt.col = "blue", xlab = "Index",cpt.width = 4)
cpts(xx[[1]])
ncpts(xx[[1]])

bdcnt11$cnt_changept_1<-lapply(1:NROW(bdcnt11),function(x){ncpts(xx[[x]]) })
bdcnt11$cnt_changept_1<-as.numeric(bdcnt11$cnt_changept_1)
y<-summary(bdcnt11$cnt_changept_1)
y
bdcnt11$Type_trip_cpt<-lapply(1:NROW(bdcnt11),function(x){ifelse(bdcnt11$cnt_changept_1[x]>1,"Abnormal","Normal")})

names(bdcnt11)<-gsub(".","_",names(bdcnt11),fixed=TRUE)
bdcnt2<-bdcnt11
bdcnt2$cnt_changept_1<-as.numeric(bdcnt2$cnt_changept_1)
bdcnt2$Type_trip_cpt<-as.character(bdcnt2$Type_trip_cpt)
bdcnt6<-merge(bdcnt2,finaltrunk1[c("Approx_match","Trunk_Network_Overhead_type1","Trunk_Network_Overhead_type2", "Trunk_Network_Overhead_type3")],by.x="Group_1",by="Approx_match",all.x=TRUE)
bdcnt6[,c("Trunk_Network_Overhead_type1","Trunk_Network_Overhead_type2", "Trunk_Network_Overhead_type3")][is.na(bdcnt6[,c("Trunk_Network_Overhead_type1","Trunk_Network_Overhead_type2", "Trunk_Network_Overhead_type3")])]<-0

names(bdcnt6)

bdcnt6$Type_trip_cpt<-ifelse((bdcnt6$Trunk_Network_Overhead_type1>=50|bdcnt6$Trunk_Network_Overhead_type2>=50|bdcnt6$Trunk_Network_Overhead_type3>=50),"Normal",bdcnt6$Type_trip_cpt)

###############MERGING LOGIC 1,2,3,4 #######################################
#logic1 logic2 logic3 logic4 bdcnt6

names(logic1)
names(logic3)
names(logic4)
names(bdcnt6)

#merging logic 3 and logic 4
logic3<-unique(logic3)
logic4<-unique(logic4)
logic1<-unique(logic1)

an1<-merge(logic3,logic4[c("Approx_match","reference_label","Pole_mounted_Reclosure","Urgent_Maintenance_Flag2")],by=c("Approx_match","reference_label"),all=TRUE)
an1<-unique(an1[c("Approx_match","reference_label" ,"Auto_reclosureAt_Grid","Urgent_Maintenance_Flag1" ,"Pole_mounted_Reclosure","Urgent_Maintenance_Flag2")])
an2<-merge(an1[c("Approx_match","reference_label" ,"Auto_reclosureAt_Grid","Urgent_Maintenance_Flag1" ,"Pole_mounted_Reclosure","Urgent_Maintenance_Flag2")],
           logic1,by=c("Approx_match","reference_label"))

an2<-unique(an2)
names(an2)
names(bdcnt6)
bdcnt6<-unique(bdcnt6)
an3<-merge(an2,bdcnt6[-c(16,18:20)],by.x="Approx_match",by.y="Group_1",all=TRUE)
names(an3)

names(an3)[names(an3)=="Type_trip_cpt"]<-"Abnormal Rate of BD"
names(an3)[names(an3)=="BD_cnt_oneYear"]<-"Count_BD_1year"
names(an3)<-gsub("data","Count_BD",fixed=TRUE,names(an3))

an3$inter_dur<-as.numeric(an3$inter_dur)
an3<-unique(an3)
names(an3)
ore.disconnect()
ore.connect(type='HIVE', host='dc1-srvbda1node04.ndpl.com',user='oracle', 
            password='welcome1',port='10000',schema='default',all=TRUE) 
ore.ls()
ore.drop("report_normal_abnormal")
ore.create(an3,"report_normal_abnormal")
