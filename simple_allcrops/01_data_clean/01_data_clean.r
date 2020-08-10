# ----------------------------------------------------------------------------
# Preparing FAOSTAT data for creating SIMPLE database
# Coded by UBaldos 07-05-2017
# ----------------------------------------------------------------------------
# This code creates country level data which will be aggregated later 
# via a GEMPACK program
#
# =============================== #
# General processing for all data #
# =============================== #
#


# ----- clear R memorylist
rm(list=ls())

# getting user input from console
args <-  commandArgs(trailingOnly=TRUE)

val1 = args[1]
val2 = args[2]
val3 = args[3]
val3 = toString(val3)
val4 = args[4]
val4 = toString(val4)
val5 = args[5]
val5 = toString(val5)
val6 = args[6]
val6 = toString(val6)
val7 = args[7]
val7 = toString(val7)


#s0 = paste(".",val4,sep = "/")
s_dir = paste(val4,"/")
#s_fin = paste(s0,"/")
search_string = ' '
replace_string = ''
s_dir = sub(search_string,replace_string,s_dir)
#s_fin = sub(search_string,replace_string,s_fin)

#          Delete 'out' folder
unlink(val4, recursive = TRUE)    
dir.create(val4, recursive = TRUE) # create 'temp' folder
s_temp = paste(val4,"/temp",sep="")
#          Delete 'temp' folder
unlink(s_temp, recursive = TRUE)    
dir.create(s_temp, recursive = TRUE)



# ----- define folders for processing data if needed
#setwd("C:/Users/ubaldos/Desktop/SIMPLE Base Data/01_data_clean/")
#d1 = paste(val3,"Inputs_LandUse_E_All_Data_(Normalized).zip",sep="/")
#d2 = paste(val3,"Macro-Statistics_Key_Indicators_E_All_Data_(Normalized).zip",sep="/")
#d3 = paste(val3,"Population_E_All_Data_(Normalized).zip",sep="/")
#d4 = paste(val3,"Prices_E_All_Data_(Normalized).zip",sep="/")
#d5 = paste(val3,"Production_Crops_E_All_Data_(Normalized).zip",sep="/")
#d6 = paste(val3,"Production_LivestockPrimary_E_All_Data_(Normalized).zip",sep="/")
#download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Inputs_LandUse_E_All_Data_(Normalized).zip", d1)
#download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Macro-Statistics_Key_Indicators_E_All_Data_(Normalized).zip", d2)
#download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Population_E_All_Data_(Normalized).zip", d3)
#download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Prices_E_All_Data_(Normalized).zip", d4)
#download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Production_Crops_E_All_Data_(Normalized).zip", d5)
#download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Production_LivestockPrimary_E_All_Data_(Normalized).zip", d6)

# ----- read in all zip files and unzip in 'temp' folder
   zipfile <- list.files(pattern = "\\.zip$",  recursive = TRUE)
   for(j in zipfile){unzip(zipfile =  j, exdir =s_temp)}

# ----- define relevant sets for base data #need to change from hard coded values to dynamic variables
# have variables for directories in, out and temp (temp directory should be deleted by the end of running the code)   

#s1 = paste(val4,val5, sep = "/")
#s2 = paste(val4,val6,sep = "/")
#s3 = paste(val4,val7,sep = "/")
s4 = paste(val4, "QLAND.csv",sep = "/")
s5 = paste(val4, "INC.csv",sep = "/")
s6 = paste(val4, "POP.csv", sep = "/")
s7 = paste(val4, "00_wldcropprice.csv", sep = "/")
s8 = paste(val4, "00_wldcornprice.csv", sep = "/")
s9 = paste(val4, "00_WldCornEqPrice.csv", sep = "/")
s10 = paste(val4, "QCROP.csv", sep = "/")
s11 = paste(val4, "VCROP.csv", sep = "/")
s12 = paste(val3, "reg_map.csv", sep = "/")

start_year <- as.numeric(val1)
end_year <- as.numeric(val2)
years     <- seq(start_year,end_year,1) # start year and end year
country   <- read.csv(val5) #reg_sets.csv
country   <- country[,1]
crop      <- read.csv(val6) #crop_sets.csv
crop      <- crop[,1]
livestock <- read.csv(val7) #livestock_sets.csv
livestock <- livestock[,1]


# ----- re-read all csv files with filtered data and process one by one
csvfile <- list.files(path = s_temp, pattern = "\\.csv$", full.names = TRUE)

# ======================== #
# Data specific processing #
# ======================== #
# These codes do the following: 
#   Read data, get subset data depending on year and variable of interest as well as 
#   SIMPLE country and crop coverage. Then drop observations with NA values for each 
#   and finally write filtered data

# 	Arable land and Permanent crops
# ----- Filter data to get GDP (Item.Code) in 2005 USD (Element.Code) and re-write file
s_pc_1 = paste(s_temp,"Inputs_LandUse_E_All_Data_(Normalized).csv",sep="/")
s_pc_2 = paste(s_temp, "00_cropland.csv",sep="/")
datatable  <- read.csv(s_pc_1)
datatable2 <- subset(datatable, select=c("Area.Code", "Item.Code", "Element.Code", "Year.Code", "Value"))
datatable2 <- subset(datatable2,  Item.Code==6620) 
datatable2 <- subset(datatable2,  Element.Code==5110) 
datatable2 <- datatable2[datatable2$ Area.Code %in% country,] 
datatable2 <- datatable2[datatable2$Year %in% years,]
datatable2 <- subset(datatable2, select=c("Area.Code", "Year.Code", "Value"))  
datatable2 <- datatable2[complete.cases(datatable2),]
write.csv(datatable2,s_pc_2, row.names=FALSE )
names(datatable2) <- c("CNTRY","YEAR","QLAND")
write.csv(datatable2, s4, row.names=FALSE )


# GDP in 2015 USD
# ----- Filter data to get GDP (Item.Code) in 2005 USD (Element.Code) and re-write file
s_gdp_1 = paste(s_temp,"Macro-Statistics_Key_Indicators_E_All_Data_(Normalized).csv",sep="/")
s_gdp_2 = paste(s_temp ,"00_realgdp.csv",sep="/")
datatable  <- read.csv(s_gdp_1)
datatable2 <- subset(datatable, select=c("Area.Code", "Item.Code", "Element.Code", "Year.Code", "Value"))
datatable2 <- subset(datatable2,  Item.Code==22008) 
datatable2 <- subset(datatable2,  Element.Code==6184) 
datatable2 <- datatable2[datatable2$ Area.Code %in% country,] 
datatable2 <- datatable2[datatable2$Year %in% years,]
datatable2 <- subset(datatable2, select=c("Area.Code", "Year.Code", "Value"))
datatable2 <- datatable2[complete.cases(datatable2),]
write.csv(datatable2,s_gdp_2, row.names=FALSE )
names(datatable2) <- c("CNTRY","YEAR","INC")
write.csv(datatable2, s5, row.names=FALSE )

# Population
# ----- Filter data to get Total Population (Element.Code) & Rename Country.Code to Area.Code
s_pop_1 = paste(s_temp,"Population_E_All_Data_(Normalized).csv",sep="/")
s_pop_2 = paste(s_temp,"00_population.csv",sep="/")
datatable  <- read.csv(s_pop_1)
datatable2 <- subset(datatable, select=c("Area.Code", "Item.Code", "Element.Code", "Year.Code", "Value"))
datatable2 <- subset(datatable2,  Item.Code==3010) 
datatable2 <- subset(datatable2,  Element.Code==511) 
datatable2 <- datatable2[datatable2$ Area.Code %in% country,] 
datatable2 <- datatable2[datatable2$Year %in% years,]
datatable2 <- subset(datatable2, select=c("Area.Code", "Year.Code", "Value"))
datatable2 <- datatable2[complete.cases(datatable2),]
colnames(datatable2) <- c("Area.Code", "Year.Code", "Value")          
write.csv(datatable2,s_pop_2, row.names=FALSE )
names(datatable2) <- c("CNTRY","YEAR","POP")
write.csv(datatable2, s6, row.names=FALSE )


# Crop Prices (USD currency only)
s_cp_1 = paste(s_temp,"Prices_E_All_Data_(Normalized).csv",sep="/")
s_cp_2 = paste(s_temp,"00_cropprices.csv",sep="/")
datatable  <- read.csv(s_cp_1)
datatable2 <- subset(datatable, select=c("Area.Code", "Item.Code", "Element.Code", "Year.Code", "Value"))
datatable2 <- subset(datatable2,  Element.Code==5532) 
datatable2 <- datatable2[datatable2$ Area.Code %in% country,] 
datatable2 <- datatable2[datatable2$ Item.Code %in% crop,] 
datatable2 <- datatable2[datatable2$Year %in% years,]
datatable2 <- subset(datatable2, select=c("Area.Code", "Item.Code", "Year.Code", "Value")) 
datatable2 <- datatable2[complete.cases(datatable2),]
write.csv(datatable2, s_cp_2, row.names=FALSE )

# Livestock Prices (USD currency only)
datatable  <- read.csv(paste(s_temp,"/Prices_E_All_Data_(Normalized).csv",sep=""))
datatable2 <- subset(datatable, select=c("Area.Code", "Item.Code", "Element.Code", "Year.Code", "Value"))
datatable2 <- subset(datatable2,  Element.Code==5532) 
datatable2 <- datatable2[datatable2$ Area.Code %in% country,] 
datatable2 <- datatable2[datatable2$ Item.Code %in% livestock,] 
datatable2 <- datatable2[datatable2$Year %in% years,]
datatable2 <- datatable2[complete.cases(datatable2),]
datatable2 <- subset(datatable2, select=c("Area.Code", "Item.Code", "Year.Code", "Value"))
write.csv(datatable2, paste(s_temp,"00_liveprices.csv",sep="/"), row.names=FALSE )

# Crop Production 
datatable  <- read.csv(paste(s_temp,"Production_Crops_E_All_Data_(Normalized).csv",sep="/"))
datatable2 <- subset(datatable, select=c("Area.Code", "Item.Code", "Element.Code", "Year.Code", "Value"))
datatable2 <- subset(datatable2,  Element.Code==5510) 
datatable2 <- datatable2[datatable2$ Area.Code %in% country,] 
datatable2 <- datatable2[datatable2$ Item.Code %in% crop,] 
datatable2 <- datatable2[datatable2$Year %in% years,]
datatable2 <- datatable2[complete.cases(datatable2),]
datatable2 <- subset(datatable2, select=c("Area.Code", "Item.Code", "Year.Code", "Value"))
write.csv(datatable2, paste(s_temp,"/00_cropprod.csv",sep=""), row.names=FALSE )

# Crop Harvested Area 
datatable  <- read.csv(paste(s_temp,"/Production_Crops_E_All_Data_(Normalized).csv",sep=""))
datatable2 <- subset(datatable, select=c("Area.Code", "Item.Code", "Element.Code", "Year.Code", "Value"))
datatable2 <- subset(datatable2,  Element.Code==5312) 
datatable2 <- datatable2[datatable2$ Area.Code %in% country,] 
datatable2 <- datatable2[datatable2$ Item.Code %in% crop,] 
datatable2 <- datatable2[datatable2$Year %in% years,]
datatable2 <- datatable2[complete.cases(datatable2),]
datatable2 <- subset(datatable2, select=c("Area.Code", "Item.Code", "Year.Code", "Value"))
write.csv(datatable2, paste(s_temp,"/00_cropharea.csv",sep=""), row.names=FALSE )

# Livestock Production 
datatable  <- read.csv(paste(s_temp,"/Production_LivestockPrimary_E_All_Data_(Normalized).csv",sep=""))
datatable2 <- subset(datatable, select=c("Area.Code", "Item.Code", "Element.Code", "Year.Code", "Value"))
datatable2 <- subset(datatable2,  Element.Code==5510) 
datatable2 <- datatable2[datatable2$ Area.Code %in% country,] 
datatable2 <- datatable2[datatable2$ Item.Code %in% livestock,] 
datatable2 <- datatable2[datatable2$Year %in% years,]
datatable2 <- datatable2[complete.cases(datatable2),]
datatable2 <- subset(datatable2, select=c("Area.Code", "Item.Code", "Year.Code", "Value"))
write.csv(datatable2, paste(s_temp,"/00_liveprod.csv",sep=""), row.names=FALSE )

# =========================================== #
# Corn-equivalent weights for crop production #
# =========================================== #
# ----- Read in crop production and prices
cropprice  <- read.csv(paste(s_temp,"/00_cropprices.csv",sep=""))
cropprod   <- read.csv(paste(s_temp,"/00_cropprod.csv",sep=""))
# ----- Merge production and price data then calculate value  
cropvalue <- merge(cropprice, cropprod, by=c("Area.Code","Item.Code","Year.Code"), all = FALSE)
names(cropvalue) <- c("Area.Code","Item.Code","Year.Code","Price","Prod")
cropvalue$Value <- cropvalue$Price*cropvalue$Prod
cropvalue <- subset(cropvalue, select=c("Area.Code","Item.Code","Year.Code","Prod","Value"))
# ----- Remove obs with zero value and prod values then aggregate prod and value by item and year
cropvalue[cropvalue == 0] <- NA
cropvalue <- cropvalue[complete.cases(cropvalue),]
write.csv(cropvalue,paste(s_temp,"/00_cropvalue.csv",sep=""), row.names=FALSE )
wldcrop <- aggregate(cropvalue, list(cropvalue$Item.Code, cropvalue$Year.Code), sum)
wldcrop <- subset(wldcrop, select=c( "Group.1","Group.2","Prod","Value"))
names(wldcrop) <- c("Item.Code","Year.Code","Prod","Value")
# ----- Calculate global prices, corn equivalent price for each crop and write data
wldcrop$PriceW <- wldcrop$Value / wldcrop$Prod
write.csv(subset(wldcrop, select=c("Item.Code","Year.Code","PriceW")), s7, row.names=FALSE ) 
wldcornprice <- subset(wldcrop, Item.Code==56, select=c("Year.Code","PriceW")) 
write.csv(wldcornprice, s8, row.names=FALSE )
names(wldcornprice) <- c("Year.Code","CornPriceW")
wldcrop <- merge(wldcrop, wldcornprice, by=c("Year.Code"))
wldcrop$CornEqPriceW <- wldcrop$PriceW / wldcrop$CornPriceW
WldCornEqPrice <- subset(wldcrop, select=c("Year.Code","Item.Code","CornEqPriceW"))
write.csv(WldCornEqPrice, s9, row.names=FALSE )

# ----- Recalculate new value data and corn equivalent data
wldcorneqprice  <- read.csv(s9)
wldcropprices   <- read.csv(s7)
cropprod        <- read.csv(paste(s_temp,"/00_cropprod.csv",sep=""))
wldcornprice    <- read.csv(s8)

fincropprod  <- merge(cropprod, wldcorneqprice, by=c("Item.Code","Year.Code"), all = FALSE)
fincropprod$QCROP <- fincropprod$Value * fincropprod$CornEqPriceW
fincropprod  <- subset(fincropprod, select=c("Item.Code","Year.Code","Area.Code","QCROP"))
fincropprod  <- merge(fincropprod, wldcornprice, by=c("Year.Code"), all = FALSE)
fincropprod$VCROP <- fincropprod$QCROP * fincropprod$PriceW
fincropprod  <- subset(fincropprod, select=c("Area.Code","Year.Code","QCROP","VCROP"))
fincropprod  <- aggregate(fincropprod, list(fincropprod$Area.Code, fincropprod$Year.Code), sum)
names(fincropprod) <- c("CNTRY","YEAR"," Area.Code"," Year.Code","QCROP","VCROP")
write.csv(subset(fincropprod, select= c("CNTRY","YEAR","QCROP")), s10, row.names=FALSE)
write.csv(subset(fincropprod, select= c("CNTRY","YEAR","VCROP")), s11, row.names=FALSE)

# =================================================== #
# Chicked-equivalent weights for livestock production #
# =================================================== #
# ----- Skip this one for now ------#


# ===================================================== #
# Merge all data together and create har file from data #
# ===================================================== #
REG    <- read.csv(s12)
INC    <- read.csv(s5)
POP    <- read.csv(s6)
QCROP  <- read.csv(s10)
VCROP  <- read.csv(s11)
QLAND  <- read.csv(s4)

data_names <- c("INC","POP","QCROP","VCROP","QLAND")

#for(i in years){
#dir.create(paste(s_fin,i, sep=""), recursive = TRUE)} 

for(i in data_names){
for(j in years){
data_table     <- merge(get(i), REG, by="CNTRY", all=FALSE)
data_table     <- data_table[data_table$YEAR == j,]
data_table2    <- aggregate(data_table[,3], list(data_table$REG), sum)
names(data_table2) <- c("REG",i)
write.csv(data_table2, paste(s_dir,"/",j,"_",i,".csv", sep=""), row.names=FALSE)}}

#for(j in years){
#for(i in data_names){
#              Create inp file for csv2har implementation
#sink(paste(getwd(),"/temp/",i,".inp", sep=""))
#cat(paste(getwd(),"/out/",j,"/",i,".csv", sep="")," ! name of input file")
#cat("\n")                                                
#cat(paste(getwd(),"/out/",j,"/",i,".har", sep="")," ! name of output file")
#cat("\n")
#cat("2              ! column no for values")           
#cat("\n")
#cat("1              ! number of index sets (columns)")
#cat("\n")
#cat("1  REG       ! column and name of index set 1")
#cat("\n")
#sink()
#}

#              Create batch file for running csv2har
#sink(paste(getwd(),"\\call_csv2har.bat", sep=""))
#cat("call c:\\GP\\csv2har.exe temp\\INC.inp")
#cat("\n") 
#cat("call c:\\GP\\csv2har.exe temp\\POP.inp")
#cat("\n") 
#cat("call c:\\GP\\csv2har.exe temp\\QCROP.inp")
#cat("\n") 
#cat("call c:\\GP\\csv2har.exe temp\\VCROP.inp")
#cat("\n") 
#cat("call c:\\GP\\csv2har.exe temp\\QLAND.inp")
#cat("\n")                                
#sink()

#shell(paste("call_csv2har.bat", sep=""), wait =TRUE)  # run csv2har
#}
          
# ======== #
# Clean-up #
# ======== #          

# Delete non year files
unlink(s4)
unlink(s5)
unlink(s6)
unlink(s7)
unlink(s8)
unlink(s9)
unlink(s10)
unlink(s11)

#          Delete 'temp' folder
unlink(s_temp, recursive = TRUE)    
#dir.create("./temp", recursive = TRUE) # create 'temp' folder
          
          
