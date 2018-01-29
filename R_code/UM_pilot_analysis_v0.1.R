## Urban Mind pilot data analysis, using the cleaned data file for participants who completed
# more than 50% of assessments: joinedDF50numeric.

# Upload data file into R and set as a dataframe
setwd("/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/")
wd <- getwd()

#Upload file and set column names
df <- read.csv("joinedDFnumeric50.csv", header = TRUE, sep = ",", dec = ".")
dim(df) # 2040 observations of 33 variables

cor(df$baseWellBeingScore, df$X001_Age, method = "pearson") # -0.04
cor(df$baseWellBeingScore, df$X001_Age, method = "spearman") # 0.067
cor(df$baseWellBeingScore, df$X001_Age, method = "kendall") # 0.052

cor.test(df$baseWellBeingScore, df$X001_Age, method = "kendall")

cor(df$baseWellBeingScore, df$X002_Gender_numeric)

res1 <- cor(df) # x must be numeric. Can't compute correlation coefficient on categorical data
round(res, 4)

res2 <- GKtau(df$baseWellBeingScore, df$X002_Gender, dgts = 4, includeNA = "no")
res2
nrow(df)

install.packages("DescTools")
library(DescTools)

res3 <- GoodmanKruskalGamma(df$baseWellBeingScore, df$X002_Gender, conf.level = NA)
res3

res4 <- GoodmanKruskalGamma(df$baseWellBeingScore, df$X006_Occupation, conf.level = NA)
res4

res5 <- GoodmanKruskalGamma(df$baseWellBeingScore, df$X104_Are.you.indoors.or.outdoors, conf.level = NA)
res5

res6 <- GoodmanKruskalGamma(df$baseWellBeingScore, df$X201_Can.you.see.trees, conf.level = NA)
res6

res7 <- GoodmanKruskalGamma(df$baseWellBeingScore, df$X203_Can.you.hear.birds.singing, conf.level = NA)
res7

?GoodmanKruskalGamma

install.packages("vcdExtra")
library(vcdExtra)

GKgamma(df)

?GKgamma

newdata <- c(df$momWellBeingScore, df$X104_Are.you.indoors.or.outdoors, df$X201_Can.you.see.trees)

GKgamma(newdata)
