## Urban Mind pilot data analysis, using the cleaned data file for participants who completed
# more than 50% of assessments: joinedDF50numeric.

# Upload data file into R and set as a dataframe
setwd("/Users/lucieburgess/Documents/KCL/Urban_Mind_Analytics/Pilot_data/")
wd <- getwd()

#Upload file and set column names
df <- read.csv("joinedDFnumeric50.csv", header = TRUE, sep = ",", dec = ".")
dim(df) # 2040 observations of 34 variables
names(df)

# Table S1 - Correlation coefficients between covariates in statistical models

cor(df$baseWellBeingScore, df$X001_Age, method = "pearson") # -0.04

cor(df$baseWellBeingScore, df$X002_Gender_numeric) # 0.1193 - Pearson

install.packages("DescTools") # Installs GKgamma{DescTools}
library(DescTools) 
?? GoodmanKruskalGamma

install.packages("vcdExtra") # Installs GKgamma{vcdExtra}
library(vcdExtra)
?? GKgamma

gkdatax <- subset(df, select=c("baseWellBeingScore","X002_Gender", "X003_Where.did.you.grow.up","X005_What.is.your.level.of.education", "X006_Occupation", "X104_Are.you.indoors.or.outdoors",                            
                  "X201_Can.you.see.trees","X202_Can.you.see.the.sky","X203_Can.you.hear.birds.singing", "X204_Can.you.see.or.hear.water","X205_Do.you.feel.in.contact.with.nature"))
dim(gkdatax)

gkdatay <- subset(df, select=c("baseWellBeingScore","X002_Gender", "X003_Where.did.you.grow.up","X005_What.is.your.level.of.education", "X006_Occupation"))

gkdataz <- subset(df, select=c("X002_Gender","X006_Occupation"))
dim(gkdataz)
GKgamma(gkdataz, level = 0.95)

res <- GoodmanKruskalGamma(gkdatay, gkdatax, conf.level = NA)
res
res

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

res7 <- GoodmanKruskalGamma(df$X203_Can.you.hear.birds.singing, df$X006_Occupation, conf.level = NA)
res7

?GoodmanKruskalGamma



GKgamma(df)

?GKgamma

newdata <- c(df$momWellBeingScore, df$X104_Are.you.indoors.or.outdoors, df$X201_Can.you.see.trees)

GKgamma(newdata)

## Now calculate a linear regression using various predictors.

lm.fit = lm(df$momWellBeingScore ~ df$baseWellBeingScore + df$X001_Age + df$X002_Gender_numeric +df$X003_Where.did.you.grow.up_numeric
            + df$X005_What.is.your.level.of.education_numeric + df$X006_Occupation_numeric + df$X007_How.would.you.rate.your.physical.health.overall_numeric + df$X008_How.would.you.rate.your.mental.health.overall_numeric
            + df$X104_Are.you.indoors.or.outdoors + df$X201_Can.you.see.trees_numeric + df$X202_Can.you.see.the.sky_numeric + df$X203_Can.you.hear.birds.singing_numeric +df$X204_Can.you.see.or.hear.water_numeric+ df$X205_Do.you.feel.in.contact.with.nature_numeric)

summary(lm.fit)

## Table S4. Associations between momentary mental wellbeing and the interaction between trait impulsivity score and self-reported environmental features
# adjusted for age, gender, occupation and mental wellbeing over the previous two weeks for > 50% completed assessments

predictors = c(df$X104_Are.you.indoors.or.outdoors, df$X201_Can.you.see.trees_numeric, df$X202_Can.you.see.the.sky_numeric, df$X203_Can.you.hear.birds.singing_numeric, df$X204_Can.you.see.or.hear.water_numeric, df$X205_Do.you.feel.in.contact.with.nature_numeric)
n = length(predictors)
n

# Syntax error in this loop - doesn't like the reference to df$predictors[i]
lm.fit <- list()
for (i in 1:n) {
  lm.fit[i] <- lm(df$momWellBeingScore ~ df$baseWellBeingScore + df$X001_Age + df$X002_Gender_numeric + df$X006_Occupation_numeric + df$baseImpulseScore:predictors[i])
}

## Last term is the interaction term between trait impulsivity and the environmental feature of interest

lm0.fit = lm(df$momWellBeingScore ~ df$baseWellBeingScore + df$X001_Age + df$X002_Gender_numeric + df$X006_Occupation_numeric +df$baseImpulseScore:df$X104_Are.you.indoors.or.outdoors)
summary(lm0.fit)

lm1.fit = lm(df$momWellBeingScore ~ df$baseWellBeingScore + df$X001_Age + df$X002_Gender_numeric + df$X006_Occupation_numeric +df$baseImpulseScore:df$X201_Can.you.see.trees_numeric)
summary(lm1.fit)

lm2.fit = lm(df$momWellBeingScore ~ df$baseWellBeingScore + df$X001_Age + df$X002_Gender_numeric + df$X006_Occupation_numeric +df$baseImpulseScore:df$X202_Can.you.see.the.sky_numeric)
summary(lm2.fit)

lm3.fit = lm(df$momWellBeingScore ~ df$baseWellBeingScore + df$X001_Age + df$X002_Gender_numeric + df$X006_Occupation_numeric +df$baseImpulseScore:df$X203_Can.you.hear.birds.singing_numeric)
summary(lm3.fit)

lm4.fit = lm(df$momWellBeingScore ~ df$baseWellBeingScore + df$X001_Age + df$X002_Gender_numeric + df$X006_Occupation_numeric +df$baseImpulseScore:df$X204_Can.you.see.or.hear.water_numeric)
summary(lm4.fit)

lm5.fit = lm(df$momWellBeingScore ~ df$baseWellBeingScore + df$X001_Age + df$X002_Gender_numeric + df$X006_Occupation_numeric +df$baseImpulseScore:df$X205_Do.you.feel.in.contact.with.nature_numeric)
summary(lm5.fit)

## Descriptive statistics for this dataset, which refers to participants who completed > 50 assessments

mean(df$X001_Age) #30.9
sd(df$X001_Age) # 11.9

mean(df$baseWellBeingScore) #48.77
sd(df$baseWellBeingScore) #8.97

mean(df$momWellBeingScore) #53.24
sd(df$momWellBeingScore) #11.83

mean(df$baseImpulseScore) #18.79 - this seems to be different
sd(df$baseImpulseScore) #3.7  


install.packages("sqldf")
library(sqldf)

# Calculate mean number of assessments

sqldf("SELECT momParticipantUUID, momAssessmentNumber, GROUP BY momParticipantUUID, COUNT(distinct(momAssessmentNumber)) as totalAssessments FROM df")

# Calculate occupational status
sqldf("SELECT momParticipantUUID, momAssessmentNumber, 006_Occupation, COUNT(distinct(006_Occupation)) as Occupations FROM df GROUP BY momParticipantUUID")





