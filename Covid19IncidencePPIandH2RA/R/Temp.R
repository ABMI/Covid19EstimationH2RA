##Create population
# 972 ppi
# 983 h2ra
# 981 non ppi non h2ra
# 1260 covid diagnosis
# 621 occurrence of intensive care

excludeIngredientConceptIds <- c(961047,911735,948078,923645,950696,929887,953076,904453,997276,43009052,19039926,43009003)
includeIngredientConceptIds <- c(316866, 134057, 201820, 255573, 4212540, 46271022)

# Optional: specify where the temporary files (used by the ff package) will be created:
options(fftempdir = "/home/cory66421/atlas/temp")

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

# The folder where the study intermediate and result files will be written:
outputFolder <- "/home/cory66421/atlas/output/Covid19IncidencePPIandH2RA"

# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sql server",
                                                                server = '128.1.99.58',
                                                                user = 'jimyung',
                                                                password = 'qwer1234!@')

# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- "CDMPv532.dbo"

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- "CDMPv1_Result.dbo"
cohortTable <- "JMPark_Covid19IncidencePPIandH2RA"

# Some meta-information that will be used by the export function:
databaseId <- "AUSOM"
databaseName <- "AUSOM"
databaseDescription <- "AUSOM"

# For Oracle: define a schema that can be used to emulate temp tables:
oracleTempSchema <- NULL

covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = T,
                                                                useDemographicsAge = T,
                                                                useDemographicsAgeGroup = T,
                                                                useDemographicsIndexYear = T,
                                                                useDemographicsIndexMonth = T,
                                                                useConditionOccurrenceLongTerm = T,
                                                                useConditionOccurrenceMediumTerm = T,
                                                                useConditionOccurrenceShortTerm = T,
                                                                useDrugExposureLongTerm = T,
                                                                useDrugExposureMediumTerm = T,
                                                                useDrugExposureShortTerm = T
                                                                # includedCovariateConceptIds = includeIngredientConceptIds,
                                                                # addDescendantsToInclude = T,
                                                                # excludedCovariateConceptIds = excludeIngredientConceptIds,
                                                                # addDescendantsToExclude = T
                                                                )


ParallelLogger::logInfo("*** Creating cohortMethodData objects ***")
cohortMethodData <- CohortMethod::getDbCohortMethodData(connectionDetails = connectionDetails,
                                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                                        oracleTempSchema = oracleTempSchema,
                                                        targetId = 972,
                                                        comparatorId = 983,
                                                        outcomeIds = 621,
                                                        studyStartDate = "",
                                                        studyEndDate = "",
                                                        exposureDatabaseSchema = cohortDatabaseSchema,
                                                        exposureTable = cohortTable,
                                                        outcomeDatabaseSchema = cohortDatabaseSchema,
                                                        outcomeTable = cohortTable,
                                                        cdmVersion = "5",
                                                        excludeDrugsFromCovariates = FALSE,
                                                        firstExposureOnly = TRUE,
                                                        removeDuplicateSubjects = "remove all",
                                                        restrictToCommonPeriod = FALSE,
                                                        washoutPeriod = 0,
                                                        maxCohortSize = 100000,
                                                        covariateSettings)

# outcome data
ParallelLogger::logInfo("*** Creating study populations ***")
population <- CohortMethod::createStudyPopulation(cohortMethodData,
                                                  population = NULL,
                                                  outcomeId = 621,
                                                  firstExposureOnly = TRUE,
                                                  restrictToCommonPeriod = FALSE,
                                                  washoutPeriod = 0,
                                                  removeDuplicateSubjects = "remove all",
                                                  removeSubjectsWithPriorOutcome = TRUE,
                                                  priorOutcomeLookback = 99999,
                                                  minDaysAtRisk = 1,
                                                  riskWindowStart = 1,
                                                  addExposureDaysToStart = NULL,
                                                  startAnchor = "cohort start",
                                                  riskWindowEnd = 365,
                                                  addExposureDaysToEnd = NULL,
                                                  endAnchor = "cohort end",
                                                  censorAtNewRiskWindow = FALSE)


# View(population)
connection <- DatabaseConnector::connect(connectionDetails)
########################
# covariates <- c('3024561','3035995','3013682','3024128','3006906','3014576','3016723','3013721','3006923','3027484','3023314','3007461','3011904','3023103','3020630','3019550','3037556','3010813','3020460')
# sql <- "select * from @cdmDatabaseSchema.measurement a, (select * from @cohortDatabaseSchema.@cohortTable where cohort_definition_id=1448) b
# where a.person_id=b.subject_id
# and (measurement_date >= cohort_start_date and measurement_date <= DATEADD(day, 3, cohort_end_date))
# and measurement_concept_id in (@covariates)"
# sql <- SqlRender::renderSql(sql=sql, cdmDatabaseSchema=cdmDatabaseSchema, cohortDatabaseSchema=cohortDatabaseSchema, cohortTable=cohortTable, covariates=covariates)$sql
# external_df <- DatabaseConnector::querySql(connection = connection, sql = sql)

covariateIds <- as.data.frame(cohortMethodData$covariates)
covariateNames <- as.data.frame(cohortMethodData$covariateRef)

View(covariateNames)

# library(dplyr)
# sql <- "select " # => extract descendant concepts
# sql
# includeIngredientConceptIds <- DatabaseConnector::querySql()
# covariateNames <-  covariateNames %>% filter(conceptId==includeIngredientConceptIds)

test <- left_join(covariateIds, covariateNames, by=c("covariateId"="covariateId"))


left_join(population, covariateIds, by=c("rowId"="rowId"))




colnames(external_df) <- tolower(colnames(external_df))
external_df <- external_df %>% select(person_id, measurement_date, measurement_concept_id, value_as_number)
population2 <- population

test2 <- left_join(population2, external_df, by=c("subjectId"="person_id", "cohortStartDate"="measurement_date"))
outcomeData <- unique(test2 %>% select(subjectId, outcomeCount))

## delete the patients who don't have measurement lab values?
## Seperate the person_id and measurement value columns then reshape the data frame. Then remerge the datasets.
external_df <- reshape2::dcast(external_df, person_id+measurement_date ~ measurement_concept_id, value.var = 'value_as_number', fun.aggregate = mean, na.rm=T)
external_df <- left_join(external_df, outcomeData, by = c("person_id"="subjectId"))

table(outcomeData$outcomeCount)
#       0      1
# 198914    369

outcomeData %>% filter(subjectId=="3180794")

# subset(external_df, (death_inhosp %in% NA)) %>% select(person_id)

#colnames(external_df) <- c("person_id", "Albumin", "Alkaline.phosphatase", "BUN", "Bilirubin..total", "Calcium", "Chloride", "Creatinine", "GOT..AST.", "GPT..ALT.", "Hb", "Hct", "PLT", "Phosphorus", "Potassium", "Protein..total", "Sodium", "Uric.Acid", "WBC", "hs.CRP.quantitation", "death_inhosp")
#colnames(external_df) <- c("person_id", "Calcium", "GPT..ALT.", "PLT", "hs.CRP.quantitation", "WBC", "Phosphorus", "BUN", "GOT..AST.", "Chloride", "Creatinine", "Sodium", "Protein..total", "Potassium", "Hct", "Bilirubin..total", "Albumin", "Hb", "Alkaline.phosphatase", "Uric.Acid", "death_inhosp")
colnames(external_df) <- c("person_id", "measurement_date", "Calcium", "GPT..ALT.", "PLT", "WBC", "Phosphorus", "BUN", "GOT..AST.", "Chloride", "Creatinine", "Sodium", "hs.CRP.quantitation", "Protein..total", "Potassium", "Hct", "Bilirubin..total", "Albumin", "Hb", "Alkaline.phosphatase", "Uric.Acid", "death_inhosp")

external_df_h2o <- external_df

external_df$countNA <- apply(external_df, 1, function(x) sum(is.na(x)))
external_df_h2o <- external_df[external_df[,"countNA"]<2,]

# person_id Calcium GPT..ALT. PLT   WBC Phosphorus  BUN GOT..AST. Chloride Creatinine Sodium hs.CRP.quantitation Protein..total Potassium   Hct Bilirubin..total Albumin
#   182809    8.95     111.5  87  5.15   2.666667  7.5       318      101       1.75 140.25               14.89           4.65     3.975 27.06              5.3     3.3
#   336394    9.20    1989.0  49  9.80   8.500000 18.3     26331       93       1.61 132.75               21.41           6.30     5.300 30.50              4.7     1.9
#  2947254    7.70    1188.0 187 12.40  10.100000 43.0      1908       92       1.90 131.00               14.11           4.40     7.700 23.90              0.5     2.8
