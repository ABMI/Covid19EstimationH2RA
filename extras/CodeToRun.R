library(PPIandH2RA)

# Optional: specify where the temporary files (used by the ff package) will be created:
options(fftempdir = "/home/cory66421/Atlas/study/PPIandH2RA/temp")

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

# The folder where the study intermediate and result files will be written:
outputFolder <- "/home/cory66421/Atlas/study/PPIandH2RA/output"

# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sql server",
                                                                server = '128.1.99.53',
                                                                user = 'jimyung',
                                                                password = 'qwer1234!@')

# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- "AUSOMv5_3.dbo"

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- "NHIS_NSC_Result.dbo"
cohortTable <- "JMPark_PPIandH2RA"

# Some meta-information that will be used by the export function:
databaseId <- "NHISNSC"
databaseName <- "NHISNSC"
databaseDescription <- "NHISNSC"

# For Oracle: define a schema that can be used to emulate temp tables:
oracleTempSchema <- NULL

execute(connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        oracleTempSchema = oracleTempSchema,
        outputFolder = outputFolder,
        databaseId = databaseId,
        databaseName = databaseName,
        databaseDescription = databaseDescription,
        createCohorts = T,
        synthesizePositiveControls = T,
        runAnalyses = T,
        runDiagnostics = T,
        packageResults = TRUE,
        maxCores = maxCores)

resultsZipFile <- file.path(outputFolder, "export", paste0("Results", databaseId, ".zip"))
dataFolder <- file.path(outputFolder, "shinyData")

prepareForEvidenceExplorer(resultsZipFile = resultsZipFile, dataFolder = dataFolder)

launchEvidenceExplorer(dataFolder = dataFolder, blind = F, launch.browser = FALSE)
