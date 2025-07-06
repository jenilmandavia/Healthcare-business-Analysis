--HealthCare Project
--Upload Files to wrk table

DROP TABLE IF EXISTS healthcare_workforce_wrk;
DROP TABLE IF EXISTS healthcare_expenditure_wrk;
DROP TABLE IF EXISTS health_data1_wrk;

--Raw Health survey Data
CREATE TABLE health_data1_wrk (
    RowId VARCHAR(50),
    YearStart INT,
    YearEnd INT,
    LocationAbbr VARCHAR(10),
    LocationDesc VARCHAR(100),
    Datasource VARCHAR(50),
    Class VARCHAR(50),
    Topic VARCHAR(255),
    Question VARCHAR(255),
    Data_Value_Unit VARCHAR(50),
    DataValueTypeID VARCHAR(50),
    Data_Value_Type VARCHAR(50),
    Data_Value FLOAT,
    Data_Value_Alt FLOAT,
    Data_Value_Footnote_Symbol VARCHAR(10),
    Data_Value_Footnote TEXT,
    Low_Confidence_Limit FLOAT,
    High_Confidence_Limit FLOAT,
    StratificationCategory1 VARCHAR(100),
    Stratification1 VARCHAR(100),
    StratificationCategory2 VARCHAR(100),
    Stratification2 VARCHAR(100),
    Geolocation VARCHAR(100), 
    ClassID VARCHAR(50),
    TopicID VARCHAR(50),
    QuestionID VARCHAR(50),
    LocationID INT,
    StratificationCategoryID1 VARCHAR(50),
    StratificationID1 VARCHAR(50),
    StratificationCategoryID2 VARCHAR(50),
    StratificationID2 VARCHAR(50)
);

UPDATE health_data1_wrk
SET 
    StratificationCategory1 = COALESCE(StratificationCategory1, 'NA'),
    Stratification1 = COALESCE(Stratification1, 'NA'),
    StratificationCategory2 = COALESCE(StratificationCategory2, 'NA'),
    Stratification2 = COALESCE(Stratification2, 'NA')
WHERE 
    StratificationCategory1 IS NULL
    OR Stratification1 IS NULL
    OR StratificationCategory2 IS NULL
    OR Stratification2 IS NULL;

UPDATE health_data1_wrk
SET 
    Data_Value = COALESCE(Data_Value, 0),
    Data_Value_Alt = COALESCE(Data_Value_Alt, 0),
    Low_Confidence_Limit = COALESCE(Low_Confidence_Limit, 0),
    High_Confidence_Limit = COALESCE(High_Confidence_Limit, 0)
WHERE 
    Data_Value IS NULL
    OR Data_Value_Alt IS NULL
    OR Low_Confidence_Limit IS NULL
    OR High_Confidence_Limit IS NULL;


--ETL for Locations Table
DROP TABLE IF EXISTS Locations;
CREATE TABLE Locations (
    LocationID INT PRIMARY KEY,
    LocationAbbr VARCHAR(10),
    LocationDesc VARCHAR(255),
	Geolocation VARCHAR(100),
    Latitude NUMERIC,
    Longitude NUMERIC
);

-- Insert unique location data from health_data1_wrk to Locations table
INSERT INTO Locations (LocationID, LocationAbbr, LocationDesc, Geolocation)
SELECT 
    LocationID,
    LocationAbbr,
    LocationDesc,
	Geolocation
 FROM 
    (SELECT DISTINCT LocationID,LocationAbbr, LocationDesc, Geolocation FROM health_data1_wrk) AS unique_locations;

UPDATE Locations
SET Geolocation = SUBSTRING(Geolocation FROM 8 FOR LENGTH(Geolocation) - 8 - 1);

-- Update Latitude and Longitude based on the Geolocation field
UPDATE Locations
SET 
    Latitude = CAST(SPLIT_PART(Geolocation, ' ', 2) AS NUMERIC),
    Longitude = CAST(SPLIT_PART(Geolocation, ' ', 1) AS NUMERIC)
WHERE 
    Geolocation IS NOT NULL;

UPDATE Locations
SET Geolocation = 'NA'
WHERE Geolocation IS NULL;

UPDATE Locations
SET Latitude = -999,
	Longitude = -999
WHERE Latitude IS NULL or Longitude is NULL;

--ETL for Classifications table
DROP TABLE IF EXISTS Classifications;
CREATE TABLE Classifications (
    ClassID VARCHAR(50) PRIMARY KEY,
    ClassificationName VARCHAR(255) UNIQUE
);

INSERT INTO Classifications (ClassID, ClassificationName)
SELECT DISTINCT ClassID, Class
FROM health_data1_wrk
WHERE Class IS NOT NULL;


--ETL for Topics table
DROP TABLE IF EXISTS Topics;
CREATE TABLE Topics (
    TopicID VARCHAR(50) PRIMARY KEY,
    ClassID VARCHAR(50) REFERENCES Classifications(ClassID),
    TopicName VARCHAR(255) UNIQUE
);

INSERT INTO Topics (TopicID, ClassID, TopicName)
SELECT DISTINCT 
    TopicID, 
    ClassID,
    Topic
FROM health_data1_wrk
WHERE Topic IS NOT NULL;

--ETL for Questions table
DROP TABLE IF EXISTS Questions;
CREATE TABLE Questions (
    QuestionID VARCHAR(50) PRIMARY KEY,
    TopicID VARCHAR(50) REFERENCES Topics(TopicID),
    QuestionText VARCHAR(255) UNIQUE
);

INSERT INTO Questions (QuestionID, TopicID, QuestionText)
SELECT DISTINCT 
    QuestionID,
    TopicID,
    Question
FROM health_data1_wrk
WHERE Question IS NOT NULL;

--ETL for DataValueTypes table
DROP TABLE IF EXISTS DataValueTypes;
CREATE TABLE DataValueTypes (
	DataValueTypeID VARCHAR(255) PRIMARY KEY,
    DataValueType VARCHAR(255)
);

INSERT INTO DataValueTypes (DataValueTypeID, DataValueType)
SELECT DISTINCT DataValueTypeID, Data_Value_Type
FROM health_data1_wrk
WHERE DataValueTypeID IS NOT NULL AND Data_Value_Type IS NOT NULL;

--ETL for healthcare workforce table
CREATE TABLE healthcare_workforce_wrk (
    fips_st VARCHAR(2) PRIMARY KEY,
    st_abbrev VARCHAR(2),
    phys_wkforc_21 INT,
    phys_mal_21 INT,
    phys_fem_21 INT,
    phys_wh_21 INT,
    phys_ofcs_phys_21 INT,
    phys_hosp_21 INT,
    rn_21 INT,
    rn_fem_21 INT,
    rn_30_39_21 INT,
    rn_40_49_21 INT,
    rn_50_59_21 INT,
    rn_ge60_21 INT,
    rn_wh_21 INT,
    rn_hosp_21 INT,
    rn_emplymt_22 INT,
    rn_medn_wage_22 FLOAT,
    rn_degrs_21 INT,
    rn_specfd_degrs_21 INT,
    rn_bachlrs_21 INT,
    rn_mastrs_21 INT,
    rn_degrs_mal_21 INT,
    rn_degrs_fem_21 INT,
    rn_degrs_nhsp_wh_21 INT,
    rn_degrs_nhsp_bl_21 INT,
    rn_degrs_hsp_21 INT,
    rn_degrs_asn_21 INT,
    rn_degrs_nhpi_21 INT,
    rn_degrs_aian_21 INT,
    rn_degrs_nraln_21 INT,
    rn_degrs_unkrace_21 INT,
    rn_degrs_2race_21 INT,
    pharm_emplymt_22 INT,
    pharm_medn_wage_22 FLOAT,
    pharm_techn_emplymt_22 INT,
    pharm_techn_medn_wage_22 FLOAT,
    popn_pums_21 INT,
    popn_mal_21 INT,
    popn_fem_21 INT,
    popn_lt30_21 INT,
    popn_30_39_21 INT,
    popn_40_49_21 INT,
    popn_50_59_21 INT,
    popn_ge60_21 INT,
    popn_wh_21 INT,
    popn_bl_21 INT,
    popn_hsp_21 INT,
    popn_asn_21 INT,
    popn_aian_21 INT,
    popn_2race_21 INT,
    popn_pums_ge16_21 INT,
    popn_mal_ge16_21 INT,
    popn_fem_ge16_21 INT,
    popn_lt30_ge16_21 INT,
    popn_30_39_ge16_21 INT,
    popn_40_49_ge16_21 INT,
    popn_50_59_ge16_21 INT,
    popn_ge60_ge16_21 INT,
    popn_wh_ge16_21 INT,
    popn_bl_ge16_21 INT,
    popn_hsp_ge16_21 INT,
    popn_asn_ge16_21 INT,
    popn_aian_ge16_21 INT,
    popn_2race_ge16_21 INT,
    popn_22 INT,
    popn_ge16_22 INT
);

CREATE TABLE healthcare_workforce (
    fips_st VARCHAR(2) PRIMARY KEY,    
    st_abbrev VARCHAR(2),               
    LocationID INT REFERENCES Locations(LocationID), 
    phys_wkforc_21 INT,                 
    phys_ofcs_phys_21 INT,              
    phys_hosp_21 INT,                  
    rn_21 INT,                          
    rn_hosp_21 INT,                    
    rn_emplymt_22 INT,                  
    rn_medn_wage_22 FLOAT,              
    pharm_emplymt_22 INT,               
    pharm_medn_wage_22 FLOAT,           
    pharm_techn_emplymt_22 INT,         
    pharm_techn_medn_wage_22 FLOAT,     
    popn_pums_21 INT,                   
    popn_22 INT,                        
    popn_ge16_22 INT                   
);

INSERT INTO healthcare_workforce (
    fips_st,
    st_abbrev,
    LocationID,  
    phys_wkforc_21,
    phys_ofcs_phys_21,
    phys_hosp_21,
    rn_21,
    rn_hosp_21,
    rn_emplymt_22,
    rn_medn_wage_22,
    pharm_emplymt_22,
    pharm_medn_wage_22,
    pharm_techn_emplymt_22,
    pharm_techn_medn_wage_22,
    popn_pums_21,
    popn_22,
    popn_ge16_22
)
SELECT 
    h.fips_st,
    h.st_abbrev,
    l.LocationID, 
    h.phys_wkforc_21,
    h.phys_ofcs_phys_21,
    h.phys_hosp_21,
    h.rn_21,
    h.rn_hosp_21,
    h.rn_emplymt_22,
    h.rn_medn_wage_22,
    h.pharm_emplymt_22,
    h.pharm_medn_wage_22,
    h.pharm_techn_emplymt_22,
    h.pharm_techn_medn_wage_22,
    h.popn_pums_21,
    h.popn_22,
    h.popn_ge16_22
FROM healthcare_workforce_wrk h
JOIN Locations l ON h.st_abbrev = l.locationabbr;

--ETL for Expenditure Fact Table
DROP TABLE IF EXISTS healthcare_expenditure_wrk;
CREATE TABLE healthcare_expenditure_wrk (
    year INT,
    region VARCHAR(100),
    division VARCHAR(100),
    state VARCHAR(100),
    population FLOAT,
    group_name VARCHAR(100),
    subgroup VARCHAR(100),
    metric VARCHAR(100),
    val FLOAT,
    upper FLOAT,
    lower FLOAT
);

--Cleaning Table expenditure 
Delete from healthcare_expenditure_wrk
WHERE metric= 'Spending per capita';

DROP TABLE IF EXISTS healthcare_expenditure;
CREATE TABLE healthcare_expenditure (
    year INT,
    region VARCHAR(100),
    division VARCHAR(100),
	LocationID INT REFERENCES Locations(LocationID),
    state VARCHAR(100),
    population FLOAT,
    group_name VARCHAR(100),
    subgroup VARCHAR(100),
    metric VARCHAR(100),
    val FLOAT,
    upper FLOAT,
    lower FLOAT
);

--Inserting location ids
INSERT INTO healthcare_expenditure (
    year,
    region,
    division,
    LocationID, 
    state,
    population,
    group_name,
    subgroup,
    metric,
    val,
    upper,
    lower
)
SELECT 
    h.year,
    h.region,
    h.division,
    l.LocationID, 
    h.state,
    h.population,
    h.group_name,
    h.subgroup,
    h.metric,
    h.val,
    h.upper,
    h.lower
FROM healthcare_expenditure_wrk h
JOIN Locations l ON h.state = l.locationdesc;

-- Creating the HealthSurveyFact table
DROP TABLE IF EXISTS HealthSurveyFact;
CREATE TABLE HealthSurveyFact (
    FactID SERIAL PRIMARY KEY, 
    YearStart INT,              
    YearEnd INT,                
    
    -- Foreign keys to link the fact data to dimensions
    LocationID INT REFERENCES Locations(LocationID),
    ClassID VARCHAR(50) REFERENCES Classifications(ClassID),
    TopicID VARCHAR(50) REFERENCES Topics(TopicID),
    QuestionID VARCHAR(50) REFERENCES Questions(QuestionID),
    DataValueTypeID VARCHAR(255) REFERENCES DataValueTypes(DataValueTypeID),
    
    -- Measure columns to store the actual survey data
    Data_Value FLOAT,
    Data_Value_Alt FLOAT,
    Low_Confidence_Limit FLOAT,
    High_Confidence_Limit FLOAT,
    
    -- Demographic information (stratifications)
    StratificationCategory1 VARCHAR(100),  
    Stratification1 VARCHAR(100),          
    StratificationCategory2 VARCHAR(100),  
    Stratification2 VARCHAR(100),          
    StratificationCategoryID1 VARCHAR(50),
    StratificationID1 VARCHAR(50),
    StratificationCategoryID2 VARCHAR(50),
    StratificationID2 VARCHAR(50)
);

INSERT INTO HealthSurveyFact (
    YearStart, 
    YearEnd, 
    LocationID, 
    ClassID, 
    TopicID, 
    QuestionID, 
    DataValueTypeID, 
    Data_Value, 
    Data_Value_Alt, 
    Low_Confidence_Limit, 
    High_Confidence_Limit,
    StratificationCategory1, 
    Stratification1, 
    StratificationCategory2, 
    Stratification2, 
    StratificationCategoryID1, 
    StratificationID1, 
    StratificationCategoryID2, 
    StratificationID2
)
SELECT 
    wrk.YearStart, 
    wrk.YearEnd, 
    wrk.LocationID, 
    wrk.ClassID, 
    wrk.TopicID, 
    wrk.QuestionID, 
    wrk.DataValueTypeID, 
    wrk.Data_Value, 
    wrk.Data_Value_Alt, 
    wrk.Low_Confidence_Limit, 
    wrk.High_Confidence_Limit, 
    wrk.StratificationCategory1, 
    wrk.Stratification1, 
    wrk.StratificationCategory2, 
    wrk.Stratification2, 
    wrk.StratificationCategoryID1, 
    wrk.StratificationID1, 
    wrk.StratificationCategoryID2, 
    wrk.StratificationID2
FROM health_data1_wrk wrk;

--Dropping work tables as not required
DROP TABLE IF EXISTS healthcare_workforce_wrk;
DROP TABLE IF EXISTS healthcare_expenditure_wrk;
DROP TABLE IF EXISTS health_data1_wrk;
