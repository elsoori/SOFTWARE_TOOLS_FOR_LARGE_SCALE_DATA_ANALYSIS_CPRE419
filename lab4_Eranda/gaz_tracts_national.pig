-- Loading the file
Inputdata = LOAD 'gaz_tracts_national.txt' AS (USPS:chararray, GEOID:chararray, POP10:double, HU10:int, ALAND:long,AWATER:double, ALAND_SQMI:double, AWATER_SQMI:float, INTPTLAT:float, INTPTLONG:float);

-- Extracting the necessary fields
Extracted = FOREACH Inputdata GENERATE USPS, ALAND;

-- Grouping by state
Grouped_State = Group Extracted by USPS;

-- Summing together all the state area
Summed = foreach Grouped_State generate group, SUM(Extracted.ALAND) AS sum;

-- Order records in descending order to get top 10
Sorted_Record = ORDER Summed BY sum DESC;

--Limiting the records to 10
Limited_Record = LIMIT Sorted_Record 10;

STORE Limited_Record INTO '/Exp1_Local/output';

