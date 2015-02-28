REGISTER  file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

SET DEFAULT_PARALLEL 10;

--Loading the input file using CSVLoader into Flights1
Flights1 = LOAD '$INPUT' USING CSVLoader() parallel 10;

--Loading the input file using CSVLoader into Flights2
Flights2 = LOAD '$INPUT' USING CSVLoader() parallel 10;

-- Filtering Flights1 to take only flights with origin ORD and removing flights which destination JFK and flights which are cancelled or diverted
Flights1Filtered = FILTER Flights1 BY ($11 == 'ORD') AND ($17 != 'JFK') AND ($41 != 1) AND ($43 != 1) parallel 10;

-- Filtering Flights2 to take only flights with destination JFK and removing flights which have origin ORD and flights which are cancelled or diverted
Flights2Filtered = FILTER Flights2 BY ($11 != 'ORD') AND ($17 == 'JFK') AND ($41 != 1) AND ($43 != 1) parallel 10;

--Projecting attributes from Flights1Filtered with alias: Attributes are year,month,flightDate, destinationF1, arrTimeF1, arrDelayMinF1
Flight1Attr = FOREACH Flights1Filtered GENERATE $0 as year, $2 as month, $5 as flightDate, $17 as destinationF1, (int)$35 as arrTimeF1, $37 as arrDelayMinF1;

--Projecting attributes from Flights2Filtered with alias: Attributes are year,month,flightDate, originF2,depTimeF2, arrDelayMinF2
Flight2Attr = FOREACH Flights2Filtered GENERATE $0 as year, $2 as month, $5 as flightDate, $11 as originF2, (int)$24 as depTimeF2, $37 as arrDelayMinF2;

--Joining Flight1Attr with Flight2Attr with join condition as: both have same flightDate and destination of Flight1 matches origin of Flight 2
Flights1and2Combined= JOIN Flight1Attr by (flightDate,destinationF1), Flight2Attr by (flightDate,originF2) parallel 10;

-- Filtering out Flights1and2Combined with condition that the departure	time in Flights2 is	not	after the arrival time in Flights1
FlightsFilter1 = FILTER Flights1and2Combined BY  depTimeF2 > arrTimeF1 parallel 10;

-- Filtering out FlightsFilter1 to remove those tuples which do not have flight date between June 2007 and May 2008 (checking date of both Flight1 and Flight2
FlightsFilter2 = FILTER FlightsFilter1 BY ((Flight1Attr::year >= 2007 AND Flight1Attr::year <= 2008)
		  AND ((Flight1Attr::year > 2007 AND Flight1Attr::year < 2008)
			OR (Flight1Attr::year == 2007 AND Flight1Attr::month >= 6)
			OR (Flight1Attr::year == 2008 AND Flight1Attr::month <= 5))) AND
 		((Flight2Attr::year >= 2007 AND Flight2Attr::year <= 2008)
		AND ((Flight2Attr::year > 2007 AND Flight2Attr::year < 2008)
			OR (Flight2Attr::year == 2007 AND Flight2Attr::month >= 6)
			OR (Flight2Attr::year == 2008 AND Flight2Attr::month <= 5))) parallel 10;

--Summing arrival delay of Flight 1 and Flight 2
SumDelay = FOREACH FlightsFilter2 GENERATE (float) arrDelayMinF1 + arrDelayMinF2 as sumofdelay;

--Grouping SumDelay
GroupDelay = GROUP SumDelay ALL parallel 10;

--Iterating over GroupDelay and finding average delay
avgDelay = FOREACH GroupDelay GENERATE AVG(SumDelay.sumofdelay);

--Storing average delay in output file
STORE avgDelay INTO '$OUTPUT/output';

