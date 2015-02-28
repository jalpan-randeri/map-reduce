--register file:/home/hadoop/lib/pig/piggybank.jar;
register /Users/jalpanranderi/pig-0.14.0/lib/piggybank.jar;
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

set default_parallel 10;

--file1 = LOAD '/Users/jalpanranderi/Downloads/test.csv' USING CSVLoader() ;
--file2 = LOAD '/Users/jalpanranderi/Downloads/test.csv' USING CSVLoader() ;

--file1 = LOAD '$INPUT' USING CSVLoader() ;
--file2 = LOAD '$INPUT' USING CSVLoader() ;

file1 = LOAD '/Users/jalpanranderi/Downloads/data.csv' USING CSVLoader();
file2 = LOAD '/Users/jalpanranderi/Downloads/data.csv' USING CSVLoader();

--$5 as date,
--$11 as origin,
--$17 as dest,
--$24 as d_time,
--$35 as delay,
--$37 as a_time,
--$41 as cancelled,
--$43 as diverted;

-- remove unwanted flights e.g. diverted, cancelled, dest = jfk and org = ord,
-- date in range 2007-12 to 2008-1
ip1 = filter file1 by ($11 == 'ORD')
                       and ($17 != 'JFK')
                       and ($41 != 1)
                       and ($43 != 1) ;



ip2 = filter file2 by ($11 != 'ORD')
                       and ($17 == 'JFK')
                       and ($41 != 1)
                       and ($43 != 1) ;


-- apply projection for only required data
origin_table = foreach ip1 generate $5 as flight_date,
                                    $17 as dest,
                                    (int)$35 as o_time,
                                    $37 as o_delay;

dest_table = foreach ip2 generate $5 as flight_date,
                                  $11 as origin,
                                  (int)$24 as d_time,
                                  $37 as d_delay;


-- join on two tables
inner_join = join origin_table by (flight_date, dest),
                  dest_table   by (flight_date, origin)  ;


-- clear the invalid entries where the depart time > arrival time
valid = filter inner_join by d_time > o_time ;


v = filter valid by ToDate(origin_table::flight_date,'yyyy-MM-dd') < ToDate('2008-06-01','yyyy-MM-dd')
                    AND ToDate(origin_table::flight_date,'yyyy-MM-dd') > ToDate('2007-05-31','yyyy-MM-dd')
                    AND ToDate(dest_table::flight_date,'yyyy-MM-dd') < ToDate('2008-06-01','yyyy-MM-dd')
                    AND ToDate(dest_table::flight_date,'yyyy-MM-dd') > ToDate('2007-05-31','yyyy-MM-dd');




-- calculate the sum of all delays
d = foreach v generate  o_delay + d_delay as sum;

-- calculate the average
ans = foreach (group d all) generate AVG(d.sum);

--store the answer
dump ans;
--store ans into '$OUTPUT/output';