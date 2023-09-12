--Loading records from hadoop fs
NT_Records = LOAD 'lab4/network_trace' USING PigStorage(' ') AS (Time:chararray, IP:chararray, Source_IP:chararray,symbol:chararray, Destination_IP:chararray, Protocol:chararray, type:chararray, data:int);

--filtered the records that has tcp protocols
Filtered_Records = FILTER NT_Records BY Protocol == 'tcp';

--formating the IP in the way to remove extra information (noise)
Formatted_Records = FOREACH Filtered_Records GENERATE SUBSTRING(Source_IP,0,LAST_INDEX_OF(Source_IP,'.')) AS Source_IP,SUBSTRING(Destination_IP,0,LAST_INDEX_OF(Destination_IP,'.')) AS Destination_IP ;

--Grouping By Source IP
group_sourceIP = GROUP Formatted_Records BY Source_IP;

--For each Source_IP in group source id , getting the distinct Destination_IP and counting it
Distinct_IP =FOREACH group_sourceIP 
{ 
   PA= Formatted_Records.Destination_IP; 
   DA = DISTINCT PA; 
   GENERATE group,
   COUNT (DA);
} 

-- descending the Output 
Sorted_ip = Order Distinct_IP by $1 DESC; 

--Limiting the order_ip for first 10 output
Results = LIMIT Sorted_ip 10 ; 

--Output File Path
STORE Results INTO '/lab4/exp2/output/';



