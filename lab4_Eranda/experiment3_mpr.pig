-- Loading the ip_trace and raw_block from the hadoop file system
ip_trace_datafile = LOAD 'lab4/ip_trace' USING PigStorage(' ') AS (Time:chararray, ConnID:chararray, SourceIP:chararray, remove:chararray, DestIP:chararray, Protocol:chararray, ProtDepData:chararray);
raw_block_datafile = LOAD 'lab4/raw_block' USING PigStorage(' ') AS (ConnectionID:chararray, Action:chararray);

IPTrace1 = FOREACH ip_trace_datafile GENERATE Time, ConnID, (ENDSWITH(SourceIP, ':') ? SUBSTRING(SourceIP, 0, LAST_INDEX_OF(SourceIP, ':')) : SourceIP) AS SourceIP, (ENDSWITH(DestIP, ':') ? SUBSTRING(DestIP, 0, LAST_INDEX_OF(DestIP, ':')) : DestIP) AS DestIP, Protocol;
IPTrace2 = FOREACH IPTrace1 GENERATE Time, ConnID, (Protocol == 'tcp' ? SUBSTRING(SourceIP, 0, LAST_INDEX_OF(SourceIP, '.')) : SourceIP) AS SourceIP, (Protocol == 'tcp' ? SUBSTRING(DestIP, 0, LAST_INDEX_OF(DestIP, '.')) : DestIP) AS DestIP, Protocol;
IPTrace3 = FOREACH IPTrace2 GENERATE Time, ConnID, (Protocol == 'UDP,' ? SUBSTRING(SourceIP, 0, LAST_INDEX_OF(SourceIP, '.')) : SourceIP) AS SourceIP, (Protocol == 'UDP,' ? SUBSTRING(DestIP, 0, LAST_INDEX_OF(DestIP, '.')) : DestIP) AS DestIP, Protocol;

RB_line = FOREACH raw_block_datafile GENERATE ConnectionID, Action;

join_data = JOIN IPTrace3 BY ConnID, RB_line BY ConnectionID;

filtered_block = FILTER join_data BY Action == 'Blocked';

firewall_temp = FOREACH filtered_block GENERATE Time, ConnID, SourceIP, DestIP, Action;

-- Write to FS
STORE firewall_temp INTO 'lab4/exp3/firewall';

-- Group data
grouped_sourceIP = GROUP firewall_temp BY SourceIP;

total_blocks = FOREACH grouped_sourceIP GENERATE COUNT(firewall_temp) AS block_count, group;

sorted_blocks = ORDER total_blocks BY block_count DESC;

STORE sorted_blocks INTO 'lab4/exp3/output';


