emp  = LOAD '/piginput/employee.txt' using PigStorage(',') AS (id:int,name:chararray,job:chararray,mgr:int,joindt:chararray,sal:int,comm:int,dept:int); 
groupa = GROUP emp BY dept;
Maxsal = FOREACH groupa GENERATE group,MAX(emp.sal);