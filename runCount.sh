#!/bin/sh  
OUTPUT="/IA_Count"  
hadoop fs -rm -r $OUTPUT  
hadoop jar HadoopIA.jar ia.ch4.counting.CountCiting /IA/cite75_99.txt $OUTPUT  
hadoop fs -cat "${OUTPUT}/*" | less
