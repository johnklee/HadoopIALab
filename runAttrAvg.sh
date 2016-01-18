#!/bin/sh  
OUTPUT="/IA_AttrAvg"  
hadoop fs -rm -r $OUTPUT  
hadoop jar HadoopIA.jar ia.ch4.avgattr.AttrAvg /IA/apat63_99.txt $OUTPUT  
hadoop fs -cat "${OUTPUT}/*" | less
