#!/bin/sh  
OUTPUT="/IA_Histogram"  
SOURCE='/IA_Count'
hadoop fs -rm -r $OUTPUT  
hadoop jar HadoopIA.jar ia.ch4.counting.CitationHistogram $SOURCE $OUTPUT  
hadoop fs -cat "${OUTPUT}"/* | less
