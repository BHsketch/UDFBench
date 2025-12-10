rm resultsTemp.out
rm resultsAPE.out
rm stats.txt

touch stats.txt
echo "3" >> stats.txt
echo "2" >> stats.txt
echo "ExtractFromDate AggregateAvg" >> stats.txt
echo "python pandas scala" >> stats.txt

timesRow=""

./automations/run_udfbench.sh pyspark s t0 cold ssd false 2
echo "################### Results: ExtractFromDate Python Version ################" >> resultsTemp.out
cat results/logs/pyspark/experiments/s-2-t0-cold-ssd.txt > resultsTemp.out
timesRow+=$(cat resultsTemp.out | grep -h "Execution Time:" | awk '{print $(NF-1)}' | tr '\n' ' ')
cat resultsTemp.out >> resultsAPE.out

timesRow+="0.0"

timesRow=$(echo "$timesRow" | sed 's/[[:space:]]\+$//')
echo $timesRow >> stats.txt
timesRow=""

./automations/run_udfbench.sh pyspark s t0 cold ssd false 2Mod1
echo "################### Results: ExtractFromDate Pandas UDF Version ################" >> resultsTemp.out
cat results/logs/pyspark/experiments/s-2Mod1-t0-cold-ssd.txt > resultsTemp.out
timesRow+=$(cat resultsTemp.out | grep -h "Execution Time:" | awk '{print $(NF-1)}' | tr '\n' ' ')

./automations/run_udfbench.sh pyspark s t0 cold ssd false 4
echo "################### Results: AggregateAvg Pandas UDF Version ################" >> resultsTemp.out
cat results/logs/pyspark/experiments/s-4-t0-cold-ssd.txt > resultsTemp.out
timesRow+=$(cat resultsTemp.out | grep -h "Execution Time:" | awk '{print $(NF-1)}' | tr '\n' ' ')
cat resultsTemp.out >> resultsAPE.out


timesRow=$(echo "$timesRow" | sed 's/[[:space:]]\+$//')
echo $timesRow >> stats.txt
timesRow=""

./automations/run_udfbench.sh pyspark s t0 cold ssd false 2Mod2
echo "################### Results: ExtractFromDate Scala Version ################" >> resultsTemp.out
cat results/logs/pyspark/experiments/s-2Mod2-t0-cold-ssd.txt > resultsTemp.out
timesRow+=$(cat resultsTemp.out | grep -h "Execution Time:" | awk '{print $(NF-1)}' | tr '\n' ' ')
cat resultsTemp.out >> resultsAPE.out


./automations/run_udfbench.sh pyspark s t0 cold ssd false 4Mod1
echo "################### Results: AggregateAvg Scala Version ################" >> resultsTemp.out
cat results/logs/pyspark/experiments/s-4Mod1-t0-cold-ssd.txt > resultsTemp.out
timesRow+=$(cat resultsTemp.out | grep -h "Execution Time:" | awk '{print $(NF-1)}' | tr '\n' ' ')
cat resultsTemp.out >> resultsAPE.out

timesRow=$(echo "$timesRow" | sed 's/[[:space:]]\+$//')
echo $timesRow >> stats.txt
timesRow=""

python3 genPlot.py

#cat resultsTemp.out | grep -h "Execution Time:" | awk '{print $(NF-1)}' | tr '\n' ' ' >> stats.txt
