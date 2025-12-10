rm resultsAPE.out

./automations/run_udfbench.sh pyspark s t0 cold ssd false 2
echo "################### Results: ExtractFromDate Python Version ################" >> resultsAPE.out
cat results/logs/pyspark/experiments/s-2-t0-cold-ssd.txt >> resultsAPE.out
echo "" >> resultsAPE.out

./automations/run_udfbench.sh pyspark s t0 cold ssd false 2Mod1
echo "################### Results: ExtractFromDate Pandas UDF Version ################" >> resultsAPE.out
cat results/logs/pyspark/experiments/s-2Mod1-t0-cold-ssd.txt >> resultsAPE.out
echo "" >> resultsAPE.out

./automations/run_udfbench.sh pyspark s t0 cold ssd false 2Mod2
echo "################### Results: ExtractFromDate Scala Version ################" >> resultsAPE.out
cat results/logs/pyspark/experiments/s-2Mod2-t0-cold-ssd.txt >> resultsAPE.out
echo "" >> resultsAPE.out

./automations/run_udfbench.sh pyspark s t0 cold ssd false 4
echo "################### Results: AggregateAvg Pandas UDF Version ################" >> resultsAPE.out
cat results/logs/pyspark/experiments/s-4-t0-cold-ssd.txt >> resultsAPE.out
echo "" >> resultsAPE.out

./automations/run_udfbench.sh pyspark s t0 cold ssd false 4Mod1
echo "################### Results: AggregateAvg Scala Version ################" >> resultsAPE.out
cat results/logs/pyspark/experiments/s-4Mod1-t0-cold-ssd.txt >> resultsAPE.out
echo "" >> resultsAPE.out
