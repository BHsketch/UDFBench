This is a fork of [baziotis/UDFBench](https://github.com/baziotis/UDFBench). The goal of this fork is to add additional experiments for the CS598 Advanced Performance Engineering project.

First build the necessary containers:

```
docker compose -f docker-compose.yaml up -d
```

Connect to the `app` container:

```
docker compose -f docker-compose.yaml exec app bash
```

First, deploy UDFBench, e.g.:

```
./automations/deploy_udfbench.sh ssd yes yes yes pyspark
```
**Note**: This will ask you to enter which dataset size to install. We've used the `s` (small) size for our experiments. The `t` (tiny) size comes with the repository and doesn't need to be downloaded. We were facing trouble downloading other sizes and hence could not test for those.

And then you can run experiments, e.g.:

```
./automations/run_udfbench.sh pyspark t t0 cold ssd false 2
```
For running experiments related to the project:

```
bash run-ape-experiments.sh
```

This will run all necessary experiments in a sequence and output results in a file called `resultsAPE.out`.
