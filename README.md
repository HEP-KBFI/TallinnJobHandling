# Job handling

Make sure you are in a CMSSW area

```bash
cmsrel CMSSW_12_3_1
cd $_ && cmsenv
```

Since this code depends on the ```HEP-KBFI/TallinnNtupleProducer```, make sure you have also this available locally

```bash
cd $CMSSW_BASE/src
git clone git@github.com:HEP-KBFI/TallinnNtupleProducer.git
scram b -j8
```

To set up the environment to use job handling:

```bash
cd $CMSSW_BASE/src
git clone git@github.com:HEP-KBFI/TallinnJobHandling.git
cd TallinnJobHandling
source setup.sh
```

## Running the jobhandling

```bash
law run ProdTallinnNTuples --version v1  --workflow slurm --CreateTallinnNtupleConfigs-workflow local --transfer-logs True --era 2017 --analysis hh-multilepton
```

```Remove output: --remove-output N, N==DEPTH```
