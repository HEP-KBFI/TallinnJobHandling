
cmsenv
source setup.sh
(needs TallinnNtupleProducer)

law run ProdTallinnNTuples --version v1  --workflow slurm --CreateTallinnNtupleConfigs-workflow local --transfer-logs True --era 2017 --analysis hh-multilepton

Remove output: --remove-output N, N==DEPTH
