python3 -m pip install -q configargparse strenum jsonpickle google-cloud-storage addict yapsy
git clone -b master https://github.com/opentargets/platform-input-support
cd platform-input-support
python3 ./platform-input-support.py -steps Evidence -o $HOME/output &> $HOME/pis.log
hadoop fs -copyFromLocal $HOME/output/prod/evidence-files /
