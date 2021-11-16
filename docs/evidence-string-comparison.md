# Protocol for comparing evidence strings

## Overview
Comparing two sets of evidence strings can be a useful control measure for validating changes in source data and/or processing code to ensure there are no regressions, such as unexpected losses of a subset of the evidence strings. However, since the evidence strings are in JSON format, they require some preprocessing to run a meaningful diff. This protocol performs it and generates user-friendly output files and summary statistics.

The scripts for running this comparison are located in the [`compare-evidence-strings`](../compare-evidence-strings) subdirectory. They do the following:
* Sort the columns inside the evidence strings in order of decreasing uniqueness, or information content. This means that the columns with the higher diversity in the data, which are more likely to contain some identifying information, appear earlier in the string and make for a more meaningful sorting.
* Sort and deduplicate the evidence strings in each set.
* Separate and count the evidence strings which are exactly the same and appear in both sets.
* Compute the diff for the evidence strings which do not fall into the previous category.

## Running the comparison
```bash
export INSTANCE_NAME=evidence-comparison
export INSTANCE_ZONE=europe-west1-d

# Create the instance and SSH.
gcloud compute instances create \
  ${INSTANCE_NAME} \
  --project=open-targets-eu-dev \
  --zone=${INSTANCE_ZONE} \
  --machine-type=e2-highmem-2 \
  --service-account=426265110888-compute@developer.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform \
  --create-disk=auto-delete=yes,boot=yes,device-name=${INSTANCE_NAME},image=projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20210927,mode=rw,size=500,type=projects/open-targets-eu-dev/zones/europe-west1-d/diskTypes/pd-balanced
gcloud compute ssh --zone ${INSTANCE_ZONE} ${INSTANCE_NAME}
screen

# Install dependencies.
sudo apt update
sudo apt -y install aha jq python3-pip python3-venv

# Configure smart diff.
wget -q -O delta.tar.gz https://github.com/dandavison/delta/releases/download/0.9.1/delta-0.9.1-x86_64-unknown-linux-gnu.tar.gz
tar --extract --gzip --file=delta.tar.gz
sudo mv delta-*/delta /usr/bin
cat << EOF >> ~/.gitconfig
[pager]
    diff = delta
    show = delta
    log = delta
    blame = delta
    reflog = delta

[interactive]
    diffFilter = delta --color-only
EOF

# Set up the repository.
git clone https://github.com/opentargets/ot-release-metrics
cd ot-release-metrics
python3 -m venv env
source env/bin/activate
python3 -m pip install -r requirements.txt
cd compare-evidence-strings
```

Now, to compare two sets of evidence strings, download and uncompress them, then run:
```bash
bash compare.sh \
  old_evidence_strings.json \
  new_evidence_strings.json
```

Only uncompressed, plain JSON files are supported. The script will take a few minutes to run and will create a `comparison/` subdirectory in the current working directory. It will contain several intermediate files, and a single final file under the name of **`report.html.gz`**. It contains a number of summary statistics, as well as the cleared up diff for the evidence strings. Download it locally and open in a web browser to examine.
