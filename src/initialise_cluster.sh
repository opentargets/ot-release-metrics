#!/bin/bash


# This part is cloned from gs://goog-dataproc-initialization-actions-europe-west1/python/pip-install.sh

set -exo pipefail

readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function run_with_retry() {
  local -r cmd=("$@")
  for ((i = 0; i < 10; i++)); do
    if "${cmd[@]}"; then
      return 0
    fi
    sleep 5
  done
  err "Failed to run command: ${cmd[*]}"
}

function install_pip() {
  if command -v pip >/dev/null; then
    echo "pip is already installed."
    return 0
  fi

  if command -v easy_install >/dev/null; then
    echo "Installing pip with easy_install..."
    run_with_retry easy_install pip
    return 0
  fi

  echo "Installing python-pip..."
  run_with_retry apt update
  run_with_retry apt install python-pip -y
}

function main() {
  if [[ -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify PIP_PACKAGES metadata key"
    exit 1
  fi

  install_pip
  run_with_retry pip install --upgrade ${PACKAGES}
}

main


# This part is custom: installing PIS and fetching evidence.

python3 -m pip install -q configargparse strenum jsonpickle google-cloud-storage addict yapsy
git clone -b master https://github.com/opentargets/platform-input-support
cd platform-input-support
python3 ./platform-input-support.py -steps Evidence -o $HOME/output &> $HOME/pis.log
