#!/bin/bash

#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

# install general requirements
yes | sudo yum install git python3-pip java-17-amazon-corretto-devel

# check out appropriate repos and branches
cd ~
yes | rm -r os_repo
mkdir os_repo && cd os_repo && git clone https://github.com/enarvade/OpenSearch.git
cd OpenSearch && git checkout caffeine-cache
cd ~
yes | rm -r osb_core
mkdir osb_core && cd osb_core && git clone https://github.com/peteralfonsi/opensearch-benchmark.git
cd opensearch-benchmark && git checkout zipf-alpha-parameter
cd ~
yes | rm -r osb
mkdir osb && cd osb && git clone https://github.com/peteralfonsi/opensearch-benchmark-workloads.git
# NOTE: does not check out a particular branch for osb workloads as this is benchmark-dependent!

# uninstall pip osb installation if present
yes | pip uninstall opensearch-benchmark

# setup to build osb from source
# install pyenv
curl https://pyenv.run | bash
cat << 'EOF' >> ~/.bashrc
export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
EOF
source ~/.bashrc

# install docker
yes | sudo yum install docker
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose

# install other dependencies
yes | sudo yum install make patch gcc zlib-devel openssl-devel libffi-devel readline-devel sqlite-devel ncurses-devel xz-devel tk-devel

# build osb
cd ~/osb_core/opensearch-benchmark
make prereq
make install
source .venv/bin/activate

# delete leftover files that might be present from earlier runs
rm jstack-outputs/*

# configure benchmark.ini to use the correct metric store
rm ~/.benchmark/benchmark.ini
mkdir -p ~/.benchmark
cat << 'EOF' >> ~/.benchmark/benchmark.ini
[meta]
config.version = 17

[system]
env.name = local

[node]
root.dir = /home/ec2-user/.benchmark/benchmarks
src.root.dir = /home/ec2-user/.benchmark/benchmarks/src

[source]
remote.repo.url = https://github.com/opensearch-project/OpenSearch.git
opensearch.src.subdir = opensearch

[benchmarks]
local.dataset.cache = /home/ec2-user/.benchmark/benchmarks/data

[results_publishing]
datastore.type = opensearch
datastore.host = search-benchmark-metrics-store-duabrmehd2byzmo47npzmqbnuy.us-east-2.es.amazonaws.com
datastore.port = 443
datastore.secure = True
datastore.user = petealft
datastore.password = Password1^
datastore.ssl.verification_mode = none

[workloads]
default.url = https://github.com/opensearch-project/opensearch-benchmark-workloads

[provision_configs]
default.dir = default-provision-config

[defaults]
preserve_benchmark_candidate = false

[distributions]
release.cache = true
EOF
