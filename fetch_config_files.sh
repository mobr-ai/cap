#!/bin/bash

# Script to fetch cardano-node and cardano-db-sync config files
# This script should be executed from the root directory

#create a directory to hold the config files
mkdir -p $HOME/cardano/preview-config

#download the config files
cd $HOME/cardano/preview-config

curl -O https://book.world.dev.cardano.org/environments/preview/config.json
curl -O https://book.world.dev.cardano.org/environments/preview/topology.json
curl -O https://book.world.dev.cardano.org/environments/preview/db-sync-config.json
curl -O https://book.world.dev.cardano.org/environments/preview/submit-api-config.json
curl -O https://book.world.dev.cardano.org/environments/preview/byron-genesis.json
curl -O https://book.world.dev.cardano.org/environments/preview/shelley-genesis.json
curl -O https://book.world.dev.cardano.org/environments/preview/alonzo-genesis.json
curl -O https://book.world.dev.cardano.org/environments/preview/conway-genesis.json
curl -O https://book.world.dev.cardano.org/environments/preview/db-sync-config.json

#go back to project root
cd -