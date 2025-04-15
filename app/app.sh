#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip wheel setuptools

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh


# Run the indexer
bash index.sh /data

# Run the ranker
bash search.sh "A Brief History of Time: From the Big Bang"

bash search.sh "Each of the guardians had their own view of the ideal"

bash search.sh "A Child Is Born is a 1939 American drama film directed by Lloyd Bacon and written"