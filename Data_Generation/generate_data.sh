#! /bin/bash

# run the producer script to generate data and send them to message broker
python3 data_generate.py


# remove all text files that were generated during previous script execution
rm "$(pwd)"/*.txt