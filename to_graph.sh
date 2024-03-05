#!/bin/bash
#SBATCH -o wikidata_graph.log 
#SBATCH -p sim
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1

python to_graph.py --entities $1 --outfile $2
