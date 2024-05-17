#!/bin/bash

ents_file=$1
triplets=$2

new_triplets=$(grep -wf $ents_file $triplets)

printf "$new_triplets" > "filtered_rdf_triplets.txt"
