#!/bin/bash

ents_file=$1
triplets=$2

new_triplets=$(grep -f $ents_file $triplets)

echo $new_triplets > "filtered_rdf_triplets.txt"
