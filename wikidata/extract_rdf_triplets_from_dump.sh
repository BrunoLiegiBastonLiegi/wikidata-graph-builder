#!/bin/bash

outfile=$1
regex="<http:\/\/www\.wikidata\.org\/entity\/Q[0-9]+> <http:\/\/www\.wikidata\.org\/prop\/direct\/P[0-9]+> <http:\/\/www\.wikidata\.org\/entity\/Q[0-9]+> \."

if [ -z "$outfile" ]
then
	outfile="rdf_triplets.txt"
fi

bzgrep -e "$regex" latest-truthy.nt.bz2 | sed "s/<http:\/\/www\.wikidata\.org\/entity\///g" | sed "s/<http:\/\/www\.wikidata\.org\/prop\/direct\///g" | sed "s/>//g" | sed "s/ \.//g" > "$outfile"
