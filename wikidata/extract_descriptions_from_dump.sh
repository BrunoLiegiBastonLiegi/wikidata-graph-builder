#!/bin/bash

regex="<http:\/\/www\.wikidata\.org\/entity\/Q[0-9]+> <http:\/\/schema\.org\/description> \".+\"@en"

outfile=$1

if [ -z "$outfile" ]
then
	outfile="descriptions.txt"
fi

bzgrep -e "$regex" latest-truthy.nt.bz2 | sed "s/<http:\/\/www\.wikidata\.org\/entity\///g" | sed "s/<http:\/\/schema\.org\/description>//g" | sed "s/>//g" | sed "s/\"//g" | sed "s/@en[-a-z ]*//g" > "$outfile"
