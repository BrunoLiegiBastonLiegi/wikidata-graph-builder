#!/bin/bash

ents_file=$1
dump=$2

ents_list=$(cat $ents_file | tr '\n' '|')
if [ "${ents_list: -1}" == "|" ]
then
    ents_list=${ents_list::${#ents_list}-1}
fi

ents_list="($ents_list)"

regex="<http:\/\/www\.wikidata\.org\/entity\/$ents_list> <http:\/\/www\.wikidata\.org\/prop\/direct\/P[0-9]+> <http:\/\/www\.wikidata\.org\/entity\/$ents_list> \."

bzgrep -e "$regex" $dump | sed "s/<http:\/\/www\.wikidata\.org\/entity\///g" | sed "s/<http:\/\/www\.wikidata\.org\/prop\/direct\///g" | sed "s/>//g" | sed "s/ \.//g" > "filtered_rdf_triplets.txt"
