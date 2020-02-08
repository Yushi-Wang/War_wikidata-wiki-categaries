# War_wikidata-wiki-categaries
These files are for getting conflicts information from wikidata and wikipedia categaries.
wikidataquery.py is for obtaining the conflicts' location, time, participant and "partof" information by using wikiquery.
wars_with_participants.py is for reating maps of conflicts to their wiki categaries, then we can using the categaries to obtain more information about their time, locations and participants.
get_location&participant.py is for getting the location (also participant) information from the wiki categaries and wikidata. 
gettime.py is for getting time information from the wiki categaries and wikidata.
aggregate.py is for getting a sheet containing conflicts and their start date, end date, location, country and continent information.
Files with "_battles" in their names have the same function as their corresponding files, the difference is that in these files, we add those items that have a label "battle" in their "instance of".
