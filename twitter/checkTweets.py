#! /usr/bin/python
import sys,fcntl,json,re
import time,random
import os.path
import gzip
from multiprocessing import Pool

tweetPath= "/net/data/twitter/gardenhose-data/summarized"


#return urls of a string
def findUrl(tweet):
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', tweet)
    return urls


#Find twitter links that are likly to be from pinterest
def parseDays(fileName):

    parcial=""
    tweets = gzip.open(tweetPath+"/"+fileName,"r")
    for tweet in tweets:
        if ( (tweet.find("Pinterest") != -1 or tweet.find("pinterest") != -1) ):
            for url in findUrl(tweet):
                if url.find("t.co/") != -1:
                    parcial = parcial + url + "\n"

    #Mark this day as totaly read
    days = open("./dias_verificados","a+")
    days.write(fileName+"\n")
    days.close()

    #Save all urls found
    saida = open("./urls","a+")
    saida.write(parcial)
    saida.close()



# ---- MAIN --- #
output_filename = "./dias_verificados"
usuariosTotais = os.listdir(tweetPath)

already_collected = set()
all_users= set()

old_file = open(output_filename, "r")
for user_id in old_file:
    already_collected.add(user_id.strip())
    old_file.close()

for user in usuariosTotais:
    all_users.add(user.rstrip())

#select the remaining days to parse
to_collect = all_users.difference(already_collected)

print "Remaning: " + str(len(to_collect))

p= Pool(processes=4)
p.map(parseDays,list(to_collect))
