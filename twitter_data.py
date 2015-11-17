#!/usr/bin/python

# Import modules

import tweepy
from tweepy import OAuthHandler
import sys
import pymongo
from pymongo import MongoClient

# Helper functions

# Authenticate
def auth(consumer_key, consumer_secret, access_token, access_secret):
	# Authenticate
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
 
	# Connect the api
	api = tweepy.API(auth)
	# Return
	return(api)

# Connect to mongodb
def connect_mongo(collection):
    # Return
    return(db)

# Download tweets with hashtag
def dTweets(collection, api, hashtag, number):
	# Get cursor
	curs = tweepy.Cursor(api.search, q=hashtag).items(number)
	# For each element, send to mongo
	for c in curs:
		cJson = c._json
    	# Check if in Mongo
		res = checkMongo(collection, hashtag, cJson["id"])
		# If true . . .
		if res == True:
			return("Found duplicate")
		else:
			saveToMongo(collection, hashtag, cJson)
	# Return
	return("All done")
    		

def checkMongo(collection, hashtag, id):
	# Connect
	client = MongoClient()
	db = client[collection]
	# Check
	if db[hashtag].find_one({"id": id}):
		found = True
	else:
		found = False
    # Disconnect
	client.close()
    # Return
	return found

# Save to mongo
def saveToMongo(collection, hashtag, result):
	client = MongoClient()
	db = client[collection]
    # Insert
	db[hashtag].insert(result)
    # Close
	client.close()

# Main
def main():
	# Sys arguments
	consumer_key = sys.argv[1]
	consumer_secret = sys.argv[2]
	access_token = sys.argv[3]
	access_secret = sys.argv[4]
	# Specify hashtag
	hashtag = sys.argv[5]
	# Number items
	number_items = sys.argv[6]
	# MongoDB collection
	mdb = sys.argv[7]

	# Authenticate
	api = auth(consumer_key, consumer_secret, access_token, access_secret)

	# Download, save and stuff
	print dTweets(mdb, api, hashtag, number_items)

# Run

if __name__ == "__main__":
	main()

