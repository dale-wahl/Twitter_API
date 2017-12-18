# This script takes a .csv file with column tweet_id that contains
# one tweet id per row. It then calls Twitter's public API and gets
# the last users to retweet that tweet up to 100.  It then goes to
# each of those users and finds the number of followers that user has.
# The idea is to get a better idea of the additional audience of a tweet.
# You will need to enter your twitter authentication information, the
# name of a new csv file and the source csv file.

# Note: this script will also keep temporary files that can be used to
# restart the process without having to start again at the begining.
# Because of restrictions on how often you can make a call to the twitter
# API, this can be a very long process for large datasets.  For 10k tweets,
# it took almost 3 days to complete running on an AWS instance.

# https://github.com/dale-wahl

import tweepy
import pandas as pd
import time
import pickle

# Enter your twitter authentication information:
consumer_key = <your consumer key>
consumer_secret = <your consumer secret>
access_token = <your access_token>
access_token_secret = <your access token secret>

# Enter the name of the .csv file you want to create with this information
new_csv = 'retweet.csv'

# Enter the name of the .csv file with a tweet_id column
source_csv = 'tweets_10percent_12042017_0058.csv'

# You are good; she'll take it from here.

# Start up tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Import MODIFIED dataframe .csv file (though honestly as long as it has a tweet_id column it should work)
twitter_df = pd.read_csv(source_csv, index_col=0)
# Get the tweet ids
tweet_id_list = list(twitter_df.tweet_id)
# Clear up some space
del twitter_df

# Initiate dataframe and save the copy we will be appending
retweet_df = pd.DataFrame(columns=['tweet_id', 'retweeters', 'num_retweeters', 'additional_exposure'])
retweet_df.to_csv(new_csv)

# Initiate empty set to all retweeters (this avoids excess calls to Twitter API)
retweeters_set = set()

# Function to go through list of tweet_id and return a list of last retweeters (up to 100)
def find_retweeters(tweet_id_list, dataframe, csv_to_save):
    print('Find Retweeters: Begining Job.')
    dataframe.to_csv('find_retweeters_temp.csv')
    # initiate temporary .csv in case process fails or is terminated
    misses = []
    for i, tweet_id in enumerate(tweet_id_list):
        try:
            retweeters = api.retweeters(tweet_id)
            # Calls twitter api and returns a list of retweeters' user ids
            retweeters_set.update(retweeters)
            num_retweeters = len(retweeters)
            dataframe.loc[len(dataframe)]=[tweet_id, retweeters, num_retweeters, 'not_calculated']
        except:
            try:
                print('API limit reached after ', i, 'tweets. Waiting 15 minutes to continue.')
                time.sleep(900)
                # try again:
                retweeters = api.retweeters(tweet_id)
                retweeters_set.update(retweeters)
                num_retweeters = len(retweeters)
                dataframe.loc[len(dataframe)]=[tweet_id, retweeters, num_retweeters, 'not_calculated']
            except:
                misses.append(tweet_id)
                pickle.dump(misses, open('retweeter_misses.pkl', 'wb'))
                print('Error on tweet_id ', tweet_id, '.')
        if i%100 == 0:
            dataframe.to_csv('find_retweeters_temp.csv')
            # update temp csv
            print('Completed ', i, ' tweets.\nWork saved to find_retweeters_temp.csv.')
        else:
            pass
    dataframe.to_csv(csv_to_save, mode='a', header=False)
    print('Work saved to ', csv_to_save, '.')
    print('These tweets were missed:')
    pickle.dump(misses, open('retweeter_misses.pkl', 'wb'))
    print(misses)
    print('Find Retweeters: Job Complete.')

# Initialize empty dictionary for retweeters and their followers
retweeters_dict = {}

# Function to run through set of retweeters and add the number of followers for each to a dictionary
def find_followers(retweeters_set, retweeters_dict):
    """Takes the tweet_id as input and returns the number of retweeters returned (between 0 and 100) and returns the sum of all of those retweeters' followers."""
    print('Find Followers: Begining Job.')
    misses = []
    for i, retweeter in enumerate(retweeters_set):
        try:
            retweeters_dict[retweeter] = api.get_user(retweeter).followers_count
        except:
            try:
                print('API limit reached after ', i, 'retweeters.\nWaiting 15 minutes to continue.')
                time.sleep(900)
                # try again:
                retweeters_dict[retweeter] = api.get_user(retweeter).followers_count
            except:
                misses.append(retweeter)
                pickle.dump(misses, open('follower_misses.pkl', 'wb'))
                print('Error on tweet_id ', retweeter, '.')
        if i%100 == 0:
            pickle.dump(retweeters_dict, open('retweeter_followers_temp.pkl', 'wb'))
            print('Completed ', i, ' retweeters.\nWork saved to retweeter_followers_temp.pkl.')
        else:
            pass
    pickle.dump(retweeters_dict, open('retweeter_followers_temp.pkl', 'wb'))
    print('Work saved to retweeter_followers_temp.pkl.')
    print('These retweeters were missed:')
    pickle.dump(misses, open('follower_misses.pkl', 'wb'))
    print(misses)
    print('Find Followers: Job Complete.')

# Iterate through the retweeter_lists and get follower counts from the dictionary; return the sum
def sum_retweeters(retweeter_list, retweeters_dict):
    num_followers = []
    for retweeter in retweeter_list:
        try:
            num_followers.append(retweeters_dict[retweeter])
        except:
            pass
    return sum(num_followers)

#Run it!
find_retweeters(tweet_id_list, retweet_df, new_csv)
find_followers(retweeters_set, retweeters_dict)
print('Twitter API connection no longer needed. \nSumming follower counts and adding as additional exposure to dataframe.')

retweet_df.additional_exposure = retweet_df.retweeters.apply(lambda x: sum_retweeters(x, retweeters_dict)))
retweet_df.to_csv(new_csv)
print('Mission Accomplished!\n')
