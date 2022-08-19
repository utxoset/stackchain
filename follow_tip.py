#!/opt/homebrew/bin/python3
#
# Follow #stackchain from provided tip and print info in structured format so to support the quant work
# Output is sent to stdout
#

# TODO implement heuristics to sync block heights
# TODO lookup USD/BTC exchange rate at time of tweet to approximate sats stacked
# TODO properly handle proxy blocks
# TODO handle multi stacks

import re
import sys
import time

import requests
import tweepy
import argparse

# simplistic pattern to recognize block height in tweet text - any sequence of 1-5 digits followed by whitespace
from tweepy import Response

block_pattern = re.compile(r'(\d{1,5})\s+')

# stack height and tweet id of the genesis block
genesis_block = (5, '1549169119924080640')

# Crude way to deal with deleted/restricted/suspended account blocks - couldn't figure out a way to 
# jump across the gap using the API - maybe possible with conversation_id?
# the "block glue" is just a dict that maps from the tweet id from the tweet before the gap to the
# next available tweet (represented as a tuple of stack height and tweet id)
# Obviously, the status of tweets might change, so this list may have to be updated
block_glue = {1554882622634921984: (811, 1554841198011056136), 1552119154035101700: (607, 1552105775723450371),
              1551038503722655744: (496, 1551029344369123330), 1550712306715299842: (460, 1550702235335528448),
              1550637885040254979: (440, 1550633990393958401), 1550624214155411457: (436, 1550617394510430209),
              1550544089820631040: (396, 1550541421517897728), 1550541421517897728: (392, 1550533947896913920),
              1550365580279963648: (352, 1550350385348579328), 1550283090387402752: (315, 1550280553638895617),
              1549893708404781056: (213, 1549881546479370241), 1549840113370365955: (193, 1549838019926302721),
              1549770600079675392: (160, 1549765584929398784), 1549753725903900673: (152, 1549741301230346243),
              1549653584370757637: (138, 1549652638924357634), 1549651198621487104: (135, 1549648850960297984),
              1549640872228265985: (125, 1549638119615475713), 1549625948613509120: (112, 1549623253953433600),
              1549614652518277125: (100, 1549611970424410113), 1549609544405303297: (93, 1549608558991478784)}


def generic_tweet_url(tweet_id: int) -> str:
    return 'https://twitter.com/i/web/status/{:d}'.format(tweet_id)


# Unfortunately, tweepy can _either_ return the parsed response, or return the headers. So can't use this.
def throttle_for_rate_limit(api_response) -> int:
    rate_limit_remaining = int(api_response.headers['x-rate-limit-remaining'])
    rate_limit_reset = int(api_response.headers['x-rate-limit-reset'])
    now = int(time.time())
    seconds_to_reset = rate_limit_reset - now
    seconds_per_request = float(seconds_to_reset) / float(rate_limit_remaining)
    if seconds_per_request > 2:
        print("Throttling (sleeping for {:d}s): {:d} requests remaining in next {:d}s".format(int(seconds_per_request),
                                                                                              rate_limit_remaining,
                                                                                              seconds_to_reset),
              file=sys.stderr)
        return int(seconds_per_request)
    return 0


def parse_mentions(text, entities):
    """Remove the @mentions from the provided tweet text using the "entities" info returned by the Twitter API"""
    if 'mentions' in entities:
        users = list(("@" + e["username"] for e in entities["mentions"]))
        for u in users:
            text = text.replace(u + " ", "").replace(u, "")
    else:
        users = []
    return text, users


def print_block_info(tweet_id, author_username, created_at, approx_block_height, txt, blocks, mentions):
    """Print structured information about given block in .tsv format suitable for easy import into Google Sheets"""
    print('{:d}\t{:s}\t{:d}\t{:d}\t{:s}\t{:s}\t{:s}'.format(tweet_id, author_username, created_at,
                                                            approx_block_height, txt, str(blocks), str(mentions)))


def get_stack_blocks(tweet_id: int, approx_block_height: int, seconds_per_request: int):
    time.sleep(seconds_per_request)  # manage to Twitter rate limit
    api_response = client.get_tweet(tweet_id,
                                    tweet_fields=["conversation_id", "author_id", "created_at", "referenced_tweets",
                                                  "source", "public_metrics", "entities"], expansions=["author_id"])
    tweet = api_response.data
    if tweet is None:
        print('Chain broken around block {:d}: see {:s}'.format(approx_block_height, generic_tweet_url(tweet_id)),
              file=sys.stderr)
        return

    (txt, mentions) = parse_mentions(tweet.text, tweet.data["entities"])
    txt = txt.replace("\t", "  ").replace("\n", "   ")
    blocks = block_pattern.findall(txt)

    author_id = tweet.data["author_id"]
    author_username = [u.username for u in api_response.includes["users"] if str(u.id) == author_id][0]
    created_at = int(time.mktime(time.strptime(tweet.data["created_at"], "%Y-%m-%dT%H:%M:%S.000%z")))

    print_block_info(tweet_id, author_username, created_at, approx_block_height, txt, blocks, mentions)

    if tweet_id in block_glue:
        (prev_block_height, prev_block_id) = block_glue[tweet_id]
        print("Using glue to followchain @{:d} to block {:d} @{:d}".format(tweet_id, prev_block_height, prev_block_id), file=sys.stderr)

        get_stack_blocks(prev_block_id, prev_block_height, seconds_per_request)
        return

    if tweet.referenced_tweets is None:
        print("Reached end of chain @{:d}".format(tweet_id), file=sys.stderr)
        return

    replied_to = list(filter(lambda r: r.type == 'replied_to', tweet.referenced_tweets))
    if len(replied_to) > 1:
        print("Unexpected backwards fork @{:s} ".format(replied_to))
    elif len(replied_to) == 1:
        prev_id = replied_to[0].id
        get_stack_blocks(prev_id, approx_block_height - 1, seconds_per_request)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("api_bearer_token", help="bearer token for the Twitter v2 API")
    parser.add_argument("tip_tweet_id", help="tweet id of the tip", type=int)
    parser.add_argument("tip_block_height", help="block height of the tip", type=int)
    args = parser.parse_args()

    raw_client = tweepy.Client(args.api_bearer_token, return_type=requests.Response)
    raw_response = raw_client.get_tweet(genesis_block[0])
    seconds_per_request = throttle_for_rate_limit(raw_response)

    client = tweepy.Client(args.api_bearer_token, wait_on_rate_limit=True)
    get_stack_blocks(args.tip_tweet_id, args.tip_block_height, seconds_per_request)
