#Get beer rating information from brewery page.

import numpy as np
import pandas as pd
from time import sleep

import requests
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup

from getUntappdURL import makeSoup, makeSoupSession

#Import brewery databse

dfBrewery = pd.read_csv('brews/breweries_final.csv')

def getBeerURL(breweryURL):
    """Get the beer page URL"""
    return breweryURL + '/beer'

#Most basic rating info is the total number of ratings,
#the aggregate (average) rating, and the number of beers in Untappd.

def getBasicRating(url):
    """Get (average rating, number of ratings, number of beers),
    given the beer page URL"""
    soup = makeSoup(url)
    #rating is under <p class="rating"> <span class="num">
    rawRating = soup.find('p', 'rating').find('span', 'num').string
    if not rawRating: #it can be N/A. I'll record these as nan
        rating = np.nan
    else:
        rating = float(rawRating.strip('()'))
    #number of ratings is under <p class="raters">
    rawRaters = soup.find('p', 'raters').string
    #number of beers is under <p class="
    rawBeers = soup.find('p', 'count').string
    
    #Each of these is a string, w/ format
    #'(#.##)', ' ###,### Ratings ', ' ### Beers '
    return (rating,
            int(rawRaters.split()[0].replace(',', '')),
            int(rawBeers.split()[0].replace(',', '')))

def getBasicRatingSession(url, session):
    """Get (average rating, number of ratings, number of beers),
    given the beer page URL"""
    soup = makeSoupSession(url, session)
    #rating is under <p class="rating"> <span class="num">
    rawRating = soup.find('p', 'rating').find('span', 'num').string
    if not rawRating: #it can be N/A. I'll record these as nan
        rating = np.nan
    else: #otherwise, strip '()'
        rating = float(rawRating.strip('()'))
    #number of ratings is under <p class="raters">
    rawRaters = soup.find('p', 'raters').string
    #number of beers is under <p class="
    rawBeers = soup.find('p', 'count').string
    
    #The last 2 are strings w/ formats ' ###,### Ratings ', ' ### Beers '
    return (rating,
            int(rawRaters.split()[0].replace(',', '')),
            int(rawBeers.split()[0].replace(',', '')))

#Write functions to extract 1. name, 2. style, 3. ABV, 4. IBU,
#5. rating, 6. number of ratings, from the HTML.

def getBeerTags(soup):
    """Get 'beer-details' tags and 'details' tags from /beer page,
    omitting the first item found for 'details', because this is
    actually 'details brewery claimed' tag."""
    return (soup.findAll('div', 'beer-details'),
            soup.findAll('div', 'details')[1:])

def getBeerRatingFromTags(tag1, tag2):
    """Given 'beer-details' tag and 'details' tag for a beer,
    get (name, style, ABV, IBU, rating, number of ratings)."""
    name      = tag1.find('p', 'name').text
    style     = tag1.find('p', 'style').text
    rawABV    = tag2.find('p', 'abv').text
    rawIBU    = tag2.find('p', 'ibu').text
    rawRating = tag2.find('p', 'rating').text
    rawRaters = tag2.find('p', 'raters').text
    #the last 4 needs parsing
    try: #read off # from ' #% ABV '
        ABV = float(rawABV.strip().split('%')[0])
    except ValueError: # ' N/A ABV '
        ABV = np.nan
    try: #read off # from ' # IBU '
        IBU = float(rawIBU.split()[0])
    except ValueError: # ' N/A IBU '
        IBU = np.nan
    try: #read off '  (#) '
        rating = float(rawRating.strip().strip('()'))
    except ValueError: # '  (N/A) '
        rating = np.nan
    try: #read off ' # Ratings '
        raters = float(rawRaters.split()[0].replace(',', ''))
    except ValueError:
        raters = np.nan
    return (name, style, ABV, IBU, rating, raters)

def getBeerRatings(url):
    """Get the list of up to 25 most popular beers for a brewery,
    where each entry is (name, style, ABV, IBU, rating, number of ratings)"""
    if not url: #no URL
        return [] #no beers
    #make soup from the beer page
    soup = makeSoup(url + '/beer')
    #get the lists of tags for beers and their ratings
    list1, list2 = getBeerTags(soup)
    return [getBeerRatingFromTags(tag1, tag2)
            for tag1, tag2 in zip(list1, list2)]

def getBeerRatingsSession(url, session):
    """Get the list of up to 25 most popular beers for a brewery,
    where each entry is (name, style, ABV, IBU, rating, number of ratings)"""
    if not url: #no URL
        return [] #no beers
    #make soup from the beer page
    soup = makeSoupSession(url + '/beer', session)
    #get the lists of tags for beers and their ratings
    list1, list2 = getBeerTags(soup)
    return [getBeerRatingFromTags(tag1, tag2)
            for tag1, tag2 in zip(list1, list2)]



if __name__ == "__main__":
    
    ratingDict = {}

    for url in dfBrewery['untappdURL']:
        if (type(url) == str) and (url not in ratingDict):
            #skip nan and URLs already seen
            sleep(5.)
            try:
                basic = getBasicRating(getBeerURL(url))
                print(url, basic)
                ratingDict[url] = basic
            except AttributeError:
                print(url, 'AttributeError')

    for url in ratingDict.keys():
        if type(ratingDict[url]) != tuple:
            basic = getBasicRating(getBeerURL(url))
            print(url, basic)
            ratingDict[url] = basic

    #URLs that didn't work
    failedURLs = set(dfBrewery['untappdURL']) - set(ratingDict.keys())

    with requests.session() as s:
        #pretend to be Chrome
        s.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'})
        for url in failedURLs:
            if type(url) == str: #ignore nan
                sleep(5.)
                try:
                    basic = getBasicRatingSession(getBeerURL(url), s)
                    print(url, basic)
                    ratingDict[url] = basic
                except AttributeError:
                    print(url, 'AttributeError')

    dfRating = pd.DataFrame.from_dict(ratingDict, orient='index')
    dfRating.columns = ['rating', 'raters', 'beers']

    dfRating.to_csv('brews/brewery_ratings.csv')

    #Get beer ratings

    beerRatingDict = {}

    for url in dfBrewery['untappdURL']:
        if url not in beerRatingDict:
            try:
                beerRatingDict[url] = getBeerRatings(url)
                print(url, len(beerRatingDict[url]))
                sleep(5.)
            except AttributeError:
                print(url, 'Attribute Error')
            except TypeError:
                print(url, 'Type Error')

    with requests.session() as s:
        #pretend to be Chrome
        s.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'})
        for url in dfBrewery['untappdURL']:
            if url not in beerRatingDict:
                try:
                    beerRatingDict[url] = getBeerRatingsSession(url, s)
                    print(url, len(beerRatingDict[url]))
                    sleep(5.)
                except AttributeError:
                    print(url, 'Attribute Error')
                except TypeError:
                    print(url, 'Type Error')

    #Make a DataFrame of beers.
    dfBeer = pd.DataFrame()

    for url in beerRatingDict:
        if beerRatingDict[url]:
            print(url, len(beerRatingDict[url]))
            dfBrewery = pd.DataFrame(beerRatingDict[url])
            #associate the brewery URL
            dfBrewery[6] = url
            #popularity rank within brewery
            dfBrewery[7] = range(1, len(dfBrewery) + 1)
            dfBeer = dfBeer.append(dfBrewery)

    dfBeer.columns = ['name', 'style', 'ABV', 'IBU', 'rating', 'raters', 'brewery_URL', 'rank']
    
    dfBeer = dfBeer.set_index('name')

    dfBeer.to_csv('brews/beers.csv')
