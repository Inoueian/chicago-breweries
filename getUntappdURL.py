#Adds Untappd URL to brewery database.

import pandas as pd
from time import sleep

import requests
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup

#Import brewery database
dfBrewery = pd.read_csv('brews/breweries_final.csv')

#Set up functions for scraping Untappd

def simpleGet(url):
    """Attempts to get the content at 'url' by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None"""
    try:
        with closing(requests.get(url, stream=True)) as resp:
            if isGoodResponse(resp):
                return resp.content
            else:
                return None
    
    except RequestException as e:
        logError('Error during requests to {0} : {1}'.format(url, str(e)))
        return None
    
def isGoodResponse(resp):
    """Returns True if the response seems to be HTML"""
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)

def logError(e):
    """Prints error"""
    print(e)

def makeSoup(url):
    """Use simpleGet function to make soup from url"""
    try:
        return BeautifulSoup(simpleGet(url), 'html.parser')
    except TypeError as e:
        logError('Non-HTML content at {0} : {1}'.format(url, str(e)))
        return None

#Make functions that takes us from brewery name to 
#Untappd brewery search page to Untappd brewery page.

#Make a dict to treat exceptional cases.
#Breweries not yet on Untappd are mapped to '',
#and others are breweries that need alternative search terms.
nameDict = {'Aleman': 'Aleman Brewing',
            'Bixi Brewery': '',
            'Crown Brewing': 'Crown Brewing Company',
            'Englewood Brews': '',
            'FIBS Brewing': '',
            'Lake Effect Brewing Co.': 'Lake Effect Brewing',
            'McHenry Brewing Company': "Chain O'Lakes Brewing Company",
            'Oak Park Brewing Co.': 'Oak Park Brewing Company (IL)',
            'Strike Ten Brewing Co.': '',
            'Scallywag Brewing': '',
            'Whiner Brewery': ''
            } 

def searchURL(name):
    """Returns the URL to search Untappd for brewery"""
    #remove parts after '–', replace ' ' by '+'
    nameParsed = name.split(sep='–')[0].strip().replace(' ', '+')
    return 'https://untappd.com/search?q=' + nameParsed + '&type=brewery'

def getBreweryURL(name):
    """Returns the Untappd brewery page URL given a brewery name"""
    if name == '': #recursion did not work
        return None
    elif name in nameDict: #convert exceptional cases
        name = nameDict[name]
        
    try:
        url = searchURL(name)
        soup = makeSoup(url)
        #get the result part of the HTML
        result = soup.find("div", {"class": "results-container"})
        #go to the top brewery in the search
        nameTag = result.find("p", {"class": "name"})
        #get the href
        href = name.find("a")['href']
        return 'https://untappd.com' + href
    except AttributeError: #no search result
        #try without the last word (recursion)
        shorterName = ' '.join(name.split()[:-1])
        return getBreweryURL(shorterName)

#For search, I need to log in.
#Define logged in versions of the above functions.

def getSessionKey(session):
    """Get session key from the Untappd login page."""
    login = session.get('http://untappd.com/login')
    soup  = BeautifulSoup(login.content, 'html.parser')
    form  = soup.find('form')
    return form.find('input', attrs={'name': 'session_key'})['value']

def simpleGetSession(url, session):
    """Attempts to get the content at 'url' by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None"""
    try:
        with closing(session.get(url, stream=True)) as resp:
            if isGoodResponse(resp):
                return resp.content
            else:
                return None
    
    except RequestException as e:
        logError('Error during requests to {0} : {1}'.format(url, str(e)))
        return None
    
def makeSoupSession(url, session):
    """Use simple_get function to make soup from url"""
    try:
        return BeautifulSoup(simpleGetSession(url, session), 'html.parser')
    except TypeError as e:
        logError('Non-HTML content at {0} : {1}'.format(url, str(e)))
        return None

def getBreweryURLSession(name, session):
    """Returns the Untappd brewery page URL given a brewery name"""
    if name == '': #recursion did not work
        return None
    elif name in nameDict: #convert exceptional cases
        name = nameDict[name]
        
    try:
        url = searchURL(name)
        soup = makeSoupSession(url, session)
        #get the result part of the HTML
        result = soup.find("div", {"class": "results-container"})
        #go to the top brewery in the search
        nameTag = result.find("p", {"class": "name"})
        #get the href
        href = nameTag.find("a")['href']
        return 'https://untappd.com' + href
    except AttributeError: #no search result
        #try without the last word (recursion)
        shorterName = ' '.join(name.split()[:-1])
        return getBreweryURLSession(shorterName, session)

#Untappd login information is saved as a text file,
#with format '{ 'username': <user name as string>,
#               'password': <password as string> }'

import ast
with open('untappdLogin.txt', 'r') as f:
    untappdDict = ast.literal_eval(f.read())

if __name__ == '__main__':

    #Record URLs in a dict first
    untappdURLDict = {}

    with requests.session() as s:
        untappdDict['session_key'] = getSessionKey(s)
        #Log into Untappd. p records the response and can be inspected
        p = s.post('https://untappd.com/login/', data=untappdDict)
        for name in dfBrewery['brewery']: #get URL for each brewery
            #sleep to get around access rate limits
            #I'm not sure how fast I can push this exactly
            sleep(1.)
            untappdURL = getBreweryURLSession(name, s)
            print(name, untappdURL)
            untappdURLDict[name] = untappdURL
    
    #Make a column for Untappd URLs
    dfBrewery['untappdURL'] = dfBrewery['brewery'].apply(lambda x: untappdURLDict[x])
    #Use empty string for breweries without an Untappd page
    dfBrewery.loc[dfBrewery['untappdURL'].isna(), 'untappdURL'] = ''
    #Save to file
    dfBrewery.to_csv('brews/breweries_final.csv')
