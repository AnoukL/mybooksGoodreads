# -*- coding: utf-8 -*-
"""
Created on Thu Aug 17 19:08:18 2017

@author: Anouk Luypaert
"""
## https://pypi.python.org/pypi/Goodreads/0.2.4
# https://www.goodreads.com/topic/show/18192128-listing-books-in-shelves?comment=157183088#comment_157183088


GENRES = ["fantasy", "philosophy", "data-science", "economics", "computer-science",
          "religion", "politics", "history", "novel", "psychology", "autism",
          "programming", "cookbooks", "science-fiction", "science", "linguistics", 
          "mathematics", "horror", "childrens", "historical-fiction", "classics", 
          "thriller", "biography", "physics", "biology", "evolution", "sociology"]
ID = 18934200
KEY = "c4YaHJZkIlYhWbxuMKUyFw"
SECRET = "e9YbkJGxtDarHYeobVHRwlyxJDY1QVSCvkxXHoKVg6o"
SAVEDGENRES = "D:\projects\goodreads\goodreadsGenres.pkl"

import requests
import bs4
import re
import numpy as np
from goodreads import client
from datetime import datetime, date
import pandas as pd

#this is only used to get a book id and the corresponding popular shelves to assign
#a genre
gc = client.GoodreadsClient(KEY, SECRET)


## get a book from GR based on ID
def get_book(id):
    return gc.book(id)

## loops through the shelves on GR that are attached to the book
## and selects the most popular one that is in the predefined list GENRES
def get_genre(id):
    boek = get_book(id)
    for g in boek.popular_shelves:
        if g.name in GENRES:
            return g.name
    else:
        return "not found"
    
## get all the books on a given shelf
def getUserBooks(shelf):
    url = 'https://www.goodreads.com/review/list'
    params = {'v': 2, 
    'id': ID, 
    'shelf': shelf, 
    'sort': 'title', 
    'key': "c4YaHJZkIlYhWbxuMKUyFw",
    'per_page': 200}
    
    response = requests.get(url, params=params)
    return response.text


#return a list based on a source list and convert into proper formatting
def addElementList(sourceL, dataType="string"):
    result = []
    
    if dataType == "string":
        for div in sourceL:
            result.append(str(div.text))
    elif dataType == "integer":
        for div in sourceL:
            if div.text == "" or div.text == 0:
                result.append(div.text)
            else:
                result.append(int(div.text))
    elif dataType == "date":
        for div in sourceL:
            #https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
            date_object = date(9999, 12, 31).strftime("%d/%m/%Y")
            if div.text == "" or div.text == 0:
                result.append(date_object) 
            else:
                date_object = datetime.strptime(div.text, "%a %b %d %H:%M:%S %z %Y")
                # convert to dd/m/yyyy
                date_object = date_object.strftime("%d/%m/%Y")
                result.append(date_object)     
    return result


# get a list of ids from a bs4 result
def getID(sourceList):
    result = []
    for div in sourceList:
        if "integer" in str(div):
            result.append(str(div.text)) 
    return result


#get avg rating of the book, skipping over the author's ratings, from a bs4 result
def getRatings(sourceList):
    avgRating = []
    rawRatings = sourceList.find_all("average_rating")         
    for i in range(0, len(rawRatings), 2):
        avgRating.append(rawRatings[i].text) 
    return avgRating


#get the books' shelves that user has added from a bs4 result
def getShelves(sourcelist):
    allMyShelves = []
    
    for div in sourcelist.find_all("shelves"):
        shelves = []
        for i in range(1, len(div.contents) - 1, 2):
            rawShelf = str(div.contents[i])
            name = re.search("name=", rawShelf)
            splitText = rawShelf[name.start()+6:].split('"')
            shelves.append(splitText[0])
        allMyShelves.append(", ".join(shelves))
        
    return allMyShelves


#check if the book is on the currently reading bookshelf
# if it is, return true
def currently_reading_status(bookshelf):
    return "currently-reading" in bookshelf 


#check if a book is read
def read_status(bookshelf): 
     return bool(re.search(r"(?<!to-)read\b", bookshelf))
     
    
# get the books on given shelf and return a dataframe
def getBooksonShelf(shelf):
    # get the info from GR parsed through bs4
    myBooks = bs4.BeautifulSoup(getUserBooks(shelf), "lxml")
    
    # get the parsed information and store it in lists
    author = addElementList(myBooks.find_all("name"))
    title = addElementList(myBooks.find_all("title"))
    isbn13 = addElementList(myBooks.find_all("isbn13"))
    published = addElementList(myBooks.find_all("publication_year"), "integer")  
    pages = addElementList(myBooks.find_all("num_pages"), "integer")
    owned = addElementList(myBooks.find_all("owned")) 
    myRating = addElementList(myBooks.find_all("rating"))
    dateAdded = addElementList(myBooks.find_all("date_added"), "date")
    dateRead = addElementList(myBooks.find_all("read_at"), "date") 
    publisher = addElementList(myBooks.find_all("publisher"))
    bookId = getID(myBooks.find_all("id"))        
    avgRating = getRatings(myBooks)
    bookShelves = getShelves(myBooks)
    
    # combine the lists into a pd dataframe    
    myPandasBooks = pd.DataFrame(np.column_stack([bookId, title, author, publisher,
                                                  isbn13, published, pages, owned,
                                                  bookShelves, dateAdded, dateRead,
                                                  avgRating, myRating]), 
                                 columns=["bookId", "title", "author", "publisher",
                                                  "isbn13", "published", "pages",
                                                  "owned", "bookshelves", "dateAdded",
                                                  "dateRead", "avgRating", "myRating"])
    
    # add  extra info for dashboard 
    myPandasBooks["currently-reading"] = myPandasBooks["bookshelves"].apply(
            currently_reading_status)
    
    myPandasBooks["read"] = myPandasBooks["bookshelves"].apply(
            read_status)
           
    return myPandasBooks


# joins the dataframe with the genres parsed in a previous run, if possible. 
# If a bookId is not in the saved file, it looks up the genre with get_genre()
def addGenreToDF(dataframe):

    #set index to BookId to be able to join with the genres df
    dataframe = dataframe.set_index("bookId", drop=False)  
    
    #if there was a previous run, there is an existing file that includes
    #the genre for some book ids.
    try:
        existingGenres = pd.read_pickle(SAVEDGENRES)
        print("adding saved genres to df")
        dataframe = dataframe.join(existingGenres)
    
    #if there is no file, an empty column for genre still has to be added
    except:
        print("creating empty column for genre")
        dataframe["genre"] = np.nan

    empty_genres = pd.isnull(dataframe["genre"])    
    lookedUpGenres = dataframe[empty_genres]["bookId"].apply(get_genre)

    # add genres to dataframe    
    dataframe["genre"][empty_genres] = lookedUpGenres

    # make dataframe of newly added genres
    new_genres = dataframe[empty_genres]["genre"].to_frame()
    
    # add newly added genres to already existing file. If there is no file,
    # create a new one
    export = pd.concat([existingGenres, new_genres])
    export.to_pickle(SAVEDGENRES) 
       
    return dataframe
   