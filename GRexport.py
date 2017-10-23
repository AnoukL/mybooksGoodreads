#! python

## Anouk Luypaert
## 8/2017

SHELVES = ["currently-reading", "read", "to-read"]
TITLES = {"bookID": "A", "Title": "b", "author": "C", "ISBN": "D", "rating": "E",
          "avgRating": "F", "Publisher": "G", "Pages": "H", "YearPublished": "I", 
          "read_at": "J", "DateAdded": "K", "bookshelves": "L", "owned": "M",
          "genre": "N"}
datatypes = {"bookId": "int", 
             "title" : "string", 
             "author" : "string", 
             "publisher" : "string",
             "isbn13" : "string", 
             "published": "int", 
             "pages" : "int",
             "owned": "int",
             "dateAdded": "blob",
             "dateRead": "blob",
             "bookshelves" : "string", 
             "avgRating": "float", 
             "myRating" : "float", 
             "read": "int",
             "currently-reading": "int",
             "genre": "string"}
dbLoc = "D:\projects\goodreads.db"

			 
import pandas as pd
import sqlite3
import GRparsebooks as grp
import time

#get all the books and add to a single dataframe
def booksToDF(shelves):
    books = pd.DataFrame()
    for shelf in shelves:
        start = time.time()
        books = books.append(grp.getBooksonShelf(shelf))
        end = time.time()
        runtime = end - start
        print("Getting the books {} took {} seconds \n".format(shelf, runtime))   
    start2 = time.time()
    books = grp.addGenreToDF(books)
    end2 = time.time()
    runtime2 = end2 - start2
    print("Updating genres took {} seconds \n".format(runtime2))
    return books


#export all books from GR to sqlDB
def updateAll(db):
    books = booksToDF(SHELVES)
    starttime = time.time()
    conn = sqlite3.connect(db)
    books.to_sql("Goodreads", conn, if_exists="replace", index=False, dtype=datatypes)
    endtime = time.time()
    runtime = endtime - starttime
    print("Updating db {} took {} seconds".format(db, runtime))
    
updateAll(dbLoc)


