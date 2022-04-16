#! python

## Anouk Luypaert



import pandas as pd
import sqlite3
import GRparsebooks as grp
import time
import logging
import os


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

dataloc = "E:/Dataprojects/"
dbLoc = os.path.join(dataloc,"goodreads.db")
			 

logging.basicConfig(filename=f'{dataloc}goodreads.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

#get all the books and add to a single dataframe
def booksToDF(shelves):
	books = pd.DataFrame()
	for shelf in shelves:
		start = time.time()
		books = books.append(grp.getBooksonShelf(shelf))
		end = time.time()
		runtime = end - start
		logging.info(f"Downloading books from shelf {shelf} took {runtime} seconds \n")   
	start2 = time.time()
	books = grp.addGenreToDF(books)
	end2 = time.time()
	runtime2 = end2 - start2
	logging.info(f"Updating genres took {runtime2} seconds \n")
	logging.info(f"result contains {len(books)} lines" )
	return books


#export all books from GR to sqlDB
def updateAll(db):
	books = booksToDF(SHELVES)
	starttime = time.time()
	conn = sqlite3.connect(db)
	books.to_sql("Goodreads", conn, if_exists="replace", index=False, dtype=datatypes)
	endtime = time.time()
	runtime = endtime - starttime
	logging.info(f"Updating db {db} took {runtime} seconds")
	return books
	
books = updateAll(dbLoc)


