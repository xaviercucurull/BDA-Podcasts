"""
Big Data Analytics (BDA)
Master in Artificial Intelligence (UPC/URV)

Final Project
Author: Xavier Cucurull Salamero <xavier.cucurull@estudiantat.upc.edu>
Course: 2021/2022
"""
import csv
import json
import time
from datetime import datetime
from multiprocessing import Pool

import numpy as np
import pymongo

import config
from spotify_podcasts_scraper import SpotifyScraper


def insert_list_into_database(dict_list, database):
    """ Insert list of dicts into a MongoDB database.
    If a duplicate element tries to be inserted it is ignored
    and the insertion continues.

    Args:
        dict_list (list): list of dictionaries
        database (MongoDB database): MongoDB database
    """
    if len(dict_list):
        # Insert many bypassing duplicates (https://stackoverflow.com/a/63655698)
        try:
            # inserts new documents even on error
            database.podcasts.insert_many(dict_list, ordered=False, bypass_document_validation=True)
        except pymongo.errors.BulkWriteError as e:
            panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
            if len(panic_list) > 0:
                print(f"these are not duplicate errors {panic_list}")


if __name__ == "__main__":    

    # Configure Spotify Client using spotipy
    sp = SpotifyScraper(config.SPOTIPY_CLIENT_ID, config.SPOTIPY_CLIENT_SECRET)
    
    # Get MongoDB database
    client = pymongo.MongoClient('localhost', 27017, username='mongoadmin', password='pass1234')
    db = client['finalproject']
    
    # Create Podcasts collection with id as unique index
    db.podcasts.create_index([('id', pymongo.ASCENDING)], unique=True)

    t0 = time.time()
    total_shows = []
    total_shows = 0
    
    # Load list of Apple Podcasts
    with open(config.PODCASTS_FILE, 'r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)   # skip header

        # For each Apple Podcast, search for podcast information in Spotify
        # Process list in batches
        
        batch_size = config.BATCH_SIZE
        names_batch = []
        count_batches = 0
        total_batches = np.inf      # Process ALL batches
        
        for row in reader:
            if total_batches:
                # Process batch
                if len(names_batch) >= batch_size:
                    # Print debug info
                    count_batches += 1
                    print(f'[{datetime.now().strftime("%H:%M:%S")}] Processing batch {count_batches}. {total_batches-1} remaining...')
                    
                    # Process Show Names using multiprocessing
                    with Pool(config.POOL_PROCESSES) as p:
                        batch_shows = p.map(sp.process_show_name, names_batch, config.MAX_SHOWS_PER_SEARCH)
                        shows_list = [item for sublist in batch_shows for item in sublist]      # flatten list
                        
                    # Insert shows into database
                    insert_list_into_database(db, shows_list)
                        
                    # Print debug info
                    print(f'[{datetime.now().strftime("%H:%M:%S")}] {len(shows_list)} shows retrieved from batch of {len(names_batch)}\n')
                    
                    # Reset batch
                    names_batch = []
                    total_batches -= 1

                # Add row to batch
                names_batch.append(row[0])
                        
        # Process last batch
        if names_batch:
            # Print debug info
            print(f'[{datetime.now().strftime("%H:%M:%S")}] Processing last batch...')

            # Process Show Names using multiprocessing
            with Pool(config.POOL_PROCESSES) as p:
                batch_shows = p.map(sp.process_show_name, names_batch, config.MAX_SHOWS_PER_SEARCH)
                shows_list = [item for sublist in batch_shows for item in sublist]      # flatten list
                
            # Insert shows into database
            insert_list_into_database(db, shows_list)
                    
            # Print debug info
            print(f'[{datetime.now().strftime("%H:%M:%S")}] {len(shows_list)} shows retrieved from batch of {len(names_batch)}\n')
        
    end_time = time.time() - t0
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Process finished in {time.strftime("%Hhour %Mmin", time.gmtime(end_time))}')    
    