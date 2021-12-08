"""
Big Data Analytics (BDA)
Master in Artificial Intelligence (UPC/URV)

Final Project
Author: Xavier Cucurull Salamero <xavier.cucurull@estudiantat.upc.edu>
Course: 2021/2022
"""
import time
import csv
import json
import pymongo
from datetime import datetime
from multiprocessing import Pool
import config
from spotify_podcasts_scraper import SpotifyScraper

        
def single_process_batch(b):
    s_list = []
    
    for s_name in b:
        process_show_name(s_name)
            
    return s_list
     

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

        # open file in read mode
        # GET NUMBER OF LINES (PODCASTS)
        # with open(r"E:\demos\files\read_demo.txt", 'r') as fp:
        #     for count, line in enumerate(fp):
        #         pass
        # print('Total Lines', count + 1)

        # For each Apple Podcast, search for podcast information in Spotify
        # Process list in batches
        
        batch_size = config.BATCH_SIZE
        names_batch = []
        count = 0
        
        total_batches = 50       # TODO remove
        
        for row in reader:
            if total_batches:   # TODO remove
                # Process batch
                if len(names_batch) >= batch_size:
                    # Print debug info
                    print(f'[{datetime.now().strftime("%H:%M:%S")}] Processing batch. {total_batches-1} remaining...')
                    
                    # Process Show Names as a single process
                    # shows_list = []
                    # for show_name in names_batch:
                    #     shows_list.extend(sp.process_show_name(show_name))
                    
                    # Process Show Names using multiprocessing
                    with Pool(config.POOL_PROCESSES) as p:
                        batch_shows = p.map(sp.process_show_name, names_batch, config.MAX_SHOWS_PER_SEARCH)
                        shows_list = [item for sublist in batch_shows for item in sublist]      # flatten list
                        
                    # Insert shows into database
                    if len(shows_list):
                        # Insert many bypassing duplicates (https://stackoverflow.com/a/63655698)
                        try:
                            # inserts new documents even on error
                            db.podcasts.insert_many(shows_list, ordered=False, bypass_document_validation=True)
                        except pymongo.errors.BulkWriteError as e:
                            # TODO: could it be updated with genre?
                            panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
                            if len(panic_list) > 0:
                                print(f"these are not duplicate errors {panic_list}")
                        
                    # Print debug info
                    print(f'[{datetime.now().strftime("%H:%M:%S")}] {len(shows_list)} shows retrieved from batch of {len(names_batch)}\n')
                    
                    # Reset batch
                    names_batch = []
                    total_batches -= 1      # TODO remove

                # Add row to batch
                names_batch.append(row[0])
                        
        # Process last batch
        if names_batch:   
            # single_process_batch(batch)
            with Pool(config.POOL_PROCESSES) as p:
                batch_shows = p.map(sp.process_show_name, names_batch, config.MAX_SHOWS_PER_SEARCH)
                shows_list = [item for sublist in batch_shows for item in sublist]      # flatten list
                
            # Insert shows into database
            if len(shows_list):
                # Insert many bypassing duplicates (https://stackoverflow.com/a/63655698)
                try:
                    # inserts new documents even on error
                    db.podcasts.insert_many(shows_list, ordered=False, bypass_document_validation=True)
                except pymongo.errors.BulkWriteError as e:
                    panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
                    if len(panic_list) > 0:
                        print(f"these are not duplicate errors {panic_list}")
                    
            # Print debug info
            print(f'[{datetime.now().strftime("%H:%M:%S")}] {len(shows_list)} shows retrieved from batch of {len(names_batch)}\n')
        
    end_time = time.time() - t0
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Process finished in {time.strftime("%Hhour %Mmin", time.gmtime(end_time))}')    
    
# with open(config.PODCASTS_FILE, 'r', encoding='utf8') as f:
#     reader = csv.reader(f, delimiter=';')
#     header = next(reader)   # skip header
    
#     c = 0
#     for count, line in enumerate(f):
#         c += 1

# sp = SpotifyScraper(config.SPOTIPY_CLIENT_ID, config.SPOTIPY_CLIENT_SECRET)
# show = sp.search_shows('Gent de Merda', 1)

###################################################################################################################
# NOTES:
# Apple podcast list: different shows with "same" name
# Different shows on different categories/genres. Not reliable, not practical to use for spotify dataset
###################################################################################################################

# Combine two collections
# database.podcasts.aggregate([{ "$lookup": {"from": "spotify", "localField": "name", "foreignField": "name", "as": "genres"}}])
