"""
Big Data Analytics (BDA)
Master in Artificial Intelligence (UPC/URV)

Final Project
Author: Xavier Cucurull Salamero <xavier.cucurull@estudiantat.upc.edu>
Course: 2021/2022
"""
import time
import csv
from datetime import datetime
from multiprocessing import Pool
import config
from spotify_podcasts_scraper import SpotifyScraper

import json

            
def single_process_batch(b):
    s_list = []
    
    for s_name in b:
        process_show_name(s_name, s_list)
            
    return s_list

def json_dump(data):
    with open('data/out_test.json', 'a') as f:
        json.dump(data , f)
        

if __name__ == "__main__":    

    # Configure Spotify Client using spotipy
    sp = SpotifyScraper(config.SPOTIPY_CLIENT_ID, config.SPOTIPY_CLIENT_SECRET)
    
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
        batch = []
        count = 0
        
        total_batches = 100       # TODO remove
        
        for row in reader:
            if total_batches:   # TODO remove
                # Process batch
                if count >= batch_size:
                    print(f'[{datetime.now().strftime("%H:%M:%S")}] Processing batch. {total_batches-1} remaining...')
                    # single_process_batch(batch)
                    with Pool(config.POOL_PROCESSES) as p:
                        batch_shows = p.map(sp.process_show_name, batch, config.MAX_SHOWS_PER_SEARCH)
                        flat_list = [item for sublist in batch_shows for item in sublist]
                        #total_shows.extend(flat_list)
                        total_shows += len(flat_list)
                        json_dump(flat_list)

                    print(f'[{datetime.now().strftime("%H:%M:%S")}] {len(flat_list)} shows retrieved from batch of {batch_size}\n')
                    
                    batch = []
                    count = 0
                    total_batches -= 1      # TODO remove

                # Add row to batch
                batch.append(row)
                count += 1
                        
        # Process last batch
        if batch:   
            # single_process_batch(batch)
            with Pool(config.POOL_PROCESSES) as p:
                batch_shows = p.map(sp.process_show_name, batch, config.MAX_SHOWS_PER_SEARCH)
                flat_list = [item for sublist in batch_shows for item in sublist]
                #total_shows.extend(flat_list)
                total_shows += len(flat_list)
        
    end_time = time.time() - t0
    print(f'Processed in {end_time:.2f} seconds') 