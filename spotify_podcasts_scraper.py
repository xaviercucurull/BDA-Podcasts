import csv
import time
import numpy as np
import sys
from multiprocessing import Pool
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import config
import json

import pandas as pd

# https://developer.spotify.com/documentation/web-api/reference/#/operations/search
# https://medium.com/analytics-vidhya/visualizing-spotify-podcast-data-6dce49ca9a43
# https://medium.datadriveninvestor.com/speed-up-web-scraping-using-multiprocessing-in-python-af434ff310c5


def get_show_episodes(spotify_client, show_id):
    """ Get list of all episodes corresponding to a given show.

    Args:
        spotify_client (spotipy.Spotify): authorized spotipy.Spotify client
        show_id (str): Spotify show ID

    Returns:
        [type]: [description]
    """
    # instantiate conditional variables
    more_runs = 1                                          
    counter = 0  
    limit = 50
    offset = 0
    max_offset = 1000
    
    episodes = []
    
    while((offset <= max_offset-limit) & (counter <= more_runs)):           # while loop to run with conditional variables
        r = spotify_client.show_episodes(show_id, limit=limit, offset=offset, market='ES')
        
        more_runs = (r['total'] // 50 )            # how many more runs of 50 are needed?       
            
        counter += 1                                                    # increase conditional counter by 1
        offset = offset + 50                                            # increase offset by 50
        
        episodes.extend([{'episode_name': i['name'], 'duration_min': i['duration_ms']/60000,
                          'languages': i['languages'], 'release_date': i['release_date'], 
                          'release_date_precision': i['release_date_precision']} for i in r['items']])
        
    return episodes

def get_show_episodes_info(episodes):
    """ Get information about the average episode duration and the
    release date of the first and last episodes from a list of episodes.

    Args:
        episodes (list): list of episodes dicts

    Returns:
        dict: average_duration_min, first date, last date, date_precision
    """
    
    average_duration_min = np.mean([e['duration_min'] for e in episodes])
    release_date = episodes[-1]['release_date']
    last_date = episodes[0]['release_date']
    date_precision = episodes[0]['release_date_precision']
        
    episodes_info = {'average_duration_min': average_duration_min, 'release_date': release_date,
                     'last_date': last_date, 'date_precision': date_precision}
    
    return episodes_info

def search_shows(spotify_client, show_name, limit=10):
    """ Search Spotify shows with the given name.
    
    The show_name is used as search query. A default limit of 10 is
    used because theoretically we only want 1 show per show_name query but
    we give room to more exploration.

    Args:
        spotify_client (spotipy.Spotify): authorized spotipy.Spotify client
        show_name (stri): search query
        limit (int): limit of items

    Returns:
        list: of shows
    """
    r = spotify_client.search(q=show_name, limit=limit, type='show', market='ES')
    
    shows = [{'name': i['name'], 'publisher': i['publisher'],
              'media_type': i['media_type'], 'id': i['id'], 'languages': i['languages'],
              'total_episodes': i['total_episodes']} for i in r['shows']['items']]
    
    return shows

def process_show_name(show_name):
    """ From a given show name (from Apple podcasts),
    search for shows in Spotify and extract relevant
    information.

    Args:
        show_name (str): name of the show to search for

    Returns:
        list: of dicts containing show information
    """
    shows = search_shows(sp, show_name, config.MAX_SHOWS_PER_SEARCH)
    
    show_list = []
    
    for s in shows:
        episodes = get_show_episodes(sp, s['id'])
        episodes_info = get_show_episodes_info(episodes)
    
        s['average_duration_min'] = episodes_info['average_duration_min']
        s['release_date'] = episodes_info['release_date']
        s['last_date'] = episodes_info['last_date']
        s['date_precision'] = episodes_info['date_precision']
        
        show_list.append(s)
        
    return show_list
            
def single_process_batch(b):
    s_list = []
    
    for s_name in b:
        process_show_name(s_name, s_list)
            
    return s_list
                
##########################################################

if __name__ == "__main__":    

    # Configure Spotify Client using spotipy
    auth_manager = SpotifyClientCredentials(config.SPOTIPY_CLIENT_ID, config.SPOTIPY_CLIENT_SECRET)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    
    t0 = time.time()
    total_shows = []
    
    # Load list of Apple Podcasts
    with open(config.PODCASTS_FILE, 'r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)   # skip header

        # For each Apple Podcast, search for podcast information in Spotify
        # Process list in batches
        
        batch_size = 50
        batch = []
        count = 0
        
        total_batches = 2       # TODO remove
        
        for row in reader:
            if total_batches:   # TODO remove
                # Process batch
                if count >= batch_size:
                    # single_process_batch(batch)
                    with Pool(10) as p:
                        batch_shows = p.map(process_show_name, batch)
                        flat_list = [item for sublist in batch_shows for item in sublist]
                        total_shows.extend(flat_list)

                    batch = []
                    count = 0
                    total_batches -= 1      # TODO remove

                # Add row to batch
                batch.append(row)
                count += 1
                        
        # Process last batch
        if batch:   
            # single_process_batch(batch)
            with Pool(10) as p:
                batch_shows = p.map(process_show_name, batch)
                flat_list = [item for sublist in batch_shows for item in sublist]
                total_shows.extend(flat_list)
        
    end_time = time.time() - t0
    print(f'Processed {len(total_shows)} in {end_time:.2f} seconds')   

    with open('data/out_test.json', 'w', encoding='utf8') as f:
        json.dump(total_shows , f)