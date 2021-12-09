"""
Spotify Podcasts Scraper

Author: Xavier Cucurull Salamero <xavier.cucurull@estudiantat.upc.edu>
Course: 2021/2022
"""

import numpy as np
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# https://developer.spotify.com/documentation/web-api/reference/#/operations/search
# https://medium.com/analytics-vidhya/visualizing-spotify-podcast-data-6dce49ca9a43
# https://medium.datadriveninvestor.com/speed-up-web-scraping-using-multiprocessing-in-python-af434ff310c5

class SpotifyScraper():
    """ Implementation of a Spotify Podcasts Scraper using Spotipy.
    
    Get a list of shows from a suggested show_name and search for
    episodes to obtain further information such as show release date, 
    last episode date and average duration of the episodes.
    
    
    Example:
        >>> sp = SpotifyScraper(SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET)
        >>> show = sp.search_shows('Gent de Merda')[0]
        >>> episodes = sp.get_show_episodes(show)
    """

    def __init__(self, client_id, client_secret):
        # Configure Spotify Client using spotipy
        auth_manager = SpotifyClientCredentials(client_id, client_secret)
        self.sp = spotipy.Spotify(auth_manager=auth_manager)
        
    def _get_date_dict(self, date_string):
        """ Get date dictionary from the date string obtained using
        Spotify's API.

        Args:
            date_string (str): date string from Spotify API.
                               Depending on the precision, it might be shown as "1981",  
                               "1981-12" or "1981-12-15"

        Returns:
            dict: date dict with the keys 'year', 'month', day'
        """
        date_list = date_string.split("-")

        if len(date_list) == 3:
            date =  {'year': int(date_list[0]), 'month': int(date_list[1]), 'day': int(date_list[2]})
        elif len(date_list) == 2:
            date =  {'year': int(date_list[0]), 'month': int(date_list[1]), 'day': None}
        else:
            date =  {'year': int(date_list[0]), 'month': None, 'day': None}

        return date

    def get_show_episodes(self, show_id, all=True):
        """ Get list of all episodes corresponding to a given show.

        Args:
            show_id (str): Spotify show ID
            all (bool, optional). Get all shows or only a set containing the first 
                                and the last episodes. Defaults to True.

        Returns:
            list: containing dicts
        """
        # instantiate conditional variables
        more_runs = 1                                          
        counter = 0  
        limit = 50
        offset = 0
        max_offset = 1000
        
        episodes = []
        
        # Get all episodes
        if all:
            # While loop to run with conditional variables
            while((offset <= max_offset-limit) & (counter <= more_runs)):           
                r = self.sp.show_episodes(show_id, limit=limit, offset=offset, market='ES')
                
                more_runs = (r['total'] // limit )      # how many more runs of "limit" are needed?       
                    
                counter += 1                            # increase conditional counter by 1
                offset = offset + limit                 # increase offset by "limit"
                
                episodes.extend([{'episode_name': i['name'], 'duration_min': i['duration_ms']/60000,
                                'languages': i['languages'], 'release_date': i['release_date'], 
                                'release_date_precision': i['release_date_precision']} for i in r['items']])
            
        # Get batches containing first and last episodes
        else:
            # Get last episodes
            r = self.sp.show_episodes(show_id, limit=limit, offset=offset, market='ES')
            episodes.extend([{'episode_name': i['name'], 'duration_min': i['duration_ms']/60000,
                                'languages': i['languages'], 'release_date': i['release_date'], 
                                'release_date_precision': i['release_date_precision']} for i in r['items']])
            
            # Get first episodes
            if r['total'] > limit:
                offset = r['total'] - limit
                r = self.sp.show_episodes(show_id, limit=limit, offset=offset, market='ES')
                episodes.extend([{'episode_name': i['name'], 'duration_min': i['duration_ms']/60000,
                                    'languages': i['languages'], 'release_date': i['release_date'], 
                                    'release_date_precision': i['release_date_precision']} for i in r['items']])
                
        return episodes

    def get_show_episodes_info(self, episodes):
        """ Get information about the average episode duration and the
        release date of the first and last episodes from a list of episodes.

        Args:
            episodes (list): list of episodes dicts

        Returns:
            dict: average_duration_min, first date, last date, date_precision
        """
        if len(episodes):
            average_duration_min = round(np.mean([e['duration_min'] for e in episodes]), 2)
            release_date = self._get_date_dict(episodes[-1]['release_date'])
            last_date = self._get_date_dict(episodes[0]['release_date'])
            date_precision = episodes[-1]['release_date_precision']
            
        else:
            average_duration_min = None
            release_date = {'year': None, 'month': None, 'day': None}
            last_date = {'year': None, 'month': None, 'day': None}
            date_precision = None
            
        episodes_info = {'average_duration_min': average_duration_min, 'release_date': release_date,
                        'last_date': last_date, 'date_precision': date_precision}
        
        return episodes_info

    def search_shows(self, show_name, limit=10):
        """ Search Spotify shows with the given name.
        
        The show_name is used as search query. A default limit of 10 is
        used because theoretically we only want 1 show per show_name query but
        we give room to more exploration.

        Args:
            self.sp (spotipy.Spotify): authorized spotipy.Spotify client
            show_name (stri): search query
            limit (int): limit of items

        Returns:
            list: of shows
        """
        r = self.sp.search(q=show_name, limit=limit, type='show', market='ES')
        
        shows = [{'name': i['name'], 'publisher': i['publisher'], 'explicit': i['explicit'],
                'media_type': i['media_type'], 'id': i['id'], 'languages': i['languages'],
                'description': i['description'], 'total_episodes': i['total_episodes']} for i in r['shows']['items']]
        
        return shows
    
    def process_show_name(self, show_name, max_shows=1):
        """ From a given show name (from Apple podcasts), search for shows in 
        Spotify and extract relevant information.

        Args:
            show_name (str): name of the show to search for
            max_shows (int, optional): maximum number of shows to obtain per search. Defaults to 1.

        Returns:
            list: of dicts containing show information
        """
        shows = self.search_shows(show_name)
                
        show_list = []
        
        for s in shows:
            # Only process shows in Spanish or Catalan
            # (mainly to reduce number of requests)
            if "es" in s['languages'] or "ca" in s['languages']:
                episodes = self.get_show_episodes(s['id'], all=False)
                episodes_info = self.get_show_episodes_info(episodes)
            
                s['average_duration_min'] = episodes_info['average_duration_min']
                s['release_date'] = episodes_info['release_date']
                s['last_date'] = episodes_info['last_date']
                s['date_precision'] = episodes_info['date_precision']
                
                show_list.append(s)
            
        return show_list
