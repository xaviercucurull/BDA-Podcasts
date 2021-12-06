import requests
import time
from bs4 import BeautifulSoup
import time
from datetime import datetime
import csv

OUT_FILE = "data/apple_podcasts.csv"
    
def get_genres():
    """ Get all genres from the Apple Podcasts page.

    Returns:
        dict: containing genre urls indexed by genre name
    """
    
    url = 'https://podcasts.apple.com/es/genre/podcasts/id26'   # Genres in Spanish
    response = requests.get(url)

    genres_dict = {}

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        genre_nav = soup.find(id='genre-nav')
        genres = genre_nav.find_all(class_="top-level-genre")

        for g in genres:
            genres_dict[g.get_text()] = g['href']

    return genres_dict


def get_podcasts_from_page(genre_url, letter, page):
    """ Get the title of all podcasts in a page.

    Args:
        genre_url (str): podcast genre url to parse
        letter (str): letter
        page (int): number of the page

    Returns:
        tuple (str, int): list of podcasts, last page of genre
    """
    podcasts_list = []

    url = f'{genre_url}?letter={letter}&page={page}'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selected_genre = soup.find(id='selectedgenre')

    podcasts = selected_genre.find(id='selectedcontent').find_all('li')

    try:
        last_page = int(selected_genre.find(class_='list paginate').find_all('li')[-2].get_text())
    except:
        last_page = 1

    podcasts_list.extend([p.get_text().rstrip() for p in podcasts])

    return podcasts_list, last_page


def get_podcasts_from_genre(genre_url, genre="", debug=False):
    """ Given a genre url, obtain the title of all the podcasts.
    
    Iterates over all the pages corresponding to each of the possible letters
    and find the title of all podcasts in each page.
    
    Args:
        genre_url (str): url of the genre podcast page.
        genre (str, optional): Name of the genre. Defaults to "".
        debug (bool, optional): Print debug information. Defaults to None.

    Returns:
        list: containing titles of podcasts
    """
    genre_podcasts = []

    genre_url = 'https://podcasts.apple.com/es/genre/podcasts-arte/id1301'
    response = requests.get(genre_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    selected_genre = soup.find(id='selectedgenre')
    letters = [l.get_text() for l in selected_genre.find(class_='list alpha').find_all('li')]

    if debug:
        print(f'[{datetime.now().strftime("%H:%M:%S")}] Retrieving "{genre}" podcasts...')
        
    for l in letters:
        p = 1
        last_page = 1

        old_len = len(genre_podcasts)

        while p <= last_page:
            new_podcasts, last_page = get_podcasts_from_page(genre_url, l, p)
            p += 1

            genre_podcasts.extend(new_podcasts)

        if debug:
            print(f'\tLetter {l}: {len(genre_podcasts) - old_len} podcasts ({p-1} pages)')
    
    return genre_podcasts


if __name__ == "__main__":    
    # Write header of output file
    with open(OUT_FILE, 'w') as f:
        write = csv.writer(f, delimiter=';')
        write.writerow(['Title', 'Genre'])
    
    # Get all Podcast genres        
    genres = get_genres()

    # For each genre, get all Podcast titles
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Starting podcast scraping...')
    t0 = time.time()
    if len(genres):
        for genre_name, genre_url in genres.items():
            podcasts = get_podcasts_from_genre(genre_url, genre_name, debug=True)
        
        # Save to output file
        with open(OUT_FILE, 'a') as f:
            write = csv.writer(f, delimiter=';')
            write.writerows([[p, genre_name] for p in podcasts])

    else:
        print('ERROR: Could not find genres!')
    
    end_time = time.time() - t0
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Processed finished in {time.strftime("%Hhour %Mmin", time.gmtime(end_time))}')