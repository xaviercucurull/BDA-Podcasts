# BDA-Podcasts
MAI - Big Data Analytics Final Project

Podcasts analysis from a database created using the Spotify developer API

<!-- Open in colab -->
<a href="https://colab.research.google.com/github/xaviercucurull/BDA-Podcasts/blob/main/BDA_Final_Project.ipynb">
    <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Colab" height="20">
</a>

## How to build the database

1. Navigate to the *Root* directory.
2. Install the necessary packages.(it is recommended to use a virtual environment).
3. Scrape Apple Podcasts to get a list of show names.
4. Build the database

```
   cd BDA-Podcasts
   pip install requirements.txt
   python apple_podcasts_scraper.python
   python build_database.py
```

## Database download
The Podcasts Database constructed for this project can also be downloaded [here](https://drive.google.com/file/d/1sX5JTAqtWuOeFYqJr-ih8VvcjP6tFRPC/view?usp=sharing).
