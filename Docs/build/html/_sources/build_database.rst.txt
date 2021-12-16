Database Builder
================

This is the main script to build the Spotify Podcast Database.

It reads the apple_podcasts.csv file in batches and processes each of the retrieved Podcast name using
SpotifyScraper. SpotifyScraper searches for a show using the given text query and then, if the language
is "ca" or "es", retrieves a list of maximum 100 episodes and returns information about the release and last
episode dates and the average duration in minutes.

.. automodule:: build_database
   :members:
   :undoc-members:
   :show-inheritance:
