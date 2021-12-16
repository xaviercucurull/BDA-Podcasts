Big Data Analytics Final Project - Spotify Podcasts Analysis
===========================================================================

This is the documentation of the code implemented for the Final Project.
In the :ref:`modindex` section the documentation for the different classes 
that have been created can be found.

`Click here <./../../../BDA_Final_Project.ipynb>`_ for the report and analysis Jupyter Notebook.


How to build the database
=========================
1. Navigate to the *Root* directory.
2. Install the necessary packages.(it is recommended to use a virtual environment).
3. Scrape Apple Podcasts to get a list of show names.
4. Build the database

.. code-block:: bat

   cd BDA-Podcasts
   pip install requirements.txt
   python apple_podcasts_scraper.python
   python build_database.py


