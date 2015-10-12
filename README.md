# Scraping-Yelp
Scrape Yelp for a city/region

The code is quite simple but relies on a crucial ingredient: a grid of points over your city of interest. Yelp's API
allows one to search within a radius of a point so we created a dense grid of points for San Diego and collected
most of the busiensses Yelp has on its site for that San Diego.

Once the business URLs are acquired, we just loop through them, scraping its 
a) priceRange
b) number of reviews
c) rating
d) ALL of the reviews themselves (including text)

Given the above data, we can extract all of the unique users and scrape ALL of their data, including:
a) location
b) elite status
c) number of friends and their ID
d) number of reviews
e) review dates

NOTE: recall that Yelp loathes scrapers and so the dataset collected through this is technically proprietary info. 
I use this for research, NOT for self-enrichment. 
