#!/usr/bin/env python3
"""Explore a dataset of near-Earth objects and their close approaches to Earth.
"""
import argparse
import json
import datetime
import sys
import requests
import mysql.connector
from urllib.parse import urljoin

from database import NEODatabase

BASE_URL = 'https://api.nasa.gov/neo/rest/v1/feed/today?detailed=true&api_key=DEMO_KEY'

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="neodb"
)

mycursor = mydb.cursor()

def get_feed():

    TODAY_DATE = datetime.today().strftime('%Y-%m-%d')
    response = requests.get(BASE_URL)
    raw_data = response.json()
    raw_objects = raw_data['near_earth_objects'][TODAY_DATE]
    result = [NeoDatabase.get_neo(raw_neo) for raw_neo in raw_objects]

    return result

def load_neos(asteroids):
    for date in dates:
        for ids in range(len(asteroids['near_earth_objects'][date])):
            if asteroids['near_earth_objects'][date][ids]['is_potentially_hazardous_asteroid'] == 'true':
                id = asteroids['near_earth_objects'][date][ids]['id']
                name = asteroids['near_earth_objects'][date][ids]['name']
                close_approach_date = asteroids['near_earth_objects'][date][ids]['close_approach_data'][close_apprach_date]
                miss_distance = asteroids['near_earth_objects'][date][ids]['close_approach_data'][miss_distance][miles]

        mycursor.executemany("""
            CREATE TABLE neo_approaches (
                neows_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                created_at TIMESTAMP NOT NULL,
                id BIGINT NOT NULL,
                name VARCHAR(50),
                close_approach_date TIMESTAMP,
                miss_distance DECIMAL(10,5)
                PRIMARY KEY (neows_id)
            );
                             
            INSERT INTO neo_approaches (
                    created_at,
                    id, 
                    name,
                    close_approach_date,
                    miss_distance
                    ) 
            VALUES(%s, %s, %s, %s);
                              """,
                             [(
                                 date,
                                 id,
                                 name,
                                 close_apprach_date,
                                 miss_distance
                             )]);
        mycursor.commit()
        mycursor.close()
    return ()

def main():
    """Run the main script."""
    args = get_feed()

    # Extract data from the data files into structured Python objects.
    database = NEODatabase(load_neos(args))

if __name__ == '__main__':
    main()
