from scrapy.crawler import CrawlerProcess
from boxofficemongo import BoxOfficeSpider 
import pymongo
import pandas as pd
import csv





#nous créons une fonction permettant de récupérer les données que l'on veut dans un dataframe
def create_df_movie() : 
    #on se connecte a la base de données box_office et a la collection movies et on supprime toutes les données qui s'y trouvent pour limiter les risques de doublons
    
    data = []
    client = pymongo.MongoClient('localhost', 27017)
    db = client['box_office']
    collection = db['movies']
    collection.delete_many({}) 
    #ici on ajoute les données au mongo
    process = CrawlerProcess()
    process.crawl(BoxOfficeSpider)
    process.start()
    #ici on cherche toutes les informations présentes dans le mongo
    movies = collection.find()
    for movie in movies:
        data.append([
            movie.get('rank'),
            movie.get('title'),
            movie.get('lifetime_gross'),
            movie.get('year'),
            movie.get('summary'),
            movie.get('Budget'),
            movie.get('Earliest Release Date', ''),
            movie.get('MPAA', ''),
            movie.get('Running Time', ''),
            movie.get('Genres', ''),
            movie.get('Original Language:', ''),
            movie.get('Director:', ''),
            movie.get('Producer:', ''),
            movie.get('Writer:', ''),
            movie.get('Production Co:', '')
        ])

    #ici on va ajouter toutes les données que l'on a pu trouver dans un dataframe on va ensuite supprimer tous les doublons qui existent
    #supprimer les doublons n'est normalement pas nécessaire si le code que l'on a fait pour scrapper la page est correct
    #cependant on est jamais trop prudent (on devrait avoir 199 lignes si tout va bien)
    df = pd.DataFrame(data, columns=['Rank', 'Title', 'Lifetime Gross', 'Year', 'Summary','Budget', 'Release Date', 'MPAA', 'Duree', 'Genres', 'Langue_origine', 'Realisateur', 'Producteurs', 'scenariste', 'production'])
    df.drop_duplicates(inplace=True)
    df.drop_duplicates(subset = ['Rank'], inplace = True)
    #df.to_csv('moviesv2.csv', index=False, encoding='utf-8') #juste pour tester et vérifier que les valeurs existent (plus facile a visualiser sur un csv que sur un df.head dans le terminal)
    #print(df.head)
    return df