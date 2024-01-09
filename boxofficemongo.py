import pandas as pd
import pymongo
import json
import scrapy
import re

class BoxOfficeSpider(scrapy.Spider):
    name = 'box_office'
    start_urls = ['https://www.boxofficemojo.com/chart/top_lifetime_gross/?area=XWW']
    
    def __init__(self):
        #ici on va se connecter a mongodb (sinon on a pas de base de données)
        #on va aussi déclarer que l'on souhaite accéder a la base de données 'box_office' et enregistrer dans 'movies'
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['box_office']
        self.collection = self.db['movies']
    def parse(self, response):
        #on va aller sur chaque ligne du tableau énnoncé ci dessous
        for row in response.css('table.mojo-body-table tr'):
            # la méthode child permet enfait de regarder chaque attribut de notre ligne et récupérer chacun de ces attributs
            #dans le premier on va enfait récupérer l'url attachée au nom du film (grace au href qui est une hyper reference)
            #dans le deuxième on va récupérer le rang du film
            #dans le troisième on va récupérer le titre du film (le nom du film est un texte avec une url que l'on récupère plus tot)
            #ensuite on récupère les revenus
            #et enfin on récupère l'année de sortie du film
            link = row.css('td:nth-child(2) a::attr(href)').get() #le a::attr(href) indique que l'on souhaite récupérer l'attribut donc le lien
            rank = row.css('td:nth-child(1)::text').get()
            title = row.css('td:nth-child(2) a::text').get()
            lifetime_gross = row.css('td:nth-child(3)::text').get()
            year = row.css('td:nth-child(4) a::text').get()

            if link:
                #pour appeler la méthode qui permet d'aller sur chaque lien de film
                full_url = response.urljoin(link)
                meta = {'rank': rank, 'title': title, 'lifetime_gross': lifetime_gross, 'year': year}
                yield scrapy.Request(full_url, callback=self.parse_movie, meta=meta)
                
            # En plus de sauvegarder dans MongoDB, on planifie une nouvelle requête vers Rotten Tomatoes
        

    def construct_rotten_tomatoes_url(self, title):
        #pour créer l'url du site rotten tomatoes
        url_title = title.replace(' ', '_').replace('-', '').replace(':', '').replace('&', 'and').replace('.','').replace(',','').lower()
        url_title = re.sub('_+', '_', url_title)
        return f'https://www.rottentomatoes.com/m/{url_title}'

    def new_parse(self, response):
        movie_data = response.meta.copy()
        #on récupère les données créées plus tot afin d'y ajouter les données supplémentaires du site rotten tomatoes
        rotten_tomatoes_data = {}

        info_containers = response.css('li.info-item')
        #ici on récupère tous les items sur la page du film concerné (on triera ce que l'on souhaite garder plus tard)
        #on récupère donc la clé (indispensable pour pouvoir associer la bonne valeur a la bonne clée)
        #on nettoie ensuite la valeur (on ne va garder que le texte important)
        for container in info_containers:
            key = container.css('b::text').get(default='').strip()
            values = container.css('span a::text').getall()
            values = [value.strip() for value in values if value.strip()]
            value = container.xpath('.//span[not(@class)]/text()').get()

            #on va uniquement garder la ou les valeurs et on va coller les valeurs si il y en a plusieurs et associer la valeur a la bonne clé
            if key and (values or value):
                values_text = ', '.join(values) if values else value.strip()
                #print(key)
                #print(values_text)
                rotten_tomatoes_data[key] = values_text
                #if key in ("Rating", "Director", "Producer", "Writer", "Production Co"):
                #    print("1")
                #    rotten_tomatoes_data[key] = values_text
                #    print(rotten_tomatoes_data[key])
                    
                    
        #print(rotten_tomatoes_data)
        # ici on ajoute les données du film avec les informations que l'on vient de récupérer
        movie_data.pop('_id', None)
        movie_data['rotten_tomatoes_info'] = rotten_tomatoes_data
        #print(movie_data)
        # nettoyage des données
        cleaned_movie_data = self.clean_data(movie_data)
        #print(cleaned_movie_data)
        #ici on va ajouter les données dans le mongo
        #la méthode update_one permet d'ajouter les données en faisant attention a ce que les données que l'on ajoute n'existent pas déja
        #si elles existent on les modifies sinon on les crées
        self.collection.update_one({'title': cleaned_movie_data['title']}, {'$set': cleaned_movie_data}, upsert=True)
    def parse_movie(self, response):
        #on rerécupère les infos du film que l'on est entrain d'observer
        #ici on crée un dictionnaire 'additional_info' qui contiendra tout ce que l'on récupère sur la page officielle du film
        #ce dictionnaire sera supprimé plus tard pour tout mettre dans le mongo
        movie_data = {
            'rank': response.meta['rank'],
            'title': response.meta['title'],
            'lifetime_gross': response.meta['lifetime_gross'],
            'year': response.meta['year'],
            'summary': response.css('span.a-size-medium::text').get(),
            'additional_info': {}
        }

        info_containers = response.css('div.a-section.a-spacing-none')
        #on récupère toutes les infos disponibles (on trie après)
        #on récupère donc la clé ainsi que la valeur associée (par exemple "Budget : $500,000,000")
        for container in info_containers:
            key = container.css('span::text').get()
            value = container.xpath('normalize-space(./span/following-sibling::span/text())').get()
            if not value:
                value = container.css('span.money::text').get()
            #on trie ce que l'on souhaite garder (comme on a une sorte de tableau on va vérifier que la clé est une des suivantes)
            #on va ensuite vérifier si la clé est associée a une valeur
            if key in ['Domestic Distributor', 'Budget', 'Earliest Release Date', 'MPAA', 'Running Time', 'Genres'] and value:

                cleaned_key = ' '.join(key.split())
                movie_data['additional_info'][cleaned_key] = value.strip()

        # on met les données obtenues dans le mongoDB en faisant attention a ce qu'elle n'existe pas déja
        # cette méthode est également utilisée plus tot
        cleaned_movie_data = self.clean_data(movie_data)
        self.collection.update_one({'title': cleaned_movie_data['title']}, {'$set': cleaned_movie_data}, upsert=True)
        #print(self.collection)
        
        #on construit l'url du site rotten tomatoes qui est associée au film que l'on étudie
        #les url des films sont toujours constituées de la meme facon il est donc facile de la créer
        rotten_tomatoes_url = self.construct_rotten_tomatoes_url(response.meta['title'])
        yield scrapy.Request(rotten_tomatoes_url, callback=self.new_parse, meta=cleaned_movie_data)
        
        
    #ici on va créer une fonction pour nettoyer les données que l'on a afin de les ajouter au mongo
    
    def clean_data(self, movie_data): 
        #on définit les données dont on veut se séparer
        donnees_inutiles = ['depth', 'download_timeout', 'download_slot', 'download_latency']
        #lorseque l'on crée les données depuis le site rottentomatoes on crée un dictionnaire pour tout stocker
        #on va donc supprimer le dictionnaire et tout ajouter dans la base de données
        if 'rotten_tomatoes_info' in movie_data:
            for key, value in movie_data['rotten_tomatoes_info'].items():
                movie_data[key] = value
            del movie_data['rotten_tomatoes_info']
        #on fait de meme avec les informations que l'on récupère dans la fonction parse_movie
        if 'additional_info' in movie_data:
            for key, value in movie_data['additional_info'].items():
                movie_data[key] = value
            del movie_data['additional_info']
        #on fait en sorte que les valeurs soient faciles à manipuler (toutes en minuscule etc...)
        for key, value in movie_data.items():
            movie_data[key] = ' '.join(str(value).split()).lower().strip(':')
        #on retire les données inutiles
        for donnee in donnees_inutiles : 
            movie_data.pop(donnee,None)
        
        return movie_data
    def close(self, reason):
        # Fermeture de la connexion MongoDB
        #for doc in self.collection.find():
        #    print(doc)
        self.client.close()