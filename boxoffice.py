import scrapy

class BoxOfficeSpider(scrapy.Spider):
    name = 'box_office'
    start_urls = ['https://www.boxofficemojo.com/chart/top_lifetime_gross/?area=XWW']

    def parse(self, response):
        # Sélectionnez toutes les lignes du tableau
        print (response.css('h1.mojo-gutter').get())
        
        for row in response.css('table.a-bordered.a-horizontal-stripes.a-size-base.a-span12.mojo-body-table.mojo-table-annotated > tbody > tr'):
            # Extrait les données de chaque cellule
            rank = row.css('td:nth-child(1)::text').get()
            title = row.css('td:nth-child(2) a::text').get()
            lifetime_gross = row.css('td:nth-child(3)::text').get()
            year = row.css('td:nth-child(4)::text').get()

            # Affiche les informations
            print(f'Rank: {rank}, Title: {title}, Lifetime Gross: {lifetime_gross}, Year: {year}')