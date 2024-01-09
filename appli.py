import pandas as pd
from donnees_appli import create_df_movie
from flask import Flask, render_template, send_file, url_for
from io import BytesIO
import matplotlib.pyplot as plt
data_movies = create_df_movie()
#print(data_movies.head)
df = pd.DataFrame({
    'x': range(1, 11),
    'y': [1, 2, 3, 4, 0, 4, 3, 2, 1, 0]
})
app = Flask(__name__)

@app.route('/')
def index():
    graph_url = url_for('plot_png')
    return render_template('index.html', graph_url=graph_url)
@app.route('/page2')
def page2():
    return render_template('page2.html')
@app.route('/plot.png')
def plot_png():
    fig = create_figure()
    img = BytesIO()
    fig.savefig(img)
    img.seek(0)
    return send_file(img, mimetype='png')
def create_figure():
    # Cette fonction crée le graphique Matplotlib
    fig, axis = plt.subplots(1, 1)
    axis.plot(df['x'], df['y'])
    return fig
if __name__ == '__main__':
    app.run(debug=True)