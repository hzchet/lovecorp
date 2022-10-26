from flask import Flask, render_template, url_for
from search import DbManager


app = Flask(__name__)
db_manager = DbManager(database_name='LOVECORP.db')


@app.route('/')
@app.route('/home')
def index():
    return render_template('index.html')


@app.route('/search')
def search(query: str):
    result = db_manager.search(query)
    return render_template('search.html', items=result, total=len(result))


@app.route('/FAQs')
def question():
    return render_template('FAQs.html')


@app.route('/about')
def about():
    return render_template('about.html')


if __name__ == '__main__':
    app.run(debug=True)
    db_manager.close_connection()
