from flask import Flask, render_template_string

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hei maailma! Tämä on Flask-sovellus.'

@app.route('/tervehdys/<nimi>')
def tervehdys(nimi):
    return render_template_string('<h1>Terve, {{ nimi }}!</h1>', nimi=nimi)

if __name__ == '__main__':
    app.run(debug=True)
