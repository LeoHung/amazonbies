from flask import request
from flask import render_template

from amazonbies import app
from datetime import datetime

@app.route('/q1', methods=['GET'])
def q1():
    """
    example query:
    /q1?key=20630300497055296189489132603428150008912572451445788755351067609550255501160184017902946173672156459
    """

    key = request.args.get('key')
    if key == None:
        abort(403)
    try:
        key = int(key)
    except:
        abort(403)

    public_key = app.config.get('PUBLIC_KEY')
    number = key /public_key
    current_date = datetime.now()

    return render_template('q1.html',number=number, current_date=current_date)

@app.route('/')
def q2():
    """
    example query:/q2?userid=123456789&tweet_time=2004-08-15+16:23:42
    """

    userid = request.args.get('uid')
    tweet_time = request.args.get('tweet_time')

    if not all([userid, tweet_time]):
        abort(403)




