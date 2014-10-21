from flask import request
from flask import render_template
from flask import abort

from amazonbies import app
from amazonbies.hbase_model import HBase
from datetime import datetime

@app.route('/')
def index():
    return "Ground control to mayor Tom."

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
    number = key / public_key
    current_date = datetime.now()

    return render_template('q1.html',number=number, current_date=current_date)

@app.route('/hbase/q2')
def hbase_q2():
    """
    example query:/q2?userid=1473664038&tweet_time=2014-04-08+02:43:27
    """

    userid = request.args.get('userid')
    tweet_time_str = request.args.get('tweet_time')

    if not all([userid, tweet_time_str]):
        abort(403)

    tweet_time_str = tweet_time_str.replace(" ", "+")

    hbase = HBase()
    row = hbase.get('tweets', userid, tweet_time_str)

    hbase.close()

    return render_template('q2.html', row=row)



