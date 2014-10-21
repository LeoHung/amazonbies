import happybase
from amazonbies import app

class HBase:
    def __init__(self):
        self.get_connection()

    def get_connection(self, ip=app.config['HBASE_HOST']):
        self.conn = happybase.Connection(ip)
        self.conn.open()

    def get(self, table, userid, tweet_time_str):
        table = self.conn.table(table)
        row_key = "%s_%s" %(userid, tweet_time_str)
        row = table.row(row_key)

        if row.has_key('cfmain:tweetId'):
            row['cfmain:tweetId'] = row['cfmain:tweetId'].decode("utf8","ignore")
        if row.has_key('cfmain:sentimentScore'):
            row['cfmain:sentimentScore'] = row['cfmain:sentimentScore'].decode("utf8","ignore")
        if row.has_key("cfmain:censoredText"):
            row["cfmain:censoredText"] = row["cfmain:censoredText"].decode("utf8", "ignore")

        return row

    def close(self):
        self.conn.close()
