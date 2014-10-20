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
        rows = table.rows(row_key)
        return rows

    def close():
        self.conn.close()
