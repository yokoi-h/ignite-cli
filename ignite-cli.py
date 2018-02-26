from __future__ import unicode_literals
from prompt_toolkit import prompt
from prompt_toolkit.history import InMemoryHistory
from cli_helpers.tabular_output import TabularOutputFormatter

formatter = TabularOutputFormatter(format_name='psql')

import pyodbc
import sys
import requests

class IgniteDriver(object):

    def __init__(self, host, cachename='default'):
        self.host = host
        self.cachename = cachename
        self.connection = None

    def connect(self):
        #uri = 'DRIVER={Apache Ignite};ADDRESS=' + self.host + ':10800;CACHE=' + self.cachename
        uri = 'DRIVER={Apache Ignite};ADDRESS=' + self.host + ':10800'
        ignite = pyodbc.connect(uri, autocommit=True)
        ignite.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        ignite.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        ignite.setencoding(str, encoding='utf-8')
        ignite.setencoding(unicode, encoding='utf-8')
        self.connection = ignite

    def process_query(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)

    def process_fetch(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        rows = cur.fetchall()

        if len(rows) > 0:
            header = (c[0] for c in rows[0].cursor_description)
            data = rows
            self.output(data, header)
        else:
            print("the result is none;")

    def process_commit(self):
        self.connection.commit()

    def process_rollback(self):
        self.connection.rollback()

    def process_show(self, sql):
        cur = self.connection.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

    def process_rest(self, url):
        response = requests.get(url)
        result = response.json()
        return result

    def process_show_tables(self):
        url = 'http://' + self.host + ':8080/ignite?cmd=top&attr=true&mtr=true'
        res = self.process_rest(url)
        data = []
        header = ('cache_name', 'table_name', 'mode')
        for cache in res['response'][0]['caches']:
            if cache['sqlSchema'] == 'PUBLIC':
                data.append((cache['name'], cache['name'][11:], cache['mode']))

        self.output(data, header)

    def process_describe(self, table_name):
        url = 'http://' + self.host + ':8080/ignite?cmd=metadata&cacheName=' + table_name
        res = self.process_rest(url)
        body = res['response'][0]
        key = body['types'][0]
        fields = body['fields'][key]

        header = ('column', 'type')
        data = []
        for column, data_type in fields.items():
            data.append((column , data_type))

        self.output(data, header)

        header = ('index name', 'fields')
        data = []
        indexes = body['indexes'][key]
        for idx in indexes:
            idx_fields = idx['fields']
            idx_name = idx['name']
            data.append((idx_name, idx_fields))

        self.output(data, header)

    def process_show_topology(self):
        url = 'http://' + self.host + ':8080/ignite?cmd=top&attr=true&mtr=true'
        res = self.process_rest(url)
        data = []
        header = ('node_id', 'hostname', 'ip addresses')
        for node in res['response']:
            node_id = node['nodeId']
            hostname = node['tcpHostNames']
            tcp_addresses = node['tcpAddresses']
            data.append((node_id, hostname, tcp_addresses))

        self.output(data, header)

    def output(self, data, header):
        for l in formatter.format_output(data, header):
            print(l)

def main():

    history = InMemoryHistory()

    host = sys.argv[1]

    driver = IgniteDriver(host)
    driver.connect()

    while(True):
        text = prompt("ignite> ", history=history)

        if len(text) == 0:
            continue
        else:
            text = text.strip(';')

        try:
            if text.lower().find('select') == 0:
                driver.process_fetch(text)
            elif text.lower() == 'quit':
                break
            elif text.lower() == 'commit;':
                driver.process_commit()
            elif text.lower() == 'rollback;':
                driver.process_rollback()
            elif text.lower().find('show tables') == 0:
                driver.process_show_tables()
            elif text.lower().find('describe') == 0:
                items = text.split()
                driver.process_describe(items[1])
            else:
                driver.process_query(text)

        except pyodbc.Error as e:
            print(e)

if __name__ == '__main__':
    main()
