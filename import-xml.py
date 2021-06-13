import psycopg2

RESOURCES = '.'

if __name__ == '__main__':
    filepath = '%s/xml/' % RESOURCES
    conn = psycopg2.connect(host='localhost',
                            port='5432',
                            dbname='default',
                            user='default',
                            password='secret')
    cursor = conn.cursor()
    cursor.execute("select identifier from pkg_md2_md_iso")
    metadata_rows = cursor.fetchall()
    count = 1
    for row in metadata_rows:
        print(str(count) + ' - ' + row[0])
        file_xml = open(filepath + row[0] + '.txt', 'r', encoding='iso-8859-1')
        xml = file_xml.read()
        cursor.execute('update pkg_md2_md_iso set xml = %s where identifier = %s', (xml, row[0]))
        count = count + 1
    conn.commit()
