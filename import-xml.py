import psycopg2
import time

RESOURCES = '.'


def insert_parse_xml_job(id_iso):
    timestamp = str(int(time.time()))
    sql_job = 'INSERT INTO public.jobs (queue, payload, attempts, available_at, created_at) values (%s, %s, %s, %s,%s)'
    cursor.execute(sql_job, ('default',
                             '{"displayName":"Geonodo\\\MetadataV2\\\Jobs\\\ParseISOXMLJob",'
                             '"job":"Illuminate\\\Queue\\\CallQueuedHandler@call","maxTries":3,"delay":null,'
                             '"timeout":300,"timeoutAt":null,"data":{'
                             '"commandName":"Geonodo\\\MetadataV2\\\Jobs\\\ParseISOXMLJob",'
                             '"command":"O:38:\\"Geonodo\\\MetadataV2\\\Jobs\\\ParseISOXMLJob\\":10:{'
                             's:5:\\"tries\\";i:3;s:7:\\"timeout\\";i:300;s:47:\\"\\u0000Geonodo\\\MetadataV2\\\Jobs'
                             '\\\ParseISOXMLJob\\u0000mdIsoId\\";i:' + str(id_iso) +
                             ';s:6:\\"\\u0000*\\u0000job\\";N;s:10:\\"connection\\";s:8:\\"database\\";s:5:\\"queue'
                             '\\";N;s:15:\\"chainConnection\\";N;s:10:\\"chainQueue\\";N;s:5:\\"delay\\";N;s:7'
                             ':\\"chained\\";a:0:{}}"}}',
                             0,
                             timestamp,
                             timestamp)
                   )


def update_xml(uuid):
    file_xml = open(filepath + uuid + '.txt', 'r', encoding='iso-8859-1')
    xml = file_xml.read()
    cursor.execute('update pkg_md2_md_iso set xml = %s where identifier = %s', (xml, uuid))


if __name__ == '__main__':
    filepath = '%s/xml/' % RESOURCES
    conn = psycopg2.connect(host='localhost',
                            port='5432',
                            dbname='default',
                            user='default',
                            password='secret')
    cursor = conn.cursor()
    cursor.execute("select identifier, id from pkg_md2_md_iso order by id")
    metadata_rows = cursor.fetchall()
    count = 1
    load_with_error = []
    for row in metadata_rows:
        print(str(count) + ' - ' + str(row[1]) + ' - ' + row[0])
        update_xml(row[0])
        count = count + 1
        insert_parse_xml_job(row[1])
    conn.commit()
    cursor.close()
    if conn is not None:
        conn.close()

