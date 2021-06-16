import psycopg2
import csv
import time


def insert_publish_job(id_metadata):
    timestamp = str(int(time.time()))
    sql_job = 'INSERT INTO public.jobs (queue, payload, attempts, available_at, created_at) values (%s, %s, %s, %s,%s)'
    cursor.execute(sql_job, ('default',
                             '{"displayName":"Geonodo\\\MetadataV2\\\Jobs\\\PublishMetadataJob",'
                             '"job":"Illuminate\\\Queue\\\CallQueuedHandler@call","maxTries":3,"delay":null,'
                             '"timeout":300,"timeoutAt":null,"data":{'
                             '"commandName":"Geonodo\\\MetadataV2\\\Jobs\\\PublishMetadataJob",'
                             '"command":"O:42:\\"Geonodo\\\MetadataV2\\\Jobs\\\PublishMetadataJob\\":10:{'
                             's:5:\\"tries\\";i:3;s:7:\\"timeout\\";i:300;s:54:\\"\\u0000Geonodo\\\MetadataV2\\\Jobs'
                             '\\\PublishMetadataJob\\u0000metadataId\\";i:' + str(id_metadata) +
                             ';s:6:\\"\\u0000*\\u0000job\\";N;s:10:\\"connection\\";s:8:\\"database\\";s:5:\\"queue'
                             '\\";N;s:15:\\"chainConnection\\";N;s:10:\\"chainQueue\\";N;s:5:\\"delay\\";N;s:7'
                             ':\\"chained\\";a:0:{}}"}}',
                             0,
                             timestamp,
                             timestamp)
                   )


if __name__ == '__main__':
    conn = psycopg2.connect(host='localhost',
                            port='5432',
                            dbname='default',
                            user='default',
                            password='secret')
    cursor = conn.cursor()
    input_file_path = 'metadata.csv'
    with open(input_file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count != 0:
                identifier = row[3]
                status = row[4]
                if status == 'approved':
                    cursor.execute("select metadata_id from public.pkg_md2_md_iso where identifier = '" + row[1] + "'")
                    result = cursor.fetchone()
                    if len(result) < 1:
                        print('NO Encontrado: ' + row[1])
                        continue
                    else:
                        print('publish metadata: ' + str(result[0]))
                        insert_publish_job(result[0])
            line_count = 1
    conn.commit()
    cursor.close()
