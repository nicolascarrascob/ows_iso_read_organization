import psycopg2
import csv

if __name__ == '__main__':
    conn = psycopg2.connect(host='localhost',
                            port='5432',
                            dbname='default',
                            user='default',
                            password='secret')
    cursor = conn.cursor()
    input_file_path = 'md.csv'
    with open(input_file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count != 0:
                cursor.execute("select id from public.pkg_md2_group where name = '"+row[3].replace("'","''")+"'")
                result = cursor.fetchone()
                if len(result) < 1:
                    print('NO Encontrado: ' + row[3])
                else:
                    print('cargando: ' + row[1])
                    cursor.execute(
                        "insert into public.pkg_md2_md_iso (profile, owner_id, instance_id, identifier, harvested) values (%s, %s, %s, %s, %s)", (row[0], result[0], 2, row[1], row[2]))
            line_count = 1
    conn.commit()
    cursor.close()
