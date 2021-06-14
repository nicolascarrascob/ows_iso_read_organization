import psycopg2

if __name__ == '__main__':
    conn = psycopg2.connect(host='localhost',
                            port='5432',
                            dbname='default',
                            user='default',
                            password='secret')
    cursor = conn.cursor()
    cursor.execute("select id from pkg_md2_md_metadata order by id")
    metadata_rows = cursor.fetchall()
    count = 1
    load_with_error = []
    for row in metadata_rows:
        print(str(count) + ' - ' + str(row[0]))
        cursor.execute("delete from pkg_md2_md_metadata where id = " + str(row[0]))
        count = count + 1
        if count > 50:
            print("Commit")
            conn.commit()
            count = 1
    conn.commit()
    cursor.close()
    if conn is not None:
        conn.close()

