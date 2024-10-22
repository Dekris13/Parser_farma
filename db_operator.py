import psycopg2


class stg_data_loader():

    def __init__(self, dataList:list, pg_conn_arg:dict) -> None:
        self.pg_conn_arg = pg_conn_arg
        self.data = dataList

    def LoadData(self) -> None:

        conn = psycopg2.connect(**self.pg_conn_arg)
        cur = conn.cursor()

        # Удаляем данные за день загрузики для избежания дублей
        cur.execute(
                    """
                        delete from stg_farma.products
                        where upload_day = %(upload_day)s and category = %(category)s

                    """,
                    {
                        'category': self.data[0][2],
                        'upload_day': self.data[0][4]
                    }
                )

        #Построчно загружаем данные за день загрузки
        for row in self.data:

            cur.execute(
                    """
                        insert into stg_farma.products (name, price, category, sourse, upload_day)
                        values (%(name)s,%(price)s,%(category)s,%(sourse)s,%(upload_day)s)

                    """,
                    {
                        'name': row[0],
                        'price': row[1],
                        'category': row[2],
                        'sourse': row[3],
                        'upload_day': row[4]
                    }
                )
        
        conn.commit()
        cur.close()
        conn.close()
            



            








