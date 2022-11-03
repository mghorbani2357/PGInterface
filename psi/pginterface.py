import collections


class TableInterFace:
    table_name = 'table_name'

    def __init__(self, sql_connection, schema_name=None):
        self.__sql_connection = sql_connection
        self.__cursor = self.__sql_connection.cursor()
        self.__schema_name = schema_name
        self.__table_schema = collections.namedtuple(self.table_name, [item[0] for item in self.__select(
            f"""
                SELECT column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='{self.__schema_name}' AND TABLE_NAME='{self.table_name}'
            """
        )])

    def __repr__(self):
        return f"<class '{self.table_name}'>"

    def __len__(self):
        return self.__select(f"""SELECT COUNT(*) FROM "{self.__schema_name}".{self.table_name}""")[0][0]

    def __getitem__(self, row_id):
        return self.__table_schema(*self.__select(f"""SELECT * FROM "{self.__schema_name}".{self.table_name} WHERE id = {row_id}""")[0])

    def __delitem__(self, row_id):
        self.__cursor.execute(f"""DELETE  FROM "{self.__schema_name}".{self.table_name} WHERE id = {row_id}""")
        self.__sql_connection.commit()

    def __iter__(self):
        for i in range(self.__len__()):
            yield self.__getitem__(i)

    def __select(self, query):
        self.__cursor.execute(query)
        return self.__cursor.fetchall()

    def __insert(self, query):
        self.__cursor.execute(query)
        self.__sql_connection.commit()

    def __update(self, query):
        self.__cursor.execute(query)
        self.__sql_connection.commit()

    def __sql_delete(self, query):
        self.__cursor.execute(query)

    def insert(self, **kwargs):
        self.__insert(f"""INSERT INTO "{self.__schema_name}".{self.table_name} ({', '.join(kwargs.keys())})
        VALUES ({', '.join(map(lambda value: f"$${value}$$", kwargs.values()))});""")

    def where(self, **kwargs):
        result = list()
        conditions = [f"{key}='{value}'" for key, value in kwargs.items()]

        for row in self.__select(f"""SELECT * FROM "{self.__schema_name}".{self.table_name} WHERE {' AND '.join(conditions)}"""):
            result.append(self.__table_schema(*row))

        return result

    def update(self, update, where=None):
        conditions = [f"{key}='{value}'" for key, value in where.items()]

        self.__update(
            f"""
                UPDATE "{self.__schema_name}".{self.table_name}
                SET {', '.join([f"{key}='{value}'" for key, value in update.items()])}
                WHERE {' AND '.join(conditions)}
            """
        )

    def delete(self, **kwargs):
        conditions = [f"{key}='{value}'" for key, value in kwargs.items()]
        self.__sql_delete(f"""DELETE FROM "{self.__schema_name}".{self.table_name} WHERE {' AND '.join(conditions)}""")

    def commit(self):
        self.__sql_connection.commit()
