from sqlalchemy import create_engine, MetaData


class Sql:
    """db connection"""

    def __init__(self, db):
        self.db = db
        self.connString = """mysql+pymysql://{user}:{pw}@#/{db}""".format(user="#", pw="#",
                                                                                           db=self.db)


    def select(self, select, table, where, conn):
        query = """select {} from {}.{} where {}""".format(select, self.db, table, where)
        result = conn.execute(query)
        return result.fetchone()[0]

    def select_all(self, select, table, where, conn):
        query = """select {} from {}.{} where {}""".format(select, self.db, table, where)
        result = conn.execute(query)
        row = [item[0] for item in result.fetchall()]
        return row

    def insert(self, table, data):
        engine = create_engine(self.connString)
        conn = engine.connect()
        meta = MetaData(bind=conn, reflect=True)
        meta_table = meta.tables[table]
        conn.execute(meta_table.insert(), data)

    def update_one(self, table, id, idName, column, value, conn):
        conn.execute("""UPDATE
        {}.{}
        SET
        {} = "{}"
        WHERE
        {} = {}
        """.format(self.db, table, column, str(value), idName, id))


    def conn(self):
        engine = create_engine(self.connString)
        conn = engine.connect()
        return conn
