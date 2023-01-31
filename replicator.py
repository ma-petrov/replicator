from sqlalchemy import create_engine, text, Table, MetaData, Column
from typing import Any, Dict, List, Tuple


class SQLAlchemyEngine:
    def __init__(self, conn, schema, table, row_id='row_id', batch_size=10000, cols=None, custom_fetch=None):
        self.conn = create_engine(self.conn_str(conn)).connect()
        self.row_id = row_id
        self.batch_size = batch_size
        self.make_table = lambda cols: Table(table, MetaData(), *[Column(c) for c in cols])
        self.template = f'SELECT {{select}} FROM {schema}.{table}'
        self.trancate_query = f'TRUNCATE TABLE {schema}.{table}'
        self.fetch_query = self.create_fetch_query(row_id, cols, self.template, custom_fetch)

    def __del__(self):
        self.conn.close()

    def conn_str(self, conn):
        return {
            ...
        }[conn]

    def execute(self, query):
        return self.conn.execute(text(query))

    def create_select(self, row_id, cols):
        if not cols: return '*'
        if row_id not in cols: cols.append(row_id)
        return ', '.join(cols)

    def create_fetch_query(self, row_id, cols, template, custom_fetch):
        if custom_fetch: return custom_fetch
        query = template.format(select=self.create_select(row_id, cols))
        return query + f' WHERE {row_id} > {{first}} ORDER BY {row_id}'

    def first(self):
        return self.execute(self.template.format(select=f'MIN({self.row_id}) - 1')).fetchone()[0]

    def last(self):
        return self.execute(self.template.format(select=f'MAX({self.row_id})')).fetchone()[0]

    def fetch_batches(self, first=None):
        first = first or self.first()
        cursor = self.execute(self.fetch_query.format(first=first))
        cols = list(cursor.keys())
        while True:
            batch = cursor.fetchmany(self.batch_size)
            if len(batch) < 1: break
            yield cols, batch

    def insert_batch(self, cols, batch):
        self.conn.execute(self.make_table(cols).insert(), [dict(zip(cols, row)) for row in batch])

    def truncate(self):
        self.execute(self.trancate_query)


class BaseReplicator:
    def __init__(self, config: Dict[str, Any]):
        """
        config: 
        {
            'src': {
                conn: str, название подключения в Airflow.

                schema: str, схема таблицы.

                table: str, название таблицы.

                row_id: str='row_id', primary key, либо сортируемая колонка.

                batch_size: int=10000, размер одной порции данных.
                    Параметр игнорируется для destination engine.

                cols: List[str]=None, колонки, которые нужно заселектить.
                    Если колонки указаны, но в них не содержится row_id, она
                    будет добавлена автоматически. Параметр игнорируется для
                    destination engine.

                custom_fetch: str=None, Кастомный sql-запрос, должен
                    обязательно содержать условие WHERE row_id > ... и
                    ORDER BY row_id. Параметр игнорируется для destination
                    engine.
            },

            'dst': {...}, тоже самое что и 'src'

            'new_cols': Dict[str, str]=None, словарь переименовывания колонок.
                Ключ - старое название, значение - новое.

            'transform_func': callable=None, функция для трансформации батчей.
                Изменяет каждую строку, должна принимать на вход Tuple и возв-
                ращать Tuple.
        }
        """
        self.src_engine = SQLAlchemyEngine(**config['src'])
        self.dst_engine = SQLAlchemyEngine(**config['dst'])
        self.new_cols = config.get('new_cols')
        self.transform_func = config.get('transform_func')

    def transform(self, cols: List, batch: List[Tuple]):
        if self.new_cols:
            cols.clear()
            cols.extend(self.new_cols)
        if self.transform_func:
            for i, row in enumerate(batch):
                batch[i] = self.transform_func(row)
    
    def replicate(self):
        raise NotImplementedError


class IncrementReplicator(BaseReplicator):
    def replicate(self):
        first = self.dst_engine.last()
        for b in self.src_engine.fetch_batches(first):
            self.transform(*b)
            self.dst_engine.insert_batch(*b)


class FullReplicator(BaseReplicator):
    def replicate(self):
        self.dst_engine.truncate()
        for b in self.src_engine.fetch_batches():
            self.transform(*b)
            self.dst_engine.insert_batch(*b)