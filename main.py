#!/usr/bin/python3

from sqlalchemy import create_engine, text, Table, MetaData, Column
from sqlalchemy.ext.asyncio import create_async_engine
from asyncio import run, create_task

from datetime import datetime

class Replicator:
    def __init__(self, src_conn_str, dst_conn_str, table, rid_col='rid', batch_size=1000):
        self.src_engine = create_async_engine(src_conn_str)
        self.dst_engine = create_async_engine(dst_conn_str)
        self.table = table
        self.rid_col = rid_col
        self.batch_size = batch_size

    async def get_max_rid(self, conn):
        result = await conn.execute(text(f'SELECT MAX({self.rid_col}) FROM {self.table}'))
        return result.fetchone()[0]

    async def get_start_rid(self, conn, dst_max_rid):
        result = await conn.execute(text(f'SELECT MIN({self.rid_col}) FROM {self.table} WHERE {self.rid_col} > {dst_max_rid}'))
        return result.fetchone()[0]

    def make_table(self, cols):
        return Table(self.table, MetaData(), *[Column(c) for c in cols])

    async def fetch_batch(self, src_conn, rid):
        print(f'start fetching batch, rid == {rid}')
        
        query = (
            f'SELECT * FROM {self.table} '
            f'WHERE {self.rid_col} >= {rid} '
            f'ORDER BY {self.rid_col} '
            f'LIMIT {self.batch_size}'
        )
        batch = await src_conn.execute(text(query))
        cols = list(batch.keys())
        
        print(f'end fetching batch, rid == {rid}')
        
        return cols, batch
    
    async def insert_batch(self, conn, cols, batch):
        print('start inserting batch')
        await conn.execute(self.make_table(cols).insert(), [dict(zip(cols, x)) for x in batch])
        print('end inserting batch')

    async def replicate_(self):
        async with self.src_engine.connect() as src_conn:
            async with self.dst_engine.begin() as dst_conn: 
                src_max_rid = await self.get_max_rid(src_conn)
                dst_max_rid = await self.get_max_rid(dst_conn)
                if dst_max_rid:
                    rid = await self.get_start_rid(src_conn, dst_max_rid)
                else:
                    rid = 1
                if rid:
                    while rid < src_max_rid:
                        cols, batch = await self.fetch_batch(src_conn, rid)
                        await create_task(self.insert_batch(dst_conn, cols, batch))
                        rid += self.batch_size

    def replicate(self):
        run(self.replicate_())

if __name__ == '__main__':
    replicator = Replicator(
        src_conn_str='postgresql+asyncpg://petrov:1111@localhost:5432/analytical',
        dst_conn_str='postgresql+asyncpg://petrov:1111@localhost:5433/analytical',
        table='test_read'
    )
    t = datetime.now()
    replicator.replicate()
    print(datetime.now() - t)