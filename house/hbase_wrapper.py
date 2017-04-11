# -*- coding: utf-8 -*-
from hbase import Hbase
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket
from thrift.transport.TTransport import TFramedTransport, TTransportException
from hbase.ttypes import IOError


class HbaseWrapper(object):
    def __init__(self, host='127.0.0.1', port=9090, table=None):
        self.host = host
        self.port = port
        if not isinstance(table, str):
            raise TypeError('table must be type str')
        self.table = table
        self._create_connection()

    def _create_connection(self):
        self.transport = TTransport.TBufferedTransport(TFramedTransport(TSocket.TSocket(self.host, self.port)))
        self.protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()

    def create_table_if_not_exists(self, column_families):
            try:
                self.client.getColumnDescriptors(self.table)
            except IOError:
                self.client.createTable(self.table, column_families)

    @classmethod
    def column(cls, column_family, qualifier):
        return '%s:%s' % (column_family, qualifier)

    @classmethod
    def mutation(cls, column_family, qualifier, value=None):
        return Hbase.Mutation(column=cls.column(column_family, qualifier), value=value)

    def _reopen(self):
        if self.transport.isOpen():
            self.transport.close()
        self.transport.open()

    def put(self, row_key, mutations, attrs={}):
        try:
            self.client.mutateRow(self.table, row_key, mutations, attrs)
        except TTransportException:
            self._reopen()
            self.put(row_key, mutations, attrs)

    def get(self, row_key, columns, attrs={}):
        try:
            return self.client.getRowWithColumns(self.table, row_key, columns, attrs)
        except TTransportException:
            self._reopen()
            return self.get(row_key, columns, attrs)

    def delete(self, row_key):
        try:
            self.client.deleteAllRow(self.table, row_key, {})
        except TTransportException:
            self._reopen()
            return self.delete(self.table, row_key)

    def scan_and_get(self, tscan, num=100):
        try:
            scan_id = self.client.scannerOpenWithScan(self.table, tscan, {})
            rows = self.client.scannerGetList(scan_id, num)
            self.client.scannerClose(scan_id)
            return [r.row for r in rows]
        except TTransportException:
            self._reopen()
            return self.scan_and_get(tscan, num)

    def close(self):
        if self.transport.isOpen():
            self.transport.close()
