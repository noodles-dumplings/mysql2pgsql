#!/usr/bin/env python
import unittest
import my2pg


class TypeConversionTestCase(unittest.TestCase):
    def test_integers(self):
        self.assertEqual(my2pg.convert_type('int', auto_increment=True), 'serial')
        self.assertEqual(my2pg.convert_type('int'), 'integer')
        self.assertEqual(my2pg.convert_type('bigint', auto_increment=True), 'bigserial')
        self.assertEqual(my2pg.convert_type('bigint'), 'bigint')
        self.assertEqual(my2pg.convert_type('int(11)'), 'integer')
        self.assertEqual(my2pg.convert_type('int(50)'), 'integer')

        self.assertEqual(my2pg.convert_type('unsigned int(50)'), 'bigint')
        self.assertEqual(my2pg.convert_type('unsigned integer(11)'), 'bigint')

        self.assertEqual(my2pg.convert_type('integer(11)'), 'integer')
        self.assertEqual(my2pg.convert_type('bigint(10)'), 'bigint')
        self.assertEqual(my2pg.convert_type('varchar(16)'), 'varchar(16)')
        self.assertEqual(my2pg.convert_type("enum('Workshop')"), 'varchar(8)')
        self.assertEqual(my2pg.convert_type("enum('Y','N')"), 'varchar(1)')
        self.assertEqual(my2pg.convert_type('smallint(5) unsigned', auto_increment=True), 'serial')
        self.assertEqual(my2pg.convert_type('tinytext'), 'text')


if __name__ == '__main__':
    unittest.main()
