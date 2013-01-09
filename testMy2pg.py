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
        self.assertEqual(my2pg.convert_type('integer(11)'), 'integer')
        self.assertEqual(my2pg.convert_type('bigint(8)'), 'bigint')
        self.assertEqual(my2pg.convert_type('unsigned int'), 'integer')

if __name__ == '__main__':
    unittest.main()
