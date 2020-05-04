import os
import shutil
import tempfile
from unittest import TestCase
import crud

class Testcrud(TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, 'test_file.txt')
        test_content = ""
        with open(self.test_file, 'w') as fp:
            fp.write(test_content)
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_chenge_crud(self):
        # test_pattern (result,[input pattern])
        test_patterns = [
            ('    ', [0,0,0,0]),
            ('C   ', [1,0,0,0]),
            ('CR  ', [1,1,0,0]),
            ('CRU ', [1,1,1,0]),
            ('CRUD', [1,1,1,1]),
            ('C U ', [1,0,1,0]),
            ('C UD', [1,0,1,1]),
            ('C  D', [1,0,0,1]),
            (' R  ', [0,1,0,0]),
            (' RU ', [0,1,1,0]),
            (' RUD', [0,1,1,1]),
            ('  U ', [0,0,1,0]),
            ('  UD', [0,0,1,1]),
            ('   D', [0,0,0,1]),
        ]
        for resultcrud,inputcrud in test_patterns:
            with self.subTest(resultcrud=resultcrud,inputcrud=inputcrud):
                self.assertEqual(resultcrud, crud.chenge_crud(inputcrud))

    def test_judgment_crud(self):
        items = ['TABLE1','TABLE2']
        findtext = 'SELECT * FROM TABLE1;'
        self.assertEqual([('TABLE1',',1,,,,,')],crud.judgmentCrud(findtext, items))

    