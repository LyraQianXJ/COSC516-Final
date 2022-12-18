import unittest

from final import finalExam

class CassandraTest(unittest.TestCase):

    def setUp(self) -> None:
        print()
        print('*'*5 + 'Cassandra test begin' + '*'*5)
        self.client = None

    def test_connect(self):
        self.client = finalExam()
        # test
        self.assertIsNotNone(self.client, None)

    def test_load_data(self):
        self.client = finalExam()
        self.client.connect()
        self.client.create_table()
        self.client.load_data()

        query_statement1 = "select * from gamestate1"
        query_statement2 = "select * from gamestate2"
        query_statement3 = "select * from gameevent"
        res1 = self.client.session.execute(query_statement1)
        res2 = self.client.session.execute(query_statement2)
        res3 = self.client.session.execute(query_statement3)
        self.assertEqual(len(res1.all()), 1000)
        self.assertEqual(len(res2.all()), 1000)
        self.assertEqual(len(res3.all()), 10000)

    def test_query_1(self):
        self.client = finalExam()
        self.client.connect()
        res1 = self.client.query_1('1')
        self.assertEqual(str(res1), "[Row(id='1', email='email1@abc.com', gold=56607, level=21, name='Name#1', power=248393, region='2', statetime='2022-12-19 15:23:07')]")
        res2 = self.client.query_1('949')
        self.assertEqual(str(res2),"[Row(id='949', email='email949@abc.com', gold=843488, level=12, name='Name#949', power=949991, region='3', statetime='2022-12-20 22:59:56')]")

    def test_query_2(self):
        self.client = finalExam()
        self.client.connect()
        res1 = self.client.query_2('1')
        self.assertEqual(str(res1), "[Row(region='1', power=995016, email='email714@abc.com', gold=879429, id='714', level=39, name='Name#714', statetime='2022-12-21 20:12:01'), "
                                   "Row(region='1', power=991827, email='email861@abc.com', gold=805421, id='861', level=27, name='Name#861', statetime='2022-12-18 13:35:38'), "
                                   "Row(region='1', power=985299, email='email42@abc.com', gold=13966, id='42', level=28, name='Name#42', statetime='2022-12-18 04:54:28'), "
                                   "Row(region='1', power=969608, email='email776@abc.com', gold=333937, id='776', level=30, name='Name#776', statetime='2022-12-22 11:11:23'), "
                                   "Row(region='1', power=965569, email='email756@abc.com', gold=359465, id='756', level=46, name='Name#756', statetime='2022-12-19 16:42:53'), "
                                   "Row(region='1', power=960370, email='email753@abc.com', gold=683081, id='753', level=5, name='Name#753', statetime='2022-12-20 14:36:08'), "
                                   "Row(region='1', power=957907, email='email745@abc.com', gold=771462, id='745', level=29, name='Name#745', statetime='2022-12-21 21:10:05'), "
                                   "Row(region='1', power=944167, email='email374@abc.com', gold=443766, id='374', level=17, name='Name#374', statetime='2022-12-21 09:39:53'), "
                                   "Row(region='1', power=942297, email='email679@abc.com', gold=791177, id='679', level=36, name='Name#679', statetime='2022-12-18 20:32:58'), "
                                   "Row(region='1', power=936979, email='email260@abc.com', gold=481570, id='260', level=47, name='Name#260', statetime='2022-12-20 15:06:42')]")
        res2 = self.client.query_2('9')
        self.assertEqual(str(res2),"[Row(region='9', power=999230, email='email56@abc.com', gold=820553, id='56', level=7, name='Name#56', statetime='2022-12-22 22:35:34'), "
                                   "Row(region='9', power=986362, email='email213@abc.com', gold=899727, id='213', level=20, name='Name#213', statetime='2022-12-23 00:41:30'), "
                                   "Row(region='9', power=975717, email='email908@abc.com', gold=49686, id='908', level=14, name='Name#908', statetime='2022-12-22 02:41:32'), "
                                   "Row(region='9', power=972267, email='email205@abc.com', gold=504200, id='205', level=37, name='Name#205', statetime='2022-12-22 10:02:55'), "
                                   "Row(region='9', power=971763, email='email523@abc.com', gold=154694, id='523', level=2, name='Name#523', statetime='2022-12-20 02:58:11'), "
                                   "Row(region='9', power=967875, email='email362@abc.com', gold=677826, id='362', level=3, name='Name#362', statetime='2022-12-22 02:38:22'), "
                                   "Row(region='9', power=967599, email='email53@abc.com', gold=834964, id='53', level=28, name='Name#53', statetime='2022-12-17 17:35:19'), "
                                   "Row(region='9', power=964710, email='email842@abc.com', gold=165299, id='842', level=1, name='Name#842', statetime='2022-12-23 21:11:29'), "
                                   "Row(region='9', power=945562, email='email641@abc.com', gold=185578, id='641', level=5, name='Name#641', statetime='2022-12-23 11:37:06'), "
                                   "Row(region='9', power=919848, email='email269@abc.com', gold=428095, id='269', level=46, name='Name#269', statetime='2022-12-18 17:27:10')]")
    def test_query_3(self):
        self.client = finalExam()
        self.client.connect()
        res1 = self.client.query_3('2','2022-12-17 05:30:00','2022-12-17 15:00:00')
        res2 = self.client.query_3('7','2022-12-18 00:00:00','2022-12-19 11:00:00')
        self.assertEqual(str(res1),"[Row(id='359', email='email359@abc.com', gold=802844, level=13, name='Name#359', power=229292, region='2', statetime='2022-12-17 09:50:08'), "
                                   "Row(id='380', email='email380@abc.com', gold=993267, level=18, name='Name#380', power=668825, region='2', statetime='2022-12-17 13:00:05'), "
                                   "Row(id='402', email='email402@abc.com', gold=747832, level=18, name='Name#402', power=23155, region='2', statetime='2022-12-17 13:07:07'), "
                                   "Row(id='849', email='email849@abc.com', gold=819676, level=39, name='Name#849', power=909198, region='2', statetime='2022-12-17 14:07:43')]")
        self.assertEqual(str(res2),"[Row(id='141', email='email141@abc.com', gold=19135, level=20, name='Name#141', power=743287, region='7', statetime='2022-12-18 14:03:45'), "
                                   "Row(id='32', email='email32@abc.com', gold=872325, level=29, name='Name#32', power=713822, region='7', statetime='2022-12-18 08:02:37'), "
                                   "Row(id='329', email='email329@abc.com', gold=626748, level=11, name='Name#329', power=472341, region='7', statetime='2022-12-18 01:02:09'), "
                                   "Row(id='87', email='email87@abc.com', gold=447993, level=8, name='Name#87', power=938463, region='7', statetime='2022-12-19 08:22:50'), "
                                   "Row(id='916', email='email916@abc.com', gold=819849, level=29, name='Name#916', power=31139, region='7', statetime='2022-12-19 07:11:10')]")
    def test_update(self):
        self.client = finalExam()
        self.client.connect()
        self.client.update('5')
        res = self.client.session.execute("select * from gamestate1 where id='5'")
        self.assertEqual(str(res.all()),"[Row(id='5', email='email5@abc.com', gold=872945, level=8, name='Name#5', power=76871, region='8', statetime='2022-12-19 04:23:49')]")
