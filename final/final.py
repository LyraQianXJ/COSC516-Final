import functools

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import re
from itertools import chain

class finalExam():
    def connect(self):
        self.cloud_config = {'secure_connect_bundle': './secure-connect-cosc516.zip'}
        self.auth_provider = PlainTextAuthProvider('XzKHohELydvtjBGRROpCAfve',
                                           'H5CgdcxsamQ,1,0cJL-kywZ+_DsT_wjp.emqlmCk_khN9.kznJ4Z8m09O-1-5zOTN9vmhJLOf8XG2HP.T2ODlE1M.4jL_oJ5oMNQzByfOC1TLHDOaanTFeoL8eHS-_Y.')
        self.cluster = Cluster(cloud=self.cloud_config, auth_provider=self.auth_provider)
        self.session = self.cluster.connect('lyra')
    def create_table(self):
        #create gamestate1 table for query1
        self.session.execute("""
                   CREATE TABLE IF NOT EXISTS gamestate1 (
                       id text,
                       statetime text,
                       region text,
                       name text,
                       email text,
                       gold int,
                       power int,
                       level int,
                       PRIMARY KEY(id));
                   """)
        #create gamestate2 table for query2
        self.session.execute("""
                   CREATE TABLE IF NOT EXISTS gamestate2 (
                       id text,
                       statetime text,
                       region text,
                       name text,
                       email text,
                       gold int,
                       power int,
                       level int,
                       PRIMARY KEY(region,power));
                   """)
        #create gameevent table
        self.session.execute("""
                   CREATE TABLE IF NOT EXISTS gameevent (
                       eventid text,
                       userid text,
                       eventtime text,
                       type text,
                       diffgold int,
                       diffpower int,
                       difflevel int,
                       PRIMARY KEY(userid,eventid));
                   """)
    def load_data(self):
        #for gamestate1
        with open("./gamestate.csv", "r") as f:
            reader = csv.reader(f, delimiter=',')
            next(reader)
            for lines in reader:
                query = "INSERT INTO gamestate1 (id,statetime,region,name,email,gold,power,level) "
                query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                id = lines[0]
                statetime = lines[1]
                region = lines[2]
                name = lines[3]
                email = lines[4]
                gold = int(lines[5])
                power = int(lines[6])
                level = int(lines[7])
                self.session.execute(query, (id,statetime,region,name,email,gold,power,level))
        # #for gamestate2
        # with open("./gamestate.csv", "r") as f:
        #     reader = csv.reader(f, delimiter=',')
        #     next(reader)
        #     for lines in reader:
        #         query = "INSERT INTO gamestate2 (id,statetime,region,name,email,gold,power,level) "
        #         query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        #         id = lines[0]
        #         statetime = lines[1]
        #         region = lines[2]
        #         name = lines[3]
        #         email = lines[4]
        #         gold = int(lines[5])
        #         power = int(lines[6])
        #         level = int(lines[7])
        #         self.session.execute(query, (id,statetime,region,name,email,gold,power,level))
        # #for gameevent
        # with open("./gameevent.csv", "r") as f:
        #     reader = csv.reader(f, delimiter=',')
        #     next(reader)
        #     for lines in reader:
        #         query = "INSERT INTO gameevent (eventid,userid,eventtime,type,diffgold,diffpower,difflevel) "
        #         query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        #         eventid = lines[0]
        #         userid = lines[1]
        #         eventtime = lines[2]
        #         type = lines[3]
        #         diffgold = int(lines[4])
        #         diffpower = int(lines[5])
        #         difflevel = int(lines[6])
        #         self.session.execute(query, (eventid,userid,eventtime,type,diffgold,diffpower,difflevel))

    def query_1(self,id):
        #id:str
        q1 = self.session.execute("SELECT * FROM lyra.gamestate1 WHERE id = '"+str(id)+"'")
        return q1.all()

    def query_2(self,region):
        #region:str
        q2 = self.session.execute("SELECT * from lyra.gamestate2 WHERE region='"+str(region)+"' ORDER BY power DESC limit 10")

        return q2.all()
    def query_3(self,region,start,end):
        q3 = self.session.execute("select id from lyra.gamestate2 where region='" + str(region) + "' and statetime>'" + str(
            start) + "' and statetime<'" + str(end) + "' ALLOW FILTERING")
        l = list(q3)
        l1 = []
        for item in l:
            item = str(item)
            l1.append(re.findall("'([^']*)'", item))
        flatten_list = list(chain.from_iterable(l1))
        l2 = l1[0:10]
        l2 = list(chain.from_iterable(l2))
        l3 = l1[10::]
        l3 = list(chain.from_iterable(l3))
        final1 = ','.join(f"'{w}'" for w in l2)
        final2 = ','.join(f"'{w}'" for w in l3)
        q3f1 = self.session.execute(
            "select COUNT(*), userid from lyra.gameevent where userid in (" + final1 + ") group by userid")
        q3f2 = self.session.execute(
            "select COUNT(*), userid from lyra.gameevent where userid in (" + final2 + ") group by userid")
        count1 = list(q3f1)
        count2 = list(q3f2)
        count = count1 + count2
        def compare(a, b):
            if a.count > b.count:
                return -1
            elif a.count < b.count:
                return 1
            else:
                return 0

        count.sort(key=functools.cmp_to_key(compare))
        userid_list = []
        if len(count) <= 5:
            for i in range(len(count)):
                userid_list.append(count[i].userid)
        else:
            for i in range(5):
                userid_list.append(count[i].userid)
        print(userid_list)
        userlist = ','.join(f"'{w}'" for w in userid_list)
        q3d = self.session.execute("SELECT * FROM lyra.gamestate1 WHERE id IN (" + userlist + ")")
        return q3d.all()
    def update(self,id):
        qchange = self.session.execute(
            "SELECT eventtime,diffgold,diffpower,difflevel from lyra.gameevent where userid='"+str(id)+"'")
        qorig = self.session.execute("SELECT statetime,gold,power,level from lyra.gamestate1 where id='"+str(id)+"'")
        qchange = list(qchange)
        qorig = list(qorig)

        from datetime import datetime
        target = qchange[0]
        recentest_time = None
        for q in qchange:
            dd = datetime.strptime(q.eventtime, "%Y-%m-%d %X")
            if recentest_time == None or recentest_time < dd:
                recentest_time = dd
                target = q
        time = str(recentest_time)
        gold = str(target.diffgold + qorig[0].gold)
        power = str(target.diffpower + qorig[0].power)
        level = str(target.difflevel + qorig[0].level)


        self.session.execute("UPDATE gamestate1 SET statetime='" + time + "' WHERE id='"+str(id)+"'")
        self.session.execute("UPDATE gamestate1 SET gold= " + gold + " WHERE id='"+str(id)+"'")
        self.session.execute("UPDATE gamestate1 SET power= " + power + " WHERE id='"+str(id)+"'")
        self.session.execute("UPDATE gamestate1 SET level= " + level + " WHERE id='"+str(id)+"'")
        #since power is updated, and it is a primary key of table famestate2, so it cannot be updated, gamestate2 has to be re-oploaded with updated information from the csv


if __name__ == '__main__':
    client = finalExam()
    client.connect()
    client.create_table()
    client.load_data()
    client.query_1('1')
    client.query_1('949')
    client.query_2('1')
    client.query_2('7')
    client.query_3()
    client.update()




