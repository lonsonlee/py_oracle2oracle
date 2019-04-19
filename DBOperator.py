import cx_Oracle
import time
import ConfigGlobal as config
# DBOperator.py
# add by lixiaoqing 2019-4-16
# 定义数据库管理和执行的API


class DBOperator:
    def __init__(self):
        ##connect DB times
        self.CONN = None
        self.CON_TIMES = 0
        self.CURSOR_CNT = 0

    def conn_ora(self,uname, password, servicename, host, port):
        if self.CONN is None:
            try:
                self.CONN = cx_Oracle.connect(uname + '/' + password + '@' + host + ':' + port + '/' + servicename)
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Connect Oracle succeed.\n")
                return self.CONN
            except Exception as e:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Connect Oracle failed!=======\n")
                return -999
        else:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), " Oracle Connect is already exists!=======\n")
            return self.CONN

    def dis_conn(self):
        if self.CONN is not None:
            try:
                self.CONN.close()
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Disconnect Oracle success!=======\n")
                return 1
            except Exception as e:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Disconnect Oracle failed!=======\n")
                return -998
        else:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "not Oracle Connection.\n")
            return -997

    def open_cursor(self):
        self.CURSOR_CNT += 1
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "get Oracle Cursor OK!=======\n")
        return self.CONN.cursor()

    def dis_cursor(self,cursor):
        if type(cursor) is not None:
            try:
                self.CURSOR_CNT -= 1
                cursor.close()
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Disconnect Oracle Cursor Succeed!=======\n")
                return 1
            except Exception as e:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Disconnect Oracle Cursor failed!=======\n")
                return -996
        else:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Oracle Cursor is not valiable.\n")
            return 2

    def db_query(self,cursor, querysql):
        if config.logLevel == 'INFO':
            print("querysql is ="+querysql)
        try:
            cursor.execute(querysql)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Query Oracle Success!=======\n")
            return cursor.fetchall()
        except Exception as e:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Query Oracle failed!=======\n")
            return -997

    def db_delete(cself,cursor, deletesql):
        if config.logLevel == 'INFO':
            print("deletesql is ="+deletesql)
        try:
            cursor.execute(deletesql)
            return self.CONN.commit()
        except Exception as e:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Delete Oracle failed!=======\n")
            return -996

    def db_update(self,cursor, updatesql):
        if config.logLevel == 'INFO':
            print("updatesql is ="+updatesql)
        try:
            cursor.execute(updatesql)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                  "Update Oracle rownum!=======" + rownum + "\n")
            return self.CONN.commit()
        except Exception as e:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), "Update Oracle failed!=======\n")
            return -995

    def db_insert_batch(self,cursor, batchsql, batchlist):
        if config.logLevel == 'INFO':
            print("batchsql is ="+batchsql)

        if config.logLevel == 'DEBUG':
            for para in batchlist:
                print("para is ="+str(para))
        try:
            cursor.executemany(batchsql,batchlist)
            self.CONN.commit()
            batchnum=len(batchlist)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),"insert_batch Oracle rownum!=======" + str(batchnum) + "\n")
            return batchnum
        except Exception as e:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                  "insert_batch Oracle failed!=======\n")
            return -994

    def db_insert_single(self,cursor, singlesql, myvalue):
        if config.logLevel == 'INFO':
            print("singlesql is ="+singlesql)
            print("myvalue is next="+str(myvalue))
        try:
            cursor.execute(singlesql,myvalue)
            self.CONN.commit()
            return 1
        except Exception as e:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                  "insert_single Oracle failed!=======\n")
            return -993

    def db_insert_singlesome(self,cursor, singlesql, myvalue):
        if config.logLevel == 'INFO':
            print("db_insert_singlesome is ="+singlesql)
            print("myvalue is next="+str(myvalue))
        try:
            cursor.execute(singlesql,myvalue)
            self.CONN.commit()
            return  1
        except Exception as e:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())), e)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                  "db_insert_singlesome Oracle failed!=======\n")
            return -993