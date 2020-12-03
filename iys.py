from http.client import HTTPSConnection
from json import dumps, loads
import sql
from config import Config
from time import sleep

c = Config()
IYS = sql.Sql("#")
phone_q = "concat('+90',(right(#,10)))"


class iys:
    def __init__(self, table, id):
        self.engine = sql.create_engine(IYS.connString)
        self.conn = self.engine.connect()
        table = str(table)
        if table == "#1":
            self.table = table
            self.type = "ARAMA"
            self.idName = "#1"
        elif table == "#2":
            self.table = table
            self.type = "MESAJ"
            self.idName = "#2"
        elif table == "#3":
            self.table = table
            self.type = "EPOSTA"
            self.idName = "#3"
        self.iysid = "{} = {}".format(self.idName, id)
        self.id = id
        self.content = {"Content-type": "application/json"}
        self.consent = IYS.select("#", table, self.iysid, self.conn).strftime("%Y-%m-%d %H:%M:%S")
        if self.consent <= "2015-05-01 00:00:00":
            self.source = "HS_2015"
            self.consent_date = "2015-05-01 00:00:00"
        else:
            self.source = "HS_WEB"
            self.consent_date = self.consent
        if table == "#1" or table == "#2":
            self.recipient = IYS.select(phone_q, table, self.iysid, self.conn)
        elif table == "#3":
            self.recipient = str(IYS.select("#", table, self.iysid, self.conn))
        self.status = "ONAY"

    def auth(self):
        connection = HTTPSConnection('api.iys.org.tr', timeout=60)
        payload = {
            "username": c.username,
            "password": c.password,
            "grant_type": "password"
        }

        connection.request('POST', '/oauth2/token', dumps(payload), self.content)
        res = connection.getresponse()
        if res.status == 200:
            response = loads(res.read().decode("utf-8"))
            token = {"Content-type": "application/json",
                     "Authorization": "Bearer {}".format(str(response.get("access_token")))}
            return token
        else:
            print("Retrying")
            sleep(3)
            self.auth()

    def transfer(self, HeaderAuth):
        connection = HTTPSConnection('api.iys.org.tr', timeout=60)
        payload = {
            "consentDate": self.consent_date,
            "source": self.source,
            "recipient": self.recipient,
            "recipientType": "BIREYSEL",
            "status": self.status,
            "type": self.type
        }
        connection.request('POST', '/sps/{}/brands/{}/consents'.format(c.iysCode, c.brandCode), dumps(payload),
                           HeaderAuth)
        res = connection.getresponse()
        if res.status == 200:
            response = loads(res.read().decode("utf-8"))
            IysTransferDate = str(response["creationDate"])
            IsErrored = 0
            IysTransactionID = str(response["transactionId"])
            IysTransferred = 1
            IYS.update_one(self.table, self.id, self.idName, "IysTransferDate", IysTransferDate, self.conn)
            IYS.update_one(self.table, self.id, self.idName, "IsErrored", IsErrored, self.conn)
            IYS.update_one(self.table, self.id, self.idName, "IysTransactionID", IysTransactionID, self.conn)
            IYS.update_one(self.table, self.id, self.idName, "IysTransferred", IysTransferred, self.conn)
            IYS.update_one(self.table, self.id, self.idName, "IsErrored", 0, self.conn)
            self.conn.close()
            self.engine.dispose()

        else:
            response = loads(res.read().decode("utf-8"))
            ErrorMessage = response["errors"][0]["message"]
            IsErrored = 1
            IYS.update_one(self.table, self.id, self.idName, "IsErrored", IsErrored, self.conn)
            IYS.update_one(self.table, self.id, self.idName, "ErrorMessage", ErrorMessage.encode("utf-8"), self.conn)
            self.conn.close()
            self.engine.dispose()
