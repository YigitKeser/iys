from iys import iys
import sql

IYS = sql.Sql("#")


if __name__ == '__main__':
    engine = sql.create_engine(IYS.connString)
    conn = engine.connect()
    phoneCount = IYS.select("count(*)", "#1", "IysTransferred = 0 and IsErrored = 0" , conn)
    SMSCount = IYS.select("count(*)", "#2", "IysTransferred = 0 and IsErrored = 0", conn)
    MailingCount = IYS.select("count(*)", "#3", "IysTransferred = 0 and IsErrored = 0", conn)
    counter = 0
    while True:
        if phoneCount > 0:
            HeaderAuth = iys("#1", "#").auth()
            PhoneCallID = IYS.select("#", "#", "IysTransferred = 0 and IsErrored = 0 limit 1",conn)
            iys("#", "#").transfer(HeaderAuth)
            counter += 1
            print("Counter :{}".format(counter))
        elif SMSCount > 0:
            HeaderAuth = iys("#2", "#").auth()
            SMSID = IYS.select("#", "#", "IysTransferred = 0 and IsErrored = 0 limit 1", conn)
            iys("#", "#").transfer(HeaderAuth)
            counter += 1
            print("Counter :{}".format(counter))
        elif MailingCount > 0:
            HeaderAuth = iys("#", "#").auth()
            MailingID = IYS.select("#", "#", "IysTransferred = 0 and IsErrored = 0 limit 1", conn)
            iys("#", "#").transfer(HeaderAuth)
            counter += 1
            print("Counter :{}".format(counter))
        else:
            print("No Update Needed")
            break
