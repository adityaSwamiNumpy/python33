from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from db_dump import DbConnector
from data_from_csv import get_csv_mail_dump,get_latest_file,clean_dir

class Handler(FileSystemEventHandler):
    def on_any_event(self,event):
        try:
            db = DbConnector()
            cursor=db.db_connection_on('gmail_records')#returns cursor
            print("connection successfull")
            path_file=get_csv_mail_dump()#returns latest file in dir
            cursor.execute(f"""BULK INSERT dbo.mails
                            FROM "{path_file}"
                            WITH ( FORMAT = 'CSV')""")
            print("before commit")
            cursor.commit()
            clean_dir()#cleans directory
            print("hello")
        except Exception as e:
            print("failed connection : ",e)
        finally:
            db.db_connection_off()


observer=Observer()
observer.schedule(Handler(),"C:\\Users\\Aditya\\Desktop\\data")#observe this directory for any changes
observer.start()
try:
    while True:
        pass #will keep program running all the time
except KeyboardInterrupt:
    observer.stop() #stop the program on keyboardInterrupt
observer.join()

