import sys, os, tempfile, logging, time
if sys.version_info >= (3,):
    import urllib.request as urllib2
    import urllib.parse as urlparse
else:
    import urllib2
    import urlparse
    import MySQLdb
    import threading
    import hashlib
    import Queue
def mysql_con(_host,_user,_pass,_db):
    try:
        file_object = open('dbcont.txt', 'a')
        file_object.writelines(str(time.time())+'__\n')
        file_object.close()
        conn = MySQLdb.Connect(host=_host, user=_user, passwd=_pass, db=_db ,charset='utf8')
        cursor = conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    except Exception, err:
        print err
    return cursor
CUR = mysql_con('localhost','root','123','dw')
queue = Queue.Queue()
mylock = threading.RLock()
class HttpGetThread(threading.Thread):
    def __init__(self,thread_type):
        threading.Thread.__init__(self)
	self.thread_type = thread_type
    def run(self):
	if self.thread_type == 'getdata':
	    getDate()
	else:
	    while True:
		    count = queue.qsize()
		    if count > 0:
			rows = queue.get()
			#file_name = hashlib.new("md5", str(rows["id"])).hexdigest()
			global mylock
			if mylock.acquire(1):
			    file_name = str(rows["id"])+'.ipa'
			    download_file(rows["dwonload_yueyu"],'ipa',file_name,rows["id"])
			    mylock.release()
		     else:
			 break

def update_status(status,id):
   file_object = open('thefile.txt', 'a')
   file_object.writelines(str(id)+'__'+str(status)+'__\n')
   file_object.close()
   sql = "UPDATE  `dw`.`t_xy_pp_iphone_singleurl` SET  `status` =  '%s' WHERE  `t_xy_pp_iphone_singleurl`.`id` = %s" %(status,id)
   CUR.execute(sql)
def getData():
    while True:
        sql = 'SELECT id,dwonload_yueyu FROM  `t_xy_pp_iphone_singleurl` where status = 0 GROUP BY id order by size asc LIMIT 0 , 300'
        count = CUR.execute(sql)
        rows = CUR.fetchall()
	if count > 0:
	    for i in range(count):
		update_status(2,rows[i]["id"])
		queue.put(row[i])
	else:
	    break 


def download_file(url, dir,desc,id):
    req = urllib2.Request(url)
    req.add_header('User-Agent', 'Mozilla/4.0 (comatible; MSIE 8.0; Win32)')
    req.add_header('Connection', 'Keep-Alive')
    req.add_header('Cache-Control', 'no-cache')
    try:
        u = urllib2.urlopen(req)
    except Exception, err:
        update_status(4,id)
        print url+'|[404]'
        return
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
    if desc:
        filename = desc
    else :
        filename = os.path.basename(path)
    if not filename:
        filename = 'downloaded.file'
    if not os.path.exists(dir):
        os.mkdir(dir)
    filename = dir+'/'+filename

    with open(filename, 'wb') as f:
        meta = u.info()
        meta_func = meta.getheaders if hasattr(meta, 'getheaders') else meta.get_all
        meta_length = meta_func("Content-Length")
        file_size = None
        if meta_length:
            file_size = int(meta_length[0])
        print("Downloading: {0} Bytes: {1}".format(url, file_size))
        file_size_dl = 0
        block_sz = 8192
        while True:
            try:
                buffer = u.read(block_sz)
                if not buffer:
                    break
                file_size_dl += len(buffer)
                f.write(buffer)
                status = "{0:16}".format(file_size_dl)
                if file_size:
                    status += "\r[{0:6.2f}%] ".format(file_size_dl * 100 / file_size)
                status += chr(13)
                sys.stdout.write(status)
                sys.stdout.flush()
            except Exception, err:
                update_status(4,id)
                print url+'|[404]'
                break
    update_status(3,id)
    return filename

def init():
       s = HttpGetThread('getdata')
       s.start()
       threads = []
       for i in range(1):
            t = HttpGetThread('set')
            t.start()
            threads.append(t)
       for t in threads:
            t.join(1)

if __name__ == '__main__':
    init()


