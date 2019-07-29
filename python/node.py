import pika
import nmap
from time import sleep
from threading import Thread

def threaded(fn,):
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

class Node(object):
    def __init__(self,):
        server = "you-server.com"
        passwd = "password-goes-here"
        username = "worker"
        credentials = pika.PlainCredentials(username,passwd)
        self.parameters = pika.ConnectionParameters(server,5672,'portscan',credentials)

    def _callback(self, ch, method, properties, body):
        print("[x] Received %r" % body.decode("utf-8"))
        nm = nmap.PortScanner()
        nm.scan(hosts=body.decode("utf-8"),arguments="--open -n -T4 -p1-65535")
        scanresult = nm.csv().split("\n",2)[-1] if len(nm.csv().split()) >= 2 else False
        if scanresult: 
            self.send("scan-result",scanresult)
            print("[x] Message sent") 
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def send(self,topic,msg):
        connection = pika.BlockingConnection(self.parameters)
        channel = connection.channel()
        channel.basic_publish(exchange="",routing_key=topic,body=msg)
        connection.close()

    @threaded
    def read(self,topic):
        connection = pika.BlockingConnection(self.parameters)
        channel = connection.channel()
        channel.queue_declare(queue=topic,auto_delete=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(topic,self._callback,auto_ack=False)
        channel.start_consuming()

node = Node()
topics = ["you","queues","goes","here"]
for topic in topics:
    print(topic)
    node.read(topic)
