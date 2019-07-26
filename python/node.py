import pika
import nmap

class Node(object):
    def __init__(self,):
        server = "servergoeshere.com"
        passwd = "passwd-goes-here"
        credentials = pika.PlainCredentials("worker",passwd)
        self.parameters = pika.ConnectionParameters(server,5672,'portscan',credentials)

    def _callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body.decode("utf-8"))
        nm = nmap.PortScanner()
        nm.scan(hosts=body.decode("utf-8"),arguments="--open -n -T4 -p1-65535")
        scanresult = nm.csv().split("\n",2)[-1] if len(nm.csv().split()) >= 2 else False
        if scanresult: self.send("subnet-scan-result",scanresult)
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def send(self,topic,msg):
        connection = pika.BlockingConnection(self.parameters)
        channel = connection.channel()
        channel.queue_declare(queue=topic)
        channel.basic_publish(exchange="",routing_key=topic,body=msg)
        connection.close()

    def read(self,topic):
        connection = pika.BlockingConnection(self.parameters)
        channel = connection.channel()
        channel.queue_declare(queue=topic)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(topic,self._callback,auto_ack=False)
        channel.start_consuming()

node = Node()
node.read("subnet-to-scan")
