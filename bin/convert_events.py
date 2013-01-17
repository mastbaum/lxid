#!/usr/bin/env python
'''ZMQ worker to read and event-ify files.'''

import socket
import sys
import zmq
from lxid.utils import events_from_ds
from lxid.dataset import Cut

def run_worker(host):
    context = zmq.Context()

    # try to connect to a server every 5 seconds
    while True:
        try:
            # get tasks from the ventilator
            receiver = context.socket(zmq.PULL)
            receiver.connect("tcp://%s:5557" % host)

            # send results to the sink
            sender = context.socket(zmq.PUSH)
            sender.connect("tcp://%s:5558" % host)

            # get control signals (kill) from the controller
            controller = context.socket(zmq.SUB)
            controller.connect("tcp://%s:5559" % host)
            controller.setsockopt(zmq.SUBSCRIBE, "")

            break

        except Exception:
            time.sleep(5)
            continue

    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(controller, zmq.POLLIN)

    while True:
        socks = dict(poller.poll())

        if socks.get(receiver) == zmq.POLLIN:
            task = receiver.recv_pyobj()
            filename, cut = task
            print filename
            r = events_from_ds(filename, cut)
            sender.send_pyobj(r)

        # everything is treated as a kill signal
        if socks.get(controller) == zmq.POLLIN:
            break

def main(bin_name, host):
    print '%s on %s' % (bin_name, socket.getfqdn())
    run_worker(host)
    print 'done'

if __name__ == '__main__':
    bin_name = sys.argv[0]
    host = sys.argv[1]
    main(bin_name, host)

