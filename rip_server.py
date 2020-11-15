import signal
import os
import sys
import socket
import select
from threading import Thread, Lock
import struct
import asyncio
import random
import logging
import time


# Configure the format of the logger
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Fix silly windows bug with interrupts
signal.signal(signal.SIGINT, signal.SIG_DFL)


class Route:
    """Routes are designed to be sorted inside a dictionary so they do not hold
    the ID of the destination. There can only be a single stored route for a
    given destination.
    """

    def __init__(self, metric, next_hop):
        """Create a route with a given metric, next hop and interface to reach the
        next hop. The route can store/set/clear the instances of the timers. (To
        prevent access to the event event_loop within the Route object these must be
        called outside of the route)

        Arguments:
            metric {int} -- The cost to use this link from this instance
            next_hop {int} -- The next instance to reach the destination
        """
        self.metric = metric
        self.next_hop = next_hop
        self.timer_task = None
        self.start_time = time.time()
        self.timed_out = False

    def __str__(self):
        """Convert a route to a human readable form.

        Returns:
            string -- Route descriptor
        """
        message = 'DIRECT  ' if self.next_hop == 0 else 'VIA: {: <2} '.format(self.next_hop)
        message += 'COST: {: <2}  '.format(self.metric)
        message += 'GBT' if self.timed_out else 'TOT'
        message += ': {: <2}s'.format(int(time.time() - self.start_time))
        return message


class Router:
    def __init__(self, router_config):
        self.my_id = router_config.routerId
        # Construct the known routers that are directly connected
        self.direct = {node.peer_id: Route(node.metric, 0) for node in router_config.outputs}
        # Initially there is an empty routing table
        self.routing_table = {}
        # Sockets used to listen and send on
        self.inputs = router_config.input_sockets
        self.outputs = router_config.outputs
        # Show the routing table is empty on startup
        logger.info('\n{}'.format(self))

    def __str__(self):
        out_string = 'Routing Table ({})'.format(self.my_id).center(46, '-')
        for key in self.routing_table:
            out_string += '\n'
            out_string += '{: <2} <- ({})'.format(key, self.routing_table[key]).center(43)
        out_string += '\n'
        out_string += ''.center(46, '-')
        return out_string


class Outputs:
    def __init__(self, port_config):
        params = port_config.strip().split('-')
        # Check there is the right amount of params
        if not len(params) == 3:
            raise ValueError('Output "{}" is not valid!'.format(port_config))

        # Parse the values
        self.port = int(params[0])
        self.metric = int(params[1])
        self.peer_id = int(params[2])

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        # Using connect_ex so an exception is not immediately raised
        # and the socket can be used by select()
        self.socket.connect_ex(('127.0.0.1', self.port))


class Configuration:
    def __init__(self, filename):
        # Get the full path to the config file and check
        # if it exists
        config_path = os.path.join(os.path.dirname(__file__), filename)
        if not os.path.isfile(config_path):
            raise FileNotFoundError('"{}" does not exist!'.format(config_path))

        # Read from the file
        with open(config_path) as configFile:

            for line in configFile:
                # Get the key/value pair defined by the first white space
                # then parse the value
                key, value = line.split(' ', 1)
                if key == 'router-id':
                    # Load/Parse host-route_id
                    self.routerId = int(value)
                    if self.routerId < 1 or self.routerId > 64000:
                        raise ValueError(
                            'host route_id "{}" is not between 1 and 64000'.format(self.routerId))
                elif key == 'input-ports':
                    # Load/Parse input-ports
                    self.inputPorts = []
                    for entry in value.strip().split(','):
                        port = int(entry)
                        # ports lower than 1024 are considered privileged ports and
                        # can only be opened by the root user
                        if port < 1024 or port > 64000:
                            raise ValueError(
                                'Input port "{}" is not between 1024 and 64000')
                        # Check to prevent duplicate ports being entered
                        if not self.inputPorts.count(port) == 0:
                            raise ValueError(
                                'Cannot contain duplicate port "{}"'.format(port))
                        self.inputPorts.append(port)
                    self.input_sockets = []
                    for port in self.inputPorts:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        sock.bind(('127.0.0.1', port))
                        self.input_sockets.append(sock)

                elif key == 'outputs':
                    # Load/Parse outputs
                    self.outputs = []
                    for entry in value.strip().split(','):
                        output = Outputs(entry)
                        # Check that the port is not defined in both input and output
                        if not self.inputPorts.count(output.port) == 0:
                            raise ValueError(
                                'Port "{}" is defined in both the input and output lists')
                        self.outputs.append(output)
                else:
                    raise ValueError(
                        'Invalid key "{}" in configuration!'.format(key))


async def tx_handler(instance: Router, instance_lock: Lock):
    """Handler that provides periodic response packets to
    all directly connected routers, with a minor variation in the
    delay time. A instance_lock is required to provide thread safety to the
    underlying instance object that is being transmitted.

    Arguments:
        instance {Router} -- The shared object that describes this instance instance.
        instance_lock {Lock} -- Used to provide threadsafety to the instance object.
    """
    # The main event_loop for this thread
    while True:
        # Send the routing table every 10 +/- 1 second
        await asyncio.sleep(10 + random.randint(-1, 1))
        with instance_lock:
            # logger.info('Sending periodic update')
            tx_message(instance)
            logger.info('\n{}'.format(instance))


def tx_message(instance: Router):
    """Sends an unsolicited response packet to all directly connected routers. Constructs
    the correct packet header, and creates a dynamic view of the routing table for
    each connected instance according to the split horizon poison reverse.

    Thread safety is not handled within this method and has to be provided by the caller.

    Arguments:
        instance {Router} -- The shared object that describes this instance instance.
    """
    # Prepare the view for each instance we are directly connected to
    for connected_router in instance.outputs:
        # Construct the header for the response message
        response = struct.pack('!BBH', 2, 2, instance.my_id)
        for route_id in instance.routing_table:
            # Dont send a instance a route to itself
            if connected_router.peer_id == route_id:
                continue
            response += struct.pack('!HHLLL', 0, 0, route_id, 0, 0)
            # Check if the next hop on the route is the instance this message is being sent to.
            if instance.routing_table[route_id].next_hop == connected_router.peer_id:
                # Poison the route with a cost of 16
                response += struct.pack('!L', 16)
            else:
                response += struct.pack('!L', instance.routing_table[route_id].metric)
        # Send the response message to the instance
        try:
            connected_router.socket.sendall(response)
        except socket.error:
            # Its possible that the instance on the receiving end might
            # not be listening
            pass


async def timer_handler(route_id: int, instance: Router, instance_lock: Lock, force=False):
    """Handler for the timeout and garbage-collection services of a route. Waits for
    the timeout timer to elapse then sets the routes metric to 16 and sends a triggered
    response packet to all connected routers. The garbage-collection timer is then
    started and upon its expiry the route is removed. This handler can be cancelled at anytime
    and the timers will be cleared.

    Arguments:
        route_id {int} -- ID of the route being managed
        instance {Router} -- The instance object for this instance
        instance_lock {Lock} -- Used to sync access to the instance object

    Keyword Arguments:
        force {bool} -- Skip waiting for the timeout timer and trigger now (default: {False})
    """
    # Initial wait time for the timeout event
    try:
        if not force:
            instance.routing_table[route_id].start_time = time.time()
            instance.routing_table[route_id].timed_out = False
            await asyncio.sleep(60)
        # Mark the route as unreachable.
        with instance_lock:
            instance.routing_table[route_id].metric = 16
            logger.info('\n{}'.format(instance))
        # At this point the route has timed out so even if the timer is
        # cancelled we need to send the update
        with instance_lock:
            # logger.info('Sending triggered response')
            tx_message(instance)
        # Now wait before we remove the route from the routing table
        instance.routing_table[route_id].start_time = time.time()
        instance.routing_table[route_id].timed_out = True
        await asyncio.sleep(40)
        # Delete the key from the routing table
        with instance_lock:
            del instance.routing_table[route_id]
            logger.info('\n{}'.format(instance))
    # The task was cancelled - The route timer has been reset
    except asyncio.CancelledError:
        return


def cancel_old_timer(route: Route):
    if route.timer_task is not None:
        route.timer_task.cancel()


def set_route_timer(route_id: int, instance: Router, instance_lock: Lock, force=False):
    """Sets the timers for a route.

    Arguments:
        route_id {int} -- ID of the route
        instance {Router} -- The instance object for this instance
        instance_lock {Lock} -- Used to sync access to the instance object

    Keyword Arguments:
        force {bool} -- Skip the timeout timer and trigger now (default: {False})
    """
    route: Route = instance.routing_table[route_id]
    # Cancel the existing task if it is present
    cancel_old_timer(route)
    # Create and set the new timer task
    route.timer_task = asyncio.get_event_loop().create_task(
        timer_handler(route_id, instance, instance_lock, force))


class RxHandleThread(Thread):
    def __init__(self, instance: Router, instance_lock: Lock, event_loop: asyncio.AbstractEventLoop):
        """Initialise the background thread for handling incomming packets.

        Arguments:
            instance {Router} -- The shared object that describes this instance instance.
            instance_lock {Lock} -- Used to provide threadsafety to the instance object.
            event_loop {asyncio.AbstractEventLoop} -- The event event_loop from the calling thread.
        """
        Thread.__init__(self)
        self.instance = instance
        self.lock = instance_lock
        self.loop = event_loop

    def run(self):
        # Set the event event_loop to the event event_loop of the main thread.
        # This prevents select.select() from blocking the event event_loop.
        # As select.select() is called on a child thread it will not
        # block the parent thread.
        asyncio.set_event_loop(loop)
        # Main event_loop for handling incoming response packets.
        while True:
            readable_sockets, _, _ = select.select(self.instance.inputs, [], [])
            for sock in readable_sockets:
                self.__process_response(sock)

    def __process_response(self, sock: socket.socket):
        data = sock.recv(1024)
        if data:
            command, version, src_id = struct.unpack(
                "!BBH", data[:4])

            # Validate the command and version are correct
            if not command == 2:
                return
            if not version == 2:
                return

            # We have received a packet from a directly connected instance. Check if it
            # in our routing table.
            if src_id not in self.instance.routing_table:
                #  Now find and add the directly connected route to the routing table
                for node in self.instance.outputs:
                    if node.peer_id == src_id:
                        with self.lock:
                            self.instance.routing_table[src_id] = Route(node.metric, 0)
                        break
            elif self.instance.routing_table[src_id].next_hop != src_id:
                cancel_old_timer(self.instance.routing_table[src_id])
                for node in self.instance.outputs:
                    if node.peer_id == src_id:
                        with self.lock:
                            self.instance.routing_table[src_id] = Route(node.metric, 0)
                        break
            # Set / Reset the timer for the directly connected route
            set_route_timer(src_id, self.instance, self.lock)

            # Read the routes that are contained in the response message
            data = data[4:]
            while data:
                _, _, dst_id, _, _, metric = struct.unpack('!HHLLLL', data[:20])
                data = data[20:]
                self.__update_routing_table(src_id, metric, dst_id)
            logger.info('\n{}'.format(self.instance))

    def __update_routing_table(self, src_id, metric, dst_id):
        # Get the instance_lock so the state doesnt change
        with self.lock:
            # Calculate the total metric to get to the destination
            route_metric = metric + self.instance.routing_table[src_id].metric

            # Check if we dont have a route in the table
            if dst_id not in self.instance.routing_table:
                if route_metric < 16:
                    self.instance.routing_table[dst_id] = Route(
                        route_metric, src_id)
                    set_route_timer(dst_id, self.instance, self.lock)
                return

            # There is already a route to this destination, check if we need to replace the metric or
            # this is a new route with a lower cost
            min_route = self.instance.routing_table[dst_id]
            # Check if this new route is an updated cost for our current route
            if src_id == min_route.next_hop:
                if route_metric < 16:
                    old_metric = self.instance.routing_table[dst_id].metric
                    self.instance.routing_table[dst_id].metric = route_metric
                    set_route_timer(dst_id, self.instance, self.lock)
                    return
                else:
                    set_route_timer(dst_id, self.instance, self.lock, True)
                    return

            # This is a route being offered to a dst we currently have a route to
            # but by a different path
            # Check if we should change our route
            if route_metric < min_route.metric:
                set_route_timer(dst_id, self.instance, self.lock)
                self.instance.routing_table[dst_id].metric = route_metric
                self.instance.routing_table[dst_id].next_hop = src_id


if __name__ == '__main__':
    # Check if there were the right number of arguments
    # passed to the program
    if not len(sys.argv) == 2:
        logger.critical('configuration file was not given!')
        exit(1)

    # Try load the configuration from file
    try:
        config_raw = Configuration(sys.argv[1])
    except Exception as error:
        logger.critical(error)
        exit(1)
    else:
        router = Router(config_raw)

        # Create the shared instance_lock
        lock = Lock()

        loop = asyncio.get_event_loop()

        # Start the thread that processes incoming pkts
        rx = RxHandleThread(router, lock, loop)
        rx.start()

        # Create the event event_loop and start sending
        loop.create_task(tx_handler(router, lock))

        loop.run_forever()
