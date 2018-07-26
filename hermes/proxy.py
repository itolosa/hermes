"""Basic XPub/XSub Proxy Interface for a cluster."""
# pylint: disable=too-few-public-methods

# Import Built-Ins
import logging

# Import Third-Party
import zmq

# Import Homebrew

# Init Logging Facilities
log = logging.getLogger(__name__)


class PostOffice:
    """
    Class to forward subscriptions from publishers to subscribers.

    Uses :const:`zmq.XSUB` & :const:`zmq.XPUB` ZMQ sockets to act as intermediary. Subscribe to
    these using the respective PUB or SUB socket by binding to the same address as
    XPUB or XSUB device.
    """

    def __init__(self, proxy_in, proxy_out, debug_addr=None):
        """
        Initialize a :class:`hermes.PostOffice` instance.

        The addresses used when instantiating these are also the ones your
        publihser and receiver nodes should bind to.

        :param proxy_in: ZMQ Address, including port - facing towards cluster nodes
        :param proxy_out: ZMQ address, including port - facing away from cluster nodes
        :param debug_addr: ZMQ address, including port
        """
        self.xsub_url = proxy_in
        self.xpub_url = proxy_out
        self._debug_addr = debug_addr
        self.ctx = zmq.Context()
        super(PostOffice, self).__init__()

    @property
    def debug_addr(self):
        """Return debug socket's address."""
        return self._debug_addr

    @property
    def running(self):
        """Check if the thread is still alive and running."""
        return self.is_alive()

    def stop(self, timeout=None):
        """Stop the thread.

        :param timeout: timeout in seconds to wait for join
        """
        self.ctx.term()
        self.join(timeout)

    def run(self):
        """
        Serve XPub-XSub Sockets.

        Relays Publisher Socket data to Subscribers, and allows subscribers
        to sub to that data. Offers the benefit of having a single static
        address to connect to a cluster.

        :return: :class:`None`
        """
        ctx = self.ctx

        log.info("Setting up XPUB ZMQ socket..")
        xpub = ctx.socket(zmq.XPUB)
        log.info("Binding XPUB socket facing subscribers to %s..", self.xpub_url)
        xpub.bind(self.xpub_url)

        log.info("Setting up XSUB ZMQ socket..")
        xsub = ctx.socket(zmq.XSUB)
        log.info("Binding XSUB socket facing publishers to %s..", self.xsub_url)
        xsub.bind(self.xsub_url)

        # Set up a debug socket, if address is given.
        if self.debug_addr:
            debug_pub = ctx.socket(zmq.PUB)
            debug_pub.bind(self.debug_addr)
        else:
            debug_pub = None

        log.info("Launching poll loop..")
        try:
            zmq.proxy(xpub, xsub, debug_pub)
        except zmq.error.ContextTerminated:
            xpub.close()
            xsub.close()
            debug_pub.close()
            log.info("Closed sockets, Proxy terminated")


class Streamer:
    """
    Class to forward pushes from pushers to pullers.

    Uses :const:`zmq.PULL` & :const:`zmq.PUSH` ZMQ sockets to act as intermediary. Pull
    these using the respective PUSH or PULL socket by binding to the same address as
    PUSH or PULL device.
    """

    def __init__(self, proxy_in, proxy_out, debug_addr=None):
        """
        Initialize a :class:`hermes.Streamer` instance.

        The addresses used when instantiating these are also the ones your
        publihser and receiver nodes should bind to.

        :param proxy_in: ZMQ Address, including port - facing towards cluster nodes
        :param proxy_out: ZMQ address, including port - facing away from cluster nodes
        :param debug_addr: ZMQ address, including port
        """
        self.pull_url = proxy_in
        self.push_url = proxy_out
        self._debug_addr = debug_addr
        self.ctx = zmq.Context()
        super(Streamer, self).__init__()

    @property
    def debug_addr(self):
        """Return debug socket's address."""
        return self._debug_addr

    @property
    def running(self):
        """Check if the thread is still alive and running."""
        return self.is_alive()

    def stop(self, timeout=None):
        """Stop the thread.

        :param timeout: timeout in seconds to wait for join
        """
        self.ctx.term()
        self.join(timeout)

    def run(self):
        """
        Serve PUSH-PULL Sockets.

        Relays Pusher Socket data to Pullers, and allows pullers
        to pull that data. Offers the benefit of having a single static
        address to connect to a cluster.

        :return: :class:`None`
        """
        ctx = self.ctx

        log.info("Setting up PUSH ZMQ socket..")
        push = ctx.socket(zmq.PUSH)
        log.info("Binding PUSH socket facing pullers to %s..", self.push_url)
        push.bind(self.push_url)

        log.info("Setting up PULL ZMQ socket..")
        pull = ctx.socket(zmq.PULL)
        log.info("Binding PULL socket facing pushers to %s..", self.pull_url)
        pull.bind(self.pull_url)

        # Set up a debug socket, if address is given.
        if self.debug_addr:
            debug_push = ctx.socket(zmq.PUSH)
            debug_push.bind(self.debug_addr)
        else:
            debug_push = None

        log.info("Launching poll loop..")
        try:
            zmq.proxy(push, pull, debug_push)
        except zmq.error.ContextTerminated:
            push.close()
            pull.close()
            debug_push.close()
            log.info("Closed sockets, Proxy terminated")
