"""Pusher component for use in a Node class."""

# Import Built-Ins
import logging
from queue import Queue
from threading import Thread, Event

# Import Third-Party
import zmq

# Import home-grown


# Init Logging Facilities
log = logging.getLogger(__name__)


class Pusher(Thread):
    """
    Allows pushing data to pullers.

    The pushing is realized with ZMQ's Push sockets, and supports pushing
    to multiple pullers.

    The :meth:`hermes.Push.run` method continuously checks for data on the internal q,
    which is fed by the :meth:`hermes.Pusher.push` method.
    """

    def __init__(self, push_addr, name, ctx=None):
        """
        Initialize Instance.

        :param push_addr: Address this instance should connect to
        :param name: Name to give this :class:`hermes.Pusher` instance.
        """
        self.push_addr = push_addr
        self._running = Event()
        self.sock = None
        self.q = Queue()
        self.ctx = ctx or zmq.Context().instance()
        super(Pusher, self).__init__(name=name)

    def publish(self, envelope):
        """
        Publish the given data to all current pullers.

        :param envelope: :class:`hermes.Envelope` instance
        :return: None
        """
        if self.sock:
            self.q.put(envelope)
            return True
        return False

    def stop(self, timeout=None):
        """
        Stop the :class:`hermes.Pusher` instance.

        :param timeout: time in seconds until :exc:`TimeOutError` is raised
        :return: :class:`None`
        """
        log.info("Stopping Pusher instance..")
        self.join(timeout=timeout)
        log.info("..done.")

    def join(self, timeout=None):
        """
        Join the :class:`hermes.Pusher` instance and shut it down.

        Clears the :attr:`hermes.Pusher._running` flag to gracefully terminate the run loop.

        :param timeout: timeout in seconds to wait for :meth:`hermes.Pusher.join` to finish
        :return: :class:`None`
        """
        log.debug("Clearing _running state..")
        self._running.clear()
        log.debug("Closing socket..")
        try:
            self.sock.close()
        except AttributeError:
            log.debug("Socket was already closed!")
            pass
        super(Pusher, self).join(timeout)

    def run(self):
        """
        Custumized run loop to push data.

        Sets up a ZMQ push socket and sends data as soon as it is available
        on the internal Queue at :attr:`hermes.Pusher.q`.

        :return: :class:`None`
        """
        self._running.set()
        ctx = zmq.Context()
        self.sock = ctx.socket(zmq.PUSH)
        log.info("Connecting Pusher to zmq.PULL Socket at %s.." % self.push_addr)
        self.sock.connect(self.push_addr)
        log.info("Success! Executing Pusher loop..")
        while self._running.is_set():
            if not self.q.empty():
                cts_msg = self.q.get(block=False)
                frames = cts_msg.convert_to_frames()
                log.debug("Sending %r ..", cts_msg)
                try:
                    self.sock.send_multipart(frames)
                except zmq.error.ZMQError as e:
                    log.error("ZMQError while sending data (%s), "
                              "stopping Pusher", e)
                    break
            else:
                continue

        ctx.destroy()
        self.sock = None
        log.info("Loop terminated.")

