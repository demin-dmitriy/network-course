__author__ = 'Demin Dmitriy'

from functools import total_ordering
from PySide import QtCore, QtGui, QtUiTools
import time
import sys
from uuid import getnode as get_mac
from bisect import insort
import re
import html
from datetime import datetime
import threading
import socket
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('log')


def current_time():
    return int(time.time() * 1000)


def format_time(timestamp):
    return datetime.fromtimestamp(timestamp / 1000).ctime()


def format_mac(mac):
    hex_mac = hex(mac)
    return ':'.join(hex_mac[i:i + 2] for i in range(2, 14, 2))


class NetworkParams:
    udp_port = 1235
    udp_packet_length = 14
    tcp_port = 1236
    tcp_packet_header_length = 18


class UDPBroadcaster:
    interval = 5

    def __init__(self, my_id):
        self.my_id = my_id.to_bytes(6, byteorder='big')

    def __call__(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        log.info('Started udp broadcasting on port {}'.format(NetworkParams.udp_port))
        while True:
            timestamp = current_time().to_bytes(8, byteorder='big')
            msg = timestamp + self.my_id
            assert len(msg) == NetworkParams.udp_packet_length
            s.sendto(msg, ('255.255.255.255', NetworkParams.udp_port))
            time.sleep(UDPBroadcaster.interval)


class UDPListener:
    class Client:
        def __init__(self, messages_sent, in_progress=False):
            self.messages_sent = messages_sent
            self.in_progress = in_progress

    def __init__(self, messages, my_id):
        self._clients = {}
        self._messages = messages
        self._my_id = my_id

    def __call__(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(('', NetworkParams.udp_port))
        log.info("Started udp listening on port {}".format(NetworkParams.udp_port))
        while True:
            data, address = s.recvfrom(NetworkParams.udp_packet_length)
            my_time = current_time()
            try:
                timestamp, mac = UDPListener.parse_udp(data)
                if mac == self._my_id: # Then ignore
                    continue
                log.info("UDP message: ip={}, time={}, mac={}".format(
                    address, format_time(timestamp), format_mac(mac)))
                if mac not in self._clients:
                    log.info("New client for sending: mac={}".format(format_mac(mac)))
                    self._clients[mac] = UDPListener.Client(0)
                client = self._clients[mac]
                if not client.in_progress and len(self._messages) > client.messages_sent:
                    client.in_progress = True
                    threading.Thread(target=TCPMessageSender(
                        address, self._messages, client, timestamp - my_time)).start()
            except TypeError:
                log.info("Invalid udp message format: ip={}, data={}".format(address, data))

    @staticmethod
    def parse_udp(msg):
        if len(msg) != NetworkParams.udp_packet_length:
            raise TypeError("UDP packet must be exactly {} bytes length".format(
                NetworkParams.udp_packet_length))
        return int.from_bytes(msg[:8], byteorder='big'), \
            int.from_bytes(msg[8:14], byteorder='big')


class TCPMessageSender:
    def __init__(self, address, messages, client, time_diff):
        self.ip, _ = address
        self.messages = messages
        self.client = client
        self.time_diff = time_diff

    def __call__(self):
        try:
            for msg in self.messages[self.client.messages_sent:]:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.ip, NetworkParams.tcp_port))
                s.sendall(self.prepare_msg(msg))
                self.client.messages_sent += 1
                log.info("Sent message ({}...) to {}. Thread {}".format(
                    msg.text[:15], self.ip, threading.current_thread()))
        finally:
            self.client.in_progress = False

    def prepare_msg(self, msg):
        encoded_text = msg.text.encode('utf-8')
        return (msg.timestamp + self.time_diff).to_bytes(8, byteorder='big') \
            + msg.author.mac.to_bytes(6, byteorder='big') \
            + len(encoded_text).to_bytes(4, byteorder='big') \
            + encoded_text


class TCPListener:
    backlog = 5

    def __init__(self, chat_app):
        self.chat_app = chat_app
        self.chat_clients = {}

    def __call__(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', NetworkParams.tcp_port))
        s.listen(TCPListener.backlog)
        log.info('Listening on tcp port {}'.format(NetworkParams.tcp_port))
        while True:
            threading.Thread(target=self.process_client, args=s.accept()).start()

    def process_client(self, client_socket, address):
        log.info('Receiving messages from {}'.format(address))
        header = TCPListener.recvn(client_socket, NetworkParams.tcp_packet_header_length)
        if not header:
            return
        timestamp, mac, length = TCPListener.parse_message(header)
        message_text = TCPListener.recvn(client_socket, length)
        if not message_text:
            return
        message_text = message_text.decode('utf-8')

        if mac not in self.chat_clients:
            self.chat_clients[mac] = Client(mac)

        client = self.chat_clients[mac]
        self.chat_app.new_message(Message(timestamp, client, message_text))

    @staticmethod
    def parse_message(msg):
        assert len(msg) == NetworkParams.tcp_packet_header_length
        return int.from_bytes(msg[0:8], byteorder='big'), \
            int.from_bytes(msg[8:14], byteorder='big'), \
            int.from_bytes(msg[14:18], byteorder='big')

    @staticmethod
    def recvn(socket, n):
        msg = b''
        while len(msg) < n:
            chunk = socket.recv(n - len(msg))
            if not chunk:
                return None
            msg += chunk
        return msg

@total_ordering
class Message:
    def __init__(self, timestamp, author, text):
        self.timestamp = timestamp
        self.text = text
        self.author = author

    def __str__(self):
        return "<b>[{}]</b> <font color=\"blue\">{}</font>: {}". \
            format(html.escape(format_time(self.timestamp)),
                   html.escape(str(self.author)),
                   html.escape(self.text))

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __eq__(self, other):
        return self.timestamp == other.timestamp


class Client:
    def __init__(self, mac, nickname=None):
        self.mac = mac
        assert nickname is None or type(nickname) == str
        self._nickname = nickname

    def get_nickname(self):
        if self._nickname is None:
            return "<" + format_mac(self.mac) + ">"
        return self._nickname

    def set_nickname(self, new_name):
        self._nickname = new_name

    def __str__(self):
        return self.get_nickname()


class ChatApp(QtCore.QObject):
    ui_file = "chat_window.ui"
    new_message_signal = QtCore.Signal(Message)

    def __init__(self):
        super(ChatApp, self).__init__()
        loader = QtUiTools.QUiLoader()
        f = QtCore.QFile(ChatApp.ui_file)
        try:
            f.open(QtCore.QFile.ReadOnly)
            self._form = loader.load(f)
        finally:
            f.close()
        self.mac = get_mac()
        self.my_client = Client(self.mac)
        self._chat_text = self._form.findChildren(QtGui.QTextEdit, 'ChatText')[0]
        self._message_text = self._form.findChildren(QtGui.QTextEdit, 'MessageText')[0]
        self._send_button = self._form.findChildren(QtGui.QPushButton, 'SendButton')[0]
        self._form.finished.connect(self.on_close)
        self._send_button.clicked.connect(self.send_message)
        self.new_message_signal.connect(self._process_message)
        self._messages = []
        self._my_messages = []
        self._form.show()
        threading.Thread(target=UDPBroadcaster(self.mac), daemon=True).start()
        threading.Thread(target=UDPListener(self._my_messages, self.mac), daemon=True).start()
        threading.Thread(target=TCPListener(self), daemon=True).start()

    def send_message(self):
        msg = Message(current_time(), self.my_client, self._message_text.toPlainText())
        self._my_messages.append(msg)
        self.new_message(msg)
        self.clear_message_text()

    def _process_message(self, message):
        nickname = re.match('Nick: (.*)$', message.text)
        if nickname:
            message.author.set_nickname(nickname.group(1))
        insort(self._messages, message)
        self.refresh_chat()

    def new_message(self, message):
        self.new_message_signal.emit(message)

    def clear_message_text(self):
        self._message_text.clear()

    def refresh_chat(self):
        text = "".join(str(msg) + "<br/>" for msg in self._messages)
        self._chat_text.setHtml(text)

    def on_close(self, exit_code):
        pass


if __name__ == "__main__":
    app = QtGui.QApplication(sys.argv)
    chat = ChatApp()
    sys.exit(app.exec_())
