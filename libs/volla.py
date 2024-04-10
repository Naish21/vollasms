import paramiko
from datetime import datetime
import uuid


class Volla:
    """Class to connect to the Vollaphone connect to SSH and send SMS"""

    def __init__(self):
        """Initialize the SSH session"""
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.host = None
        self.username = None
        self.__password = None

    def connect(self, host, username, password) -> None:
        """Connect to a SSH host"""
        self.host = host
        self.username = username
        self.__password = password
        self.ssh.connect(hostname=host, username=username, password=password, look_for_keys=False, allow_agent=False)

    def __reconnect(self) -> None:
        """Reconnect to the SSH host"""
        self.__disconnect()
        self.connect(self.host, self.username, self.__password)

    def __disconnect(self) -> None:
        """Disconnet from the SSH host"""
        self.ssh.close()

    def test(self) -> bool:
        """Test if the SSH connection is alive and active"""
        try:
            status = self.ssh.get_transport()
            if 'cipher aes128-ctr, 128 bits' in str(status) and 'active' in str(status):
                return True
            elif 'unconnected' in str(status) or status is None:
                return False
        except paramiko.SSHException:
            self.__disconnect()
            return False
        except Exception:
            self.__disconnect()
            return False
        return False

    @staticmethod
    def build_response(outcome: str, phonenumber: str, _id: str, date_on: datetime, message: str) -> dict:
        if _id is None:
            _id = str(uuid.uuid4()).replace('-', '')
        return {
            'outcome': outcome,
            'phone': phonenumber,
            'id': _id,
            'on': date_on,
            'message': message,
        }

    def send_sms(self, phonenumber: int, message: str) -> dict:
        """Send an SMS through the SSH connection"""
        if not self.test():
            self.__reconnect()
        if len(message) > 160:
            return self.build_response('MESSAGE_TOO_LONG', phonenumber, None, datetime.now(), message)
        try:
            ssh_stdin, ssh_stdout, ssh_stderr = self.ssh.exec_command(
                f'/usr/share/ofono/scripts/send-sms /ril_0 {phonenumber} "{message}" 0')
        except (paramiko.SSHException, ConnectionResetError):
            self.__disconnect()
            return self.build_response('SSH_SESSION_LOST', phonenumber, None, datetime.now(), message)
        _id = ''
        if len(ssh_stderr.readlines()) > 0:
            return self.build_response('ERROR_SENDING', phonenumber, None, datetime.now(), message)
        try:
            ans = ssh_stdout.readlines()
            _id = ans[-1].strip().split('message_')[-1]
        except (NameError, KeyError, IndexError, AttributeError):
            return self.build_response('ERROR_SENDING', phonenumber, None, datetime.now(), message)
        if message == '':
            return self.build_response('UNEXPECTED', phonenumber, _id, datetime.now(), message)
        return self.build_response('OK', phonenumber, _id, datetime.now(), message)
