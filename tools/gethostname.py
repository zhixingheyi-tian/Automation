import threading
import paramiko
from paramiko import AuthenticationException


def gethostname(ip, user="root", password="bdpe123", bakpassword="intel123", bakpassword2="Intel123!"):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())


        try:
            client.connect(hostname=ip, port=22, username=user, password=password, auth_timeout=5)
        except AuthenticationException as e:
            try:
                client.connect(hostname=ip, port=22, username=user, password=bakpassword, auth_timeout=5)
            except AuthenticationException as e:
                client.connect(hostname=ip, port=22, username=user, password=bakpassword2, auth_timeout=5)

        ssh_session = client.get_transport().open_session()
        if ssh_session.active:
            ssh_session.exec_command("hostname")
        res = ssh_session.recv(1024)

        client.close()
        print(ip + " : " + res.decode('utf-8'))
        return res.decode('utf-8')
    except:
        return "error"
if __name__ == "__main__":
    for i in range(1, 255):
        ip = "10.239.44." + str(i)
        t = threading.Thread(target=gethostname, args=(str(ip),))
        t.start()

