import paramiko
import threading

from paramiko import Transport

'''（1）基于用户名和密码的连接'''
"""
    # 创建SSH对象
    ssh = paramiko.SSHClient()
    # 允许连接不在know_hosts文件中的主机
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # 连接服务器
    ssh.connect(hostname='c1.salt.com', port=22, username='GSuser', password='123')
    # 执行命令
    stdin, stdout, stderr = ssh.exec_command('ls')
    # 获取命令结果
    result = stdout.read()
    # 关闭连接
    ssh.close()

    '''SSHClient 封装 Transport'''

    transport = paramiko.Transport(('hostname', 22))
    transport.connect(username='GSuser', password='123')
    
    ssh = paramiko.SSHClient()
    ssh._transport = transport
    
    stdin, stdout, stderr = ssh.exec_command('df')
    print(stdout.read())
    
    transport.close()
"""

'''（2）基于公钥秘钥连接'''

"""    private_key = paramiko.RSAKey.from_private_key_file('/home/auto/.ssh/id_rsa')
    # 创建SSH对象
    ssh = paramiko.SSHClient()
    # 允许连接不在know_hosts文件中的主机
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # 连接服务器
    ssh.connect(hostname='c1.salt.com', port=22, username='wupeiqi', key=private_key)
    # 执行命令
    stdin, stdout, stderr = ssh.exec_command('df')
    # 获取命令结果
    result = stdout.read()
    # 关闭连接
    ssh.close()
    
    '''SSHClient 封装Transport'''
    private_key = paramiko.RSAKey.from_private_key_file('/home/auto/.ssh/id_rsa')
    transport = paramiko.Transport(('hostname', 22))
    transport.connect(username='wupeiqi', pkey=private_key)
    ssh = paramiko.SSHClient()
    ssh._transport = transport
    stdin, stdout, stderr = ssh.exec_command('df')
    transport.close()
"""


class SSHConnection(threading.Thread):

    def __init__(self, hostname='localhost', port=22, username='zWX1000092', passwd='123abc!'):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.passwd = passwd
        super(SSHConnection, self).__init__()
        self.__transport = None

    def connect(self):
        transport = Transport((self.hostname, self.port))
        transport.connect(username=self.username, password=self.passwd)
        self.__transport = transport

    def upload(self, loacl_path, remote_path):
        sftp = paramiko.SFTPClient.from_transport(self.__transport)
        sftp.put(loacl_path, remote_path)

    def download(self, remote_path, local_path):
        sftp = paramiko.SFTPClient.from_transport(self.__transport)
        sftp.get(remote_path, local_path)

    def cmd(self, command):
        ssh = paramiko.SSHClient()
        ssh.__transport = self.__transport
        stdin, stdout, stderr = ssh.exec_command(command)
        result = stdout.read()
        print(result)
        return result


if __name__ == '__main__':
    obj = SSHConnection()
    obj.connect()
    obj.cmd('ls')
