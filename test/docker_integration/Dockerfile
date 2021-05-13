FROM docker.io/centos:centos7.3.1611
ARG PROXY_URL
ARG PROXY_PORT
ENV PROXY_URL child-prc.intel.com
ENV PROXY_PORT 913
ENV NO_PROXY localhost,127.0.0.1,.intel.com,10.239.47.*
ENV Container Docker
ENV http_proxy http://$PROXY_URL:$PROXY_PORT
ENV ftp_proxy http://$PROXY_URL:$PROXY_PORT
ENV https_proxy https://$PROXY_URL:$PROXY_PORT
ENV no_proxy $NO_PROXY

RUN echo -e "http_proxy=http://$PROXY_URL:$PROXY_PORT\nftp_proxy=http://$PROXY_URL:$PROXY_PORT\nhttps_proxy=https://$PROXY_URL:$PROXY_PORT\nexport http_proxy\nexport ftp_proxy\nexport https_proxy\nexport no_proxy=$NO_PROXY" >> /etc/profile
RUN source /etc/profile

RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == systemd-tmpfiles-setup.service ] || rm -f $i; done); \
rm -f /lib/systemd/system/multi-user.target.wants/* \
/etc/systemd/system/*.wants/* \
/lib/systemd/system/local-fs.target.wants/* \
/lib/systemd/system/sockets.target.wants/*udev* \
/lib/systemd/system/sockets.target.wants/*initctl* \
/lib/systemd/system/basic.target.wants/* \
/lib/systemd/system/anaconda.target.wants/*;

RUN rm /etc/yum.repos.d/* -rf && cd /etc/yum.repos.d/ && curl http://10.239.47.148/docker/os.repo --output os.repo

RUN yum -y upgrade
RUN yum -y update
RUN yum -y install net-tools openssh* git wget make gcc.x86_64 vim expect* zlib* python-paramiko zip

RUN ssh-keygen -q -t rsa -b 2048 -f /etc/ssh/ssh_host_rsa_key -N ''
RUN ssh-keygen -q -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key -N ''
RUN ssh-keygen -t dsa -f /etc/ssh/ssh_host_ed25519_key  -N ''
RUN sed -i "s/#UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config
RUN echo 'Host *' >> /etc/ssh/ssh_config
RUN echo '    StrictHostKeyChecking no' >> /etc/ssh/ssh_config
RUN sed -i "s/UsePAM.*/UsePAM no/g" /etc/ssh/sshd_config
RUN echo "root:bdpe123" | chpasswd

EXPOSE 22
VOLUME [ "/sys/fs/cgroup" ]
CMD ["/usr/sbin/init"]
