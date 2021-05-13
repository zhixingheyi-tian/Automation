In order to test the function of the project. We need a pure system environment of CentOS7. But it's cumbersome to reinstall the system everytime.
So we use the Docker to build a image about centOS7. It on need a few seconds to run a container for centOS7 system from an image.There are several steps to build a docker environment for our project.

## Docker configuration for Beaver

1 . Install Docker
```
yum install -y docker
```
2 .If your network need a proxy for connecting to Internet,you may need to set the proxy for docker
```
edit the file /usr/lib/systemd/system/docker.service
Add belows text to it.
Environment=http_proxy=$proxy_url:$proxy_port
Environment=https_proxy=$proxy_url:$proxy_port
Environment=ftp_proxy=$proxy_url:$proxy_port
```
after set the proxy,you should reload daemon
```
systemctl daemon-reload
```

3 . Now you can start docker
```
systemctl start docker
```

4 . Pull an image
```
docker pull centos:centos7.3.1611
```
5 . Build an image by our Dockerfile
```
dockerfile_path=$Beaver_home/test/docker_integration
docker build --rm -t centos7:beaver $dockerfile_path
```
centos7 is the name of the image.
beaver is the TAG of the centos7

6 .Now you can test Beaver

Before you test Beaver by the file of run_beaver_docker.sh . 
You should confirm that you have installed the expect. 
You can install it by command:yum install expect -y
```
sh $Beaver_home/test/docker_integration/run_beaver_docker.sh Beaver centos7:beaver $beaver_path
```
The file of run_beaver_docker.sh will start two container from our image of centos7. And run Beaver on it to build a cluster.
Beaver is the project name.
centos7:beaver is the image name.
$beaver_path is the path of Beaver(It's not the Beaver_HOME)

## Jenkins configuration for Docker
After you have setup jenkins and docker in you system. You can build our project by jenkins automatically.

1 .Login you jenkins page and create a new item(Freestyle project).

2 .You need to do some configuration to your item. You should only modify the configuration item bellows.

### General
![image](https://github.com/intel-hadoop/Beaver/blob/master/test/docker_integration/images/General.PNG)

### source Code Management 
![image](https://github.com/intel-hadoop/Beaver/blob/master/test/docker_integration/images/SourceCode.PNG)

### Build Triggers
![image](https://github.com/intel-hadoop/Beaver/blob/master/test/docker_integration/images/BuildTrigger.PNG)

### Build Environment
![image](https://github.com/intel-hadoop/Beaver/blob/master/test/docker_integration/images/BuildEnvironment.PNG)

### Build
![image](https://github.com/intel-hadoop/Beaver/blob/master/test/docker_integration/images/Build.PNG)
