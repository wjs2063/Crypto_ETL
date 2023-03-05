## 서버구축



#### Mongo DB

도커 hub 에서 mongodb latest version 
````

docker run  --name crypto_mongo -d -p 45000:27017 -it -v /Users/pn_jh/Desktop/fastapiserver:/desktop mongo
````






#### Flask 서버

STEP 1

````
docker pull centos:7

docker run  --name crypto-flask -d -p 35000:8500 -it -v /Users/pn_jh/Desktop/crypto:/desktop centos:7



````

STEP 2
````
yum -y install gcc openssl-devel bzip2-devel libffi-devel vim

yum -y install make Debian

yum -y install wget

wget https://www.python.org/ftp/python/3.9.16/Python-3.9.16.tgz

tar xzf Python-3.9.16.tgz

cd Python-3.9.16

./configure --enable-optimizations

make altinstall


````

STEP 3
````
vi .bashrc
alias python=/usr/local/bin/python3.9


````

STEP 4
````
source .bashrc
````

STEP 5
````
pythom -m pip install requirements.txt
````


#### Docker container Image 
```
docker run  --name binance -d -p 7777:7777 -it -v /Users/pn_jh/Desktop/crypto:/desktop flask-server
docker run  --name bybit -d -p 8888:8888 -it -v /Users/pn_jh/Desktop/crypto:/desktop flask-server
docker run  --name bitmex -d -p 9999:9999 -it -v /Users/pn_jh/Desktop/crypto:/desktop flask-server
```
