## 서버구축



#### Mongo DB

도커 hub 에서 mongodb latest version 
````

docker run  --name crypto_mongo -d -p 45000:27017 -it -v /Users/pn_jh/Desktop/fastapiserver:/desktop mongo
````






#### Flask,Bybit,Binance,Bitmex 동일 

STEP 1

````
docker pull centos:7

docker run  --name flask -d -p container_port:pc_port -it -v /Users/pn_jh/Desktop/crypto:/desktop jahy5352/crypto-server



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
이 단계는 각 bybit,bitmex,binance,flask 폴더 내부의 requirements.txt 를 설치한다.
````
pythom -m pip install requirements.txt
````

### Pulling Docker image


해당 image 로 Bybit,Bitmex,Binance,Flask 도커 컨테이너를 띄운다

````
docker pull jahy5352/crypto-server
````

#### Docker container Image 

```


docker run  --name binance -d -p 7777:7777 -it -v /Users/pn_jh/Desktop/crypto:/desktop jahy5352/crypto-server
docker run  --name bybit -d -p 8888:8888 -it -v /Users/pn_jh/Desktop/crypto:/desktop jahy5352/crypto-server
docker run  --name bitmex -d -p 9999:9999 -it -v /Users/pn_jh/Desktop/crypto:/desktop jahy5352/crypto-server
docker run  --name flask -d -p 11111:11111 -it -v /Users/pn_jh/Desktop/crypto:/desktop jahy5352/crypto-server
```
