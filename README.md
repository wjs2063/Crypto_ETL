
# Exchange ETL Pipline Project  

## Getting start 

아래 코드를 이용하여 binance,bybit,bitmex,flask 서버를 개설한다. Database 는 mongo:latest 를 이용한다

마운트할 디렉토리를 지정하고 로컬디렉토리와 컨테이너 디렉토리를 연결한다. 공유디렉토리  

각 서버의 목적에 맞는 git repository 의 폴더를 넣어준다. 예를들어 binance 면 해당 reposit 의 binance 폴더를 넣어주면된다.  




````
docker pull jahy5352/crypto-server

docker run  --name [원하는이름] -d -p [로컬포트]:[컨테이너포트] -it -v /Users/pn_jh/Desktop/crypto:/desktop jahy5352/crypto-server



예시

docker run  --name binance -d -p 5555:5555 -it -v /Users/pn_jh/Desktop/crypto:/desktop jahy5352/crypto-server

docker pull mongo:latest
docker run  --name mongodb -d -p 9999:27017 -it -v /Users/pn_jh/Desktop/crypto:/desktop mongo
````

* 이전 작업을 통하여 컨테이너를 생성했다면 컨테이너에 접속하여 각 서버에 맞는 library 들을 설치한다

````
python -m pip install -r requirements.txt
````


* Directory/File explanation
예시 (Binance) 이하동일(bybit,bitmex)

    - logs : 로그파일 기록하는 폴더  
    - constant.py : 사용자 정의상수 기록 파일
    - database.py : database 관련 파일 
    - requriements.txt : 설치 및 필요한  library 정보
    - binance.py : binance_API 로부터 ETL 작업 수행하는 코드 

<br></br>



1. Binance, Bybit, Bitmex 거래데이터를 1분단위로 집계하여 데이터베이스에 저장하는 ETL 파이프라인 구축
2. Flask 를 이용하여 start_date <= time < end_date 에있는 분단위 데이터를 추출하여 API 로 서빙

* Docker Container 를 총 5대 띄운다 ( Binance,Bybit,Bitmex, Flask , Mongodb ) 

* 각각의 거래소 및 Flask 컨테이너 내부에 Binance,Bybit,Bitmex,Flask 폴더를 둔다. (현재는 repository에 합쳐놓음) 



각각의 거래소 컨테이너 내부에서 아래 코드를 실행한다

```
python [exchange_name].py 

예시

python binance.py
```


* Flask 
http://localhost:8888 -> Swagger docs page -> API test 

````
flask 폴더로 들어가 flask run --host=0.0.0.0 --port=8888
````



## 1. Directory Sturcture
    dir_structure.txt 에 디렉토리 구조 저장







## 2 . Server Architecture 
````

  __기술스택__ : Docker, Centos7, Mongodb ,Flask, Python 

  __OS__ : Centos:7 

  __Server__ : Binance,Bybit,Bitmex,Flask  

  __Database__ : NoSQL (mongo database) 
````

Architecture.pdf -> [프로젝트아키텍쳐](https://github.com/wjs2063/Crypto_ETL/blob/main/%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8%20%EC%95%84%ED%82%A4%ED%85%8D%EC%B3%90.pdf)


## 3. Flask Server with Swagger
````
Swagger 를 사용하여 API 관리가 쉽도록 함.
````

### Why Mongodb ?

프로젝트 목적상 분당 데이터 추출(Extract) -> 데이터 집계 (Transform)  -> DB 저장 (Load) -> 데이터 질의(DML) 이기떄문에     
데이터 단순적재 + 추가적으로 database scale out 을 감안한다면 Nosql 이 편할것으로 판단함 .  조사한결과 Mongodb 가 DB 샤딩(밸런싱기능) 등의 지원도 준수한편      


데이터 전처리작업이 synchronization하게  이루어지고있으므로 작업의 밀림현상이 발생할수있다. 이럴경우는 Asyncc or multiprocessing 을 활용해볼수있다.

## 4. Use Rest API vs Websocket
```
Rest API : stateless 하기때문에 recent_trade data 를 가져오기위해서는 잦은 호출을 해야함
반면 
Websocket : 양방향통신으로 데이터에 변화가있을때 데이터의 전송이 흘러들어옴 . 

이번 프로젝트에선 RestAPI 로 구현하고, 추후 Websocket 버젼으로 확장하는 방안으로 한다.
```


### 5. Log Level

log level = INFO level ( DEBUG < INFO < ERROR )  현재 단계에서 INFO 이상레벨만 기록하기로한다. 


### 6. Flow chart 

<img width="400" alt="crypto_ETL Flow chart" src="https://user-images.githubusercontent.com/76778082/223666950-8a4d7aa4-966f-411b-b351-d1d054abe0bf.png">


[프로젝트설명](https://github.com/wjs2063/Crypto_ETL/tree/main/%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8%20%EC%84%A4%EB%AA%85)

### 7. Data schema


[Database Schema.pdf](https://github.com/wjs2063/Crypto_ETL/files/10918916/Database.Schema.pdf)





#### 참고 Documentation  

https://docs.python.org/ko/3/library/asyncio.html
https://pandas.pydata.org/docs/user_guide/index.html#user-guide

https://motor.readthedocs.io/en/stable/api-tornado/motor_client.html    
https://www.mongodb.com/docs/manual/tutorial/getting-started/  

https://docs.pydantic.dev/usage/types/  
https://flask-restx.readthedocs.io/en/latest/swagger.html  



