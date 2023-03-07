
# Exchange ETL Pipline Project  





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


데이터 전처리작업이 synchronization하게  이루어지고있으므로 작업의 밀림현상이 발생할수있다. 이럴경우는 multiprocessing 을 활용해볼수있다.

## 4. Use Rest API vs Websocket
```
Rest API : stateless 하기때문에 recent_trade data 를 가져오기위해서는 잦은 호출을 해야함
반면 
Websocket : 양방향통신으로 데이터에 변화가있을때 데이터의 전송이 흘러들어옴 . 

이번 프로젝트에선 RestAPI 로 구현하고, 추후 Websocket 버젼으로 확장하는 방안으로 한다.
```


### 5. Log Level

log level = INFO level ( DEBUG < INFO < ERROR )  현재 단계에서 INFO 이상레벨만 기록하기로한다. 
