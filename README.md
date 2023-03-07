
# Exchange ETL Pipline Project  





## 1. Directory Sturcture 
dir_structure.txt 에 디렉토리 구조 저장



Server : Binance,Bybit,Bitmex,Flask 

Database : NoSQL (mongo database) 




### Why Mongodb ?

프로젝트 목적상 분당 데이터 추출(Extract) -> 데이터 집계 (Transform)  -> DB 저장 (Load) -> 데이터 질의(DML) 이기떄문에   
데이터 단순적재  추가적으로 database scale out 을 감안한다면 Nosql 이 편할것으로 판단함 .  조사한결과 Mongodb 가 DB 샤딩(밸런싱기능) 등의 지원도 준수한편   



