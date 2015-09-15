# DB-Pulldown
A database pull down tool for MySQL. Copies production databases to development enviroments written in Python

##Requirements
MySQL Client
Python 2.7
	Python mysqldb
Trickle -- for rate limiting requests

##Usage
```
  python db-pulldown.py -d=production_database --sourceDB=master --destDB=local
  
```
