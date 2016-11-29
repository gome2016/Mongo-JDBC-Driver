# Mongo jdbc driver

## Prerequisites
1. Maven
2. jdk 1.7 (or higher)
 
## Installation
* Clone git repository
    ```bash
    $ git clone git@github.com:Slemma/Mongo-JDBC-Driver.git mongo-jdbc
    ```
* Install mongodb
* Create database 'test'
* Create collections and import data from source folder 'src/main/data'
* Create test user
    * run mongo server with out --auth
    * execute commands & restart mongo server with --auth

```javascript
        use admin;
        
        db.runCommand( 
         {
            dropUser: "test",
            writeConcern: { w: "majority", wtimeout: 5000 }
         } 
        );
        
        use test;
        
        db.createUser(
           {
             user: "test",
             pwd: "test",
             roles: [ { role: "read", db: "test" } ]
           }
        );
```

test
