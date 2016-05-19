# Mongo jdbc driver

## Prerequisites
1. Maven
2. jdk 1.7 (or higher) 
## Installation
1. Clone git repository
```bash
$ git clone git@github.com:Slemma/Mongo-JDBC-Driver.git mongo-jdbc
2. Install mongodb
3. Create database 'test'
4. Create collections and import data from source folder 'src/main/data'
5. Create test user
- run mongo server with out --auth
- execute commands

<code>
use admin;
</code>
<p>
<code>
db.runCommand( 
 {
    dropUser: "test",
    writeConcern: { w: "majority", wtimeout: 5000 }
 } 
);
</code>
</p>

<p>
<code>
use test;
</code>
</p>

<p>
<code>
db.createUser(
   {
     user: "test",
     pwd: "test",
     roles: [ { role: "read", db: "test" } ]
   }
);
</code>
</p>

- restart mongo server with --auth
