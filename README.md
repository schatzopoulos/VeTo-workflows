# VeTo Workflows

Contains workflows needed by [VeTo](https://github.com/schatzopoulos/VeTo) application. 

This repo contains workflows for the following operations in HINs: 
* similarity-search [(PSJoin)](https://github.com/schatzopoulos/psjoin)
* expert set expansion [(VeTo)](https://github.com/vergoulis/rev-sim-recommender)

## Compile 
In order to compile the Java code for the entity similarity, navigate in `heysim-veto` folder and execute the following command:
```
mvn clean package
```
This will create a self-contained jar file under the `target` directory. 
