# VeTo-Workflows

This repository contains workflows needed by [VeTo](https://github.com/schatzopoulos/VeTo) application. 

This repo contains workflows for the following operations in HINs: 
* similarity-search [(PSJoin)](https://github.com/schatzopoulos/psjoin)
* expert set expansion [(VeTo)](https://github.com/vergoulis/rev-sim-recommender)

## How to cite
```
@inproceedings{vergoulis2020veto,
  title={Veto: Expert set expansion in academia},
  author={Vergoulis, Thanasis and Chatzopoulos, Serafeim and Dalamagas, Theodore and Tryfonopoulos, Christos},
  booktitle={International Conference on Theory and Practice of Digital Libraries},
  pages={48--61},
  year={2020},
  organization={Springer}
}
```

## Compile 
In order to compile the Java code for the entity similarity, navigate in `heysim-veto` folder and execute the following command:
```
mvn clean package
```
This will create a self-contained jar file under the `target` directory. 
