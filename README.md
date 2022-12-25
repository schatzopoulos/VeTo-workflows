# VeTo-Workflows

This repository contains workflows needed by [VeTo](https://github.com/schatzopoulos/VeTo) application. 

This repo contains workflows for the following operations in HINs: 
* similarity-search [(PSJoin)](https://github.com/schatzopoulos/psjoin)
* expert set expansion [(VeTo)](https://github.com/vergoulis/rev-sim-recommender)

## How to cite
```
@inproceedings{chatzopoulos2021veto,
  title={VeTo-web: A Recommendation Tool for the Expansion of Sets of Scholars},
  author={Chatzopoulos, Serafeim and Vergoulis, Thanasis and Dalamagas, Theodore and Tryfonopoulos, Christos},
  booktitle={2021 ACM/IEEE Joint Conference on Digital Libraries (JCDL)},
  pages={334--335},
  year={2021},
  organization={IEEE}
}

@article{chatzopoulos2021veto+,
  title={VeTo+: improved expert set expansion in academia},
  author={Chatzopoulos, Serafeim and Vergoulis, Thanasis and Dalamagas, Theodore and Tryfonopoulos, Christos},
  journal={International Journal on Digital Libraries},
  pages={1--19},
  year={2021},
  publisher={Springer}
}

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

## Usage
In order to perform an analysis, you need to navigate inside the root directory of the cloned repository and execute the entrypoiint bash script with the appropriate configuration file as a parameter: 

```
/bin/bash entrypoint.sh config.json
```

This configuration file is in JSON format and includes all apporopriate paramreters; 
a sample can be found [here](https://github.com/schatzopoulos/VeTo-workflows/blob/master/sample_config.json).
Its main parameters are described below:

| Parameter Name  | Description |
| ------------- | ------------- |
| apv_hin | a tsv ile encoding the metapath-based HIN view according to the APV metapath with columns: source, destination, number of paths.
| apt_hin | a tsv file encoding the metapath-based HIN view according to the APT metapath with columns: source, destination, number of paths.
| author_names | a tsv file containing expert identifiers and names.
| expert_set | a text file representing the initial expert set; one expert identifier per row.
| apv_sims_dir | the folder to store the APV based similarities
| apt_sims_dir | the folder to store the APT based similarities
| veto_output | the folder to store the raw output containing expert identifiers and scores.
| final_output | the folder to store the final output with expert names appended.
| sim_threshold | pairs of nodes with lower similarity score than this threshold are ignored
| sim_min_values | connections that occur fewer times in the initial HIN than this threshold are not considered.
| sims_per_expert | an integer number indicating how many of the top most similar experts will be considered.
| apv_weight | a weighting factor of the APV similarities.
| apt_weight | a weighting factor of the APT similarities.
| output_size | an integer number indicating the top authors with the higher aggregated score to be included in the results.

## Paper Recommender
For the docs please refer [here](https://github.com/gbouzioto/VeTo-workflows/tree/master/paper_recommender/README.md)
