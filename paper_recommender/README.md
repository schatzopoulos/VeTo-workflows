## Paper Recommender Veto
### Dataset
In order to build the similarities needed for Veto algorithms, the [DBLP Article Similarities (DBLP-ArtSim) dataset](https://zenodo.org/record/4567527#.Y6XVpNJBzWl)
was used and more specifically the following files:

- [PAP.csv.gz](https://zenodo.org/record/4567527/files/PAP.csv.gz?download=1), contains the paper->author->paper metapath sim scores
- [PTP.csv.gz](https://zenodo.org/record/4567527/files/PTP.csv.gz?download=1), contains the paper->topic->paper metapath sim scores
- [aminer_ids.csv.gz](https://zenodo.org/record/4567527/files/aminer_ids.csv.gz?download=1), contains the aminer to veto id mapping

In order to make recommendations the [AMiner's DBLP v10](https://www.aminer.org/citation) dataset was used.


### Installation
#### Python
Python 3.8.9 was used to run the recommendation scripts.
You can install the package requirements through the [requirements.txt](https://github.com/gbouzioto/VeTo-workflows/tree/master/paper_recommender/requirements.txt) file through the command:

```bash
pip3 install -r requirements.txt
```
#### NLTK
Follow the installation guide for nltk [here](https://www.nltk.org/data.html)  
(instead of `all` only `stopwords` and `punkt` files should be downloaded)

#### MongoDB
MongoDB version v4.4.6 was used for this project.
You can follow the instructions to install it [here](https://www.mongodb.com/docs/manual/installation/) 

### Execution
#### Building Similarities
In order to build the similarities for the recommender scripts, you need to navigate inside the root directory of the cloned repository and execute the build_sims.sh bash script with the appropriate configuration file as a parameter: 

```
/bin/bash build_sims.sh config.json
```

This configuration file is in JSON format. 
a sample file can be found [here](https://github.com/gbouzioto/VeTo-workflows/tree/master/paper_recommender/sample_config.json).

Its main parameters are described below:

| Parameter Name | Description |
|--- | --- |
| pap_hin        | a tsv file encoding the metapath-based HIN view according to the PAP metapath with columns: source, destination, number of paths.
| ptp_hin        | a tsv file encoding the metapath-based HIN view according to the PTP metapath with columns: source, destination, number of paths.
| pap_sims_dir   | the folder to store the PAP based similarities
| ptp_sims_dir   | the folder to store the PTP based similarities

#### Building MongoDB

Before building the mongo db, you will need to edit the file in [local_secrets.py](https://github.com/gbouzioto/VeTo-workflows/tree/master/paper_recommender/local_secrets.py). 
and replace the credentials that your database uses. The file should contain the following variables:

```python
DB_NAME = 'your_database_name'
DB_HOST = 'your_database_host'
DB_USER = 'database_user'
DB_PWD = 'database_password'
DB_PORT = 'database_port'
```

To build the MongoDB you will need to run the [build_db.py](https://github.com/gbouzioto/VeTo-workflows/tree/master/paper_recommender/build_db.py) script.
You can find details on how to run it by the command:

```
python3 build_db.py -h
```

The arguments are outlined below:

| Argument Name               | Description |
|-----------------------------| --- |
| '-f' / '--files'            | A string containing the dataset filepaths, can be comma separated to included multiple filepaths
| '-af' / '--aminer_ids_file' | The csv file containing the aminer to veto id mapping

Once built the database will contain 2 collections, 'papers' and 'aminer_mapper'. Papers collection has the exact same columns as the json files provided in the dataset.
Aminer mapper collection has 2 columns: 'id', namely the veto id and 'aminer_id', namely the aminer id of the paper in the dataset.

An example is outlined below:
```
python3 build_db.py -f '/path/to/dataset_file_1.json /path/to/dataset_file_2.json /path/to/dataset_file_n.json' -af '/path/to/aminer_ids.csv'
```

#### Producing Recommendations

In order to run recommendations using `PaperVeto` you will need to run the following script [paper_veto.py](https://github.com/gbouzioto/VeTo-workflows/tree/master/paper_recommender/paper_veto.py).
You can find details on how to run it by the command:

Example:
```
python3 paper_veto.py -h
```
The arguments are outlined below:

| Argument Name               | Description |
|-----------------------------| ------------- |
| '-pf' / '--paper_file'      | the filepath containing the veto paper ids (**newline separated**)
| '-vo' / '--veto_output'     | filepath where the results will be written
| '-pap' / '--pap_sims'       | directory containing the PAP similarity scores
| '-ptp' / '--ptp_sims'       | directory containing the PTP similarity scores
| '-spe' / '--sims_per_paper' | how many similarities per paper should be considered
| '-papw' / '--pap_weight'    | score weight for the PAP similarities
| '-ptpw' / '--ptp_weight'    | score weight for the PTP similarities
| '-algo' / '--algorithm'     | the scoring algorithm to be used
| '-rrfk' / '--rrf_k'         | rrf k algorithm ranking parameter (**used only with rrf algorithm**)
| '-outs' / '--output_size'   | the size of the output

Example:
```
python3 paper_veto.py --paper_file /path/to/input.txt --veto_output /path/to/output.csv --pap_sims /path/to/PAP/ --ptp_sims /path/to/PTP/ --sims_per_paper 50 --pap_weight 0.2 --ptp_weight 0.8```
```

In order to run recommendations using `ExtendedPaperVeto` you will need to run the following script [extended_paper_veto.py](https://github.com/gbouzioto/VeTo-workflows/tree/master/paper_recommender/extended_paper_veto.py).
You can find details on how to run it by the command:

Example:
```
python3 paper_veto.py -h
```
The arguments are outlined below:

| Argument Name               | Description |
|-----------------------------| ------------- |
| '-pf' / '--paper_file'      | the filepath containing the veto paper ids (**newline separated**)
| '-vo' / '--veto_output'     | filepath where the results will be written
| '-pap' / '--pap_sims'       | directory containing the PAP similarity scores
| '-ptp' / '--ptp_sims'       | directory containing the PTP similarity scores
| '-spe' / '--sims_per_paper' | how many similarities per paper should be considered
| '-papw' / '--pap_weight'    | score weight for the PAP similarities
| '-ptpw' / '--ptp_weight'    | score weight for the PTP similarities
| '-kw' / '--keyword_weight'  | score weight for the keyword similarities
| '-algo' / '--algorithm'     | the scoring algorithm to be used
| '-rrfk' / '--rrf_k'         | rrf k algorithm ranking parameter (**used only with rrf algorithm**)
| '-outs' / '--output_size'   | the size of the output

Example:
```
python3 paper_veto.py --paper_file /path/to/input.txt --veto_output /path/to/output.csv --pap_sims /path/to/PAP/ --ptp_sims /path/to/PTP/ --sims_per_paper 50 --pap_weight 0.2 --ptp_weight 0.3 --keyword_weight 0.5```
```

#### Helpers

One helper script is also included, [convert_aminer_ids.py](https://github.com/gbouzioto/VeTo-workflows/tree/master/paper_recommender/convert_aminer_ids.py), which can convert veto ids to aminer and vice versa.
Another idea would be to use the database instead.