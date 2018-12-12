Python post-processors for VAMDC Query Store
=============================================

This project is composed of two independent python post-processors for the VAMDC Query Store (https://github.com/VAMDC/QueryStore):
 * Fireblock integration: All the files stored on the VAMDC Query Store are certified with Fireblock.io (https://fireblock.io) into an Ethereum blockchain. Each file is time-stamped and signed with VAMDC private key. Fireblock.io is open source and free for open source projects (https://github.com/fireblock/go-fireblock). The code in the sub-folder "fireblock" is in charge of the integration between the VAMDC Query-Store and the Fireblock facilities. 
 * Zenodo integration: The link between the Query-Store service and the Zenodo open science repository (https://zenodo.org) is implemented using, on the Query Store side, the Zenodo public REST API. The data+metadata of a given query may be uploaded to Zenodo, which assign them a DOI. The program realizing the upload to Zenodo is contained into the sub-folder "zenodo"
 
 
