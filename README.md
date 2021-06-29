Python post-processors for VAMDC Query Store
=============================================

This project is composed of four independent post-processors for the VAMDC Query Store (https://github.com/VAMDC/QueryStore):

* Maintance: This folder contains two python scripts. The ErrorHandler.py for dealing with internal errors in the Query Store and for notifying those errors to the appropriate database/node mantainer. The maintenace.py script clean the Errors which are older than one year and clean the files associated with queries that have not been requested for more than 2 years and have no DOI associated.  

 * Fireblock integration: While Fireblock services were operational, all the files stored on the VAMDC Query Store were certified with Fireblock.io (https://fireblock.io) into an Ethereum blockchain. Each file was time-stamped and signed with VAMDC private key. Fireblock.io was open source and free for open source projects (https://github.com/fireblock/go-fireblock). The code in the sub-folder "fireblock" is in charge of the integration between the VAMDC Query-Store and the Fireblock facilities. Since the stop of Fireblock, we sign ourself the files using a SHA256 alghorithm. 
 
 * Zenodo integration: The link between the Query-Store service and the Zenodo open science repository (https://zenodo.org) is implemented using, on the Query Store side, the Zenodo public REST API. The data+metadata of a given query may be uploaded to Zenodo, which assign them a DOI. The program realizing the upload to Zenodo is contained into the sub-folder "zenodo"

* Dump: a simple bash script for dumping the Query Store internal database


 
 
