# Preliminary Data Sources Analysis

This project investigates metadata access and retrieval from the following repositories:

## ZENODO (https://zenodo.org/)
- The schema used is an adapted version of DataCite metadata schema (https://schema.datacite.org/meta/kernel-4.6/).
- API available but with limited functionalities (it's only possible to retrive records through DOI) and API key is required (https://developers.zenodo.org/#rest-api).
- As far as we could gather Zenodo doesn't offer directly dumps of their data but it provides metadata harvesting interface via OAI-PMH, which allows access to all public records in standard formats like Dublin Core and DataCite XML (https://developers.zenodo.org/#changes).

## AMS Acta (https://amsacta.unibo.it/)

* **API Limitations:** The API appears to have limited functionality, with no apparent option for bulk metadata downloads.
* A research tool is available, but it does not allow direct querying based on project affiliation.
* **Affiliation Metadata:** Affiliation metadata is not consistently and explicitly provided for all projects.
* It is possible to filter content by document type (e.g., dataset, software) and export results in various formats.
* **IRIS Dump:** An IRIS data dump is available on AMS Acta. Notably, the file `ODS_L1_IR_ITEM_CON_PERSON.csv` within this dump contains information about current University of Bologna (UNIBO) authors, including their ORCID IDs (ITEM_ID, RM_PERSON_ID, PID, ORCID, FIRST_NAME, LAST_NAME, PLACE).

## SOFTWARE HERITAGE ([https://www.softwareheritage.org/](https://www.softwareheritage.org/))

* **[Documentation](https://docs.softwareheritage.org/devel/index.html):** Comprehensive documentation is available, covering usage, infrastructure, API reference, and development.
* A guide is available for searching and browsing the web archive.
* **GitLab:** Their development and collaboration platform is accessible via [GitLab](https://gitlab.softwareheritage.org/explore).
* **Metadata:** They utilize the concepts of [intrinsic](https://docs.softwareheritage.org/devel/glossary.html#term-intrinsic-metadata) and [extrinsic](https://docs.softwareheritage.org/devel/glossary.html#term-extrinsic-metadata) metadata.<br>
  Their metadata workflow and architecture are detailed [here](https://docs.softwareheritage.org/devel/architecture/metadata.html#architecture-metadata).
* **APIs and Endpoints:**
    * Primary API Root: [https://archive.softwareheritage.org/api/1/](https://archive.softwareheritage.org/api/1/)
    * Metadata Endpoint: [https://archive.softwareheritage.org/api/1/#metadata](https://archive.softwareheritage.org/api/1/#metadata)
    * Web API: [https://forge.softwareheritage.org/source/swh-web/](https://forge.softwareheritage.org/source/swh-web/)
* **Data Model:** Their data model is described in a paper as a directed graph with relational tables - Pietri et al. (2019), [_The Software Heritage Graph Dataset: Public Software Development Under One Roof_](https://ieeexplore.ieee.org/document/8816748)<br>

Software Heritage API primarily supports:

* Retrieving objects by their intrinsic identifiers (SWHIDs).

* Browsing the content graph (origins, snapshots, revisions, directories, contents).

* Looking up objects by URL (e.g., GitHub URL).

* There is no direct full-text search API for metadata like author names or institutions. Instead, the main entry point is the origin (URL).

 It is not clear how to find content related to University of Bologna.

**N.B.!** **Terms of Use:** Access to the SWH API is subject to specific terms and conditions, including restrictions on massive data extraction: [https://www.softwareheritage.org/legal/api-terms-of-use/](https://www.softwareheritage.org/legal/api-terms-of-use/).



