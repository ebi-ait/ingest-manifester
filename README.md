[![Docker Repository on Quay](https://quay.io/repository/humancellatlas/ingest-exporter/status "Docker Repository on Quay")](https://quay.io/repository/humancellatlas/ingest-demo)

# ingest-exporter

Component that handles the generation and transmission of DSS bundles from submissions
 
This component listens for messages from the Ingest Core using RabbitMQ. When a submission is valid and complete (i.e. all data files have been uploaded to the upload area), Ingest Core will notify this component and this will generate files and bundles in the Data Store. The export service needs the URL of the messaging queue along with the queue name. You can also override the URLs to the staging API and the DSS API.  To see all the argument use the --help argument. 

```
pip install -r requirements.txt
```

```
python exporter.py
```