
import apache_beam as beam

options = beam.options.pipeline_options.PipelineOptions()
gcloud_options = options.view_as(
    beam.options.pipeline_options.GoogleCloudOptions)
gcloud_options.job_name = 'bq2bq'
gcloud_options.project = '$$$'
gcloud_options.staging_location = 'gs://$$$'
gcloud_options.temp_location = 'gs://$$$'

worker_options = options.view_as(beam.options.pipeline_options.WorkerOptions)
worker_options.disk_size_gb = 20
worker_options.max_num_workers = 2

options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'

p1 = beam.Pipeline(options=options)

query = 'SELECT INDVDL_ID, ORDER_ID FROM `BQDATASET.BQTABLE` LIMIT 1000'

(p1 | 'read' >> beam.io.Read(beam.io.BigQuerySource(project='$$$', use_standard_sql=True, query=query))
    | 'write' >> beam.io.Write(beam.io.BigQuerySink(
        'BQDATASET.BQTABLE',
        schema='INDVDL_ID:INTEGER, ORDER_ID:STRING',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
)
p1.run() 
