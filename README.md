Get logs from https://www.secrepo.com/self.logs/ with Azure Functions and upload them to ADLS (only logs that haven't been processed before should be uploaded).
Process the logs in Databricks (check for malformed records using regex, transform the fields to the right format and send the malformed records to ADLS)
