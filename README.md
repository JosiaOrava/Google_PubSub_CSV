# Google Cloud Pub/Sub
This is a Python script to fetch messages from Google Cloud Pub/Sub and checks for specific sensors (Ruuvitag and TEROS12). It creates csv file for each day and outputs data from the messages. If message contains anything else than correct sensor data, it will be skipped and not written in file.

Example of the output:
| Device ID        | Sensor Type   | Temperature | Humidity | Pressure | Volumetric Water Content | Electrical Conductivity |
|------------------|---------------|-------------|----------|----------|--------------------------|-------------------------|
| TEROS12          | TEROS 12      | 22.3        |          |          | 1817.13                  | 512.5                     |
| DD:83:3D:A4:CE:C6 | Ruuvitag      | 21.23       | 44.26    | 1155.35  |                          |                         |
| DB:C3:58:D9:03:70 | Ruuvitag      | 21.24       | 43.96    | 1155.35  |                          |                         |
| E2:70:D7:96:45:18 | Ruuvitag      | 21.28       | 42.12    | 1155.35  |                          |                         |
| DD:42:FA:12:2A:CD | Ruuvitag      | 21.34       | 42.08    | 1155.35  |                          |                         |
| EE:DF:9F:BA:8D:49 | Ruuvitag      | 21.23       | 45.97    | 1155.35  |                          |                         |

## Configuration
Copy `conf.py-TEMPLATE` and rename it to `conf.py`. In that file you will need to insert the following information to variables:
* *PROJECT_ID* = project ID of the google cloud project  
* *SUBSCRIPTION_ID* = subscription ID for the topic messages are sent 
* *PRIVATE_KEY_FILE* = name of the service account key json file

To get service account keyfile you will need to go to Google Cloud -> Service Accounts -> Select service account that has rights to Pub/Sub subscription -> Keys -> Add key

That will download a json file that contains your private information. `WARNING: Do not share that file, it contains sensitive information`. Move that file into directory that contains this python script.

## Dependencies
You need the following python packets:
* `google-cloud-pubsub`
* `google-cloud-core`

You can install them by running `pip install google-cloud-pubsub google-cloud-core`


