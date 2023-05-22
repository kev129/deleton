<p align="center">
<img src= "https://user-images.githubusercontent.com/78930833/195614941-313554aa-24a8-4192-8706-3e64a45aec80.png" alt = "Deloton Logo">
</p>

<h1 align = "center"><b>Deloton</b></h1>

<p align="center">
<img src = "https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54")>
<img src = "https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white">
<img src = "https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white">
<img src = "https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white">
<img src = "https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white">
<img src = "https://img.shields.io/badge/Plotly-%233F4F75.svg?style=for-the-badge&logo=plotly&logoColor=white">
<img src = "https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka">
</p>
<p align="center">
<img src = "https://user-images.githubusercontent.com/78930833/195623863-0d1143d0-322c-4039-865c-637ced26b644.png" alt = "EZ Brokers Pipeline">
</p>
<br>
<h2><b>About The Project</b></h2>
<p align = "justify">This project aims to produce an effective and efficient pipeline which will automatically transform and load date through a number of deliverables for the client, Deloton.<br>The pipeline utilizes the seamless nature of AWS' services and makes use of services such as EC2, Lambda, SES and Aurora. The pipeline follows the ETL format, Extract, Transform, and Load.<br>The ingestion of data is completed from a Kafka stream, transformed and staged within Aurora and loaded and analyzed through multiple deliverables such as a daily report and live ride information.<br> The project has 4 deliverables/objectives:
<br>
1. 24 Hour Report to be emailed to CEO <br>
2. API to access all historical rides and users<br>
3. Live Dashboard featuring current ride of bike<br>
4. 12 Hour Report available on website as well<br>
</p>
<br>
<h2><b>Libraries and Dependencies</b></h2>
<p>Pandas<br>Confluent Kafka<br>SQLAlchemy<br>Boto3<br>fpdf<br>Kaleido<br>numpy<br>plotly.express<br>PyPDF2<br>Dash<br>Dash Bootstrap</p>
<br>
<h2>Setup Instructions</h2>
<p>In order to utilise this library, you must be able to build and run Docker Images.</p>

<h3>Docker Image Creation </h3>

The following command can be used to create the Image from the DockerFile.\
`docker build . -t [IMG NAME]`

<h3>Running Docker Images Locally</h3>

Docker Images can be run with this simple command.\
`docker run [NAME]`

<h3>Runner Docker Images Via AWS</h3>

<h4>EC2 Docker Installation</h4>

The first step is to provide you instance with access to your .env file via SSH.\
`scp -i {path_to_pem} {path_to_.env} ec2-user@{instance_ip}:/home/ec2-user`

<br>

You can then SSH into your instance to input commands directly onto your instance.\
`ssh -i {path_to_pem} ec2-user@{instance_ip}`

<br>

You can then install, and run Docker via the following commands.\
`sudo yum update`\
`sudo yum install docker`\
`sudo systemctl enable docker.service`\
`sudo systemctl start docker.service`

<h4>Deploy Docker Image</h4>

Docker must first be authenticated to your AWS account. Instructions to complete this can be found on [AWS'](https://docs.aws.amazon.com/AmazonECR/latest/userguide/registry_auth.html) website.
<br>

The first step is to tag the Image in preparation for pushing to ECR. This can be done via the following command in your local console.\
`docker tag [Image ID] {aws_account_id}.dkr.ecr.{region}.amazonaws.com/{repository}:{tag}`

It can then be pushed to the ECR.\
`docker push {aws_account_id}.dkr.ecr.{region}.amazonaws.com/{repository}:{tag}`

From the AWS SSH, you can run the following commands to run your Image.\
`sudo docker pull {account_id}.dkr.ecr.{region}.amazonaws.com/{ecr_registry_name}:{tag}`\
`sudo docker run -d -p {port}:{port} --env-file ./.env {account_id}.dkr.ecr.{region}.amazonaws.com/{ecr_registry_name}`
