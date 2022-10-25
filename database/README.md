**Region** will be `us-east-2` or Ohio (free-tier)



### Live Database Connection
**Currently disabled**
* https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
1. Set credentials & set them as environment variables
   1. `~/.aws/credentials`
2. Set region 
   1. `~/.aws/config`
      1. See above (`us-east-2`)
   
### 
1. [Install DynamoDB local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html#DynamoDBLocal.DownloadingAndRunning.title) (requires JRE)
2. Set AWS credentials via. AWS CLI (can use any credentials)
   1. `aws configure`
      1. `Default region name` = `us-east-1`
      2. All credentials are set to "local"
3. Start local dynamodb  
   `java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb`
