# DynamoDB stream to Atlas : CDC
### Overview
DynamoDB의 Stream 과 Kinesis, lambda를 이용하여 DynamoDB에서 변경된 데이터가 Stream으로 Kinesis Data Stream에 저장 되며 저장된 내용은 Lambda에 의해 소비 됩니다.   
<img src="/images/image01.png" width="90%" height="90%"> 


### DynamoDB Stream Enable
DynamoDB의 변경 내용을 Stream으로 제공하기 위해서 Stream을 Enable 하여야 합니다. 또한 생성된 stream을 Kinesis data stream으로 보내기 위한 연결을 enable을 하여 줍니다. View Type은 Stream으로 생성할 데이터를 지저아는 것으로 최소 최종 데이터 (New Image) 이상을 선택 하여야 합니다.     
DynamoDB의 Stream은 Lambda에서 바로 소비하는 것이 가능하나 DynamoDB의 Stream 최대 저장 기간은 1일 입니다. 이를 보완하기 위해서 Kinesis Data Stream을 중간에 넣는 경우 보관기간을 1년 까지 변경 할 수 있습니다.    

<img src="/images/image02.png" width="70%" height="70%"> 



### Kinesis Stream 생성
DynamoDB stream에 대한 수집하고 lambda에 제공하는 큐서비스로 Kinesis Stream을 생성 합니다.    
생성시 옵션 (Capacity)는 On-demand, Provisioned를 선택 할 수 있으며 기본 on-demand를 선택 합니다.
설정 정보는 Configuration에서 상시 변경 가능 합니다.     

<img src="/images/image03.png" width="80%" height="80%">     




### Lambda 생성
Kinesis에 생성되 Data Stream을 주기적으로 읽고 실행 하여 줍니다. 주요 기능은 Stream을 읽고 DynamoDB에 발생한 이벤트의 종류 (Create, Update, Delete)에 따라 Atlas에 적용할 MQL을 실행 하여 줍니다.
Java Lambda function을 생성합니다. (Runtime 을 Java 8 on Amazon Linux 2로 선택 합니다.)     
VPC Peering을 이용하여 Atlas에 접근하기 위해 Function이 수행된 VPC를 선택 합니다.    
<img src="/images/image04.png" width="90%" height="90%">     



생성한 Function이 Kinesis를 읽고 결과를 S3에 업로드 하기 위한 권한 설정이 필요 합니다.    
Function > Configuration > Permissions 에서 Execution role 항목의 Role name을 클릭 하면 lambda function이 사용하는 권한 정보를 볼 수 있습니다. VPC peering을 이용하기 위한 EC2와 Kinesis에 저장된 Record를 읽기 위한 권한을 추가 하여 줍니다.
<img src="/images/image05.png" width="80%" height="80%">     
(가능하면 최소 권한을 부여 하도록 조정하여 줍니다)



Function overview 에서 Add trigger를 클릭 하여 생성한 Kinesis를 선택 합니다.
Batch Size와 Batch Window를 필요에 따라 적정한 값으로 설정하여야 합니다. 한번에 최대 1000 건을 읽을 수 있으며 읽는 주기를 5초로 합니다.
읽는 건수가 늘어나는 경우 function의 computing resource를 높은 사양으로 선택 하여야 합니다. (높은 메모리를 사용하여야 합니다.)    

<img src="/images/image06.png" width="90%" height="90%">     

Code를 업로드 하고 Runtime settings에서 맞는 class handler 를 입력 하여 줍니다.     
<img src="/images/image07.png" width="90%" height="90%">     



포함된 Java Source의 Handler class 는 com.amazonaws.lambda.demo.LambdaFunctionHandler 입니다.

Atlas 접근을 위한 연결 정보를 환경 변수로 등록 하여 줍니다.    
<img src="/images/image08.png" width="70%" height="70%">     



### Lambda Java Code
Maven 기반의 프로젝트로 connectionString 을 수정 한 후 build 하여 줍니다.
mapToList 함후는 DynamoDB 의 데이터 포멧을 JSON으로 변경 하여 줍니다.

아래와 같은 Dynamo Data 를 Json으로 변경 합니다.
````` DynamoDB Data
{
  "customerId": {
    "S": "Test-001"
  },
  "ListKey": {
    "L": [
      {
        "S": "StringValue0"
      },
      {
        "S": "StringValue1"
      },
      {
        "N": "0"
      },
      {
        "N": "1"
      },
      {
        "BOOL": false
      },
      {
        "BOOL": true
      },
      {
        "SS": [
          "StringSet1",
          "StringSet2"
        ]
      },
      {
        "NS": [
          "0",
          "1"
        ]
      },
      {
        "L": [
          {
            "S": "InnerListListValue1"
          }
        ]
      },
      {
        "M": {
          "InnerListMapKey1": {
            "S": "StringValue1"
          },
          "InnerListMapKey2": {
            "S": "StringValue2"
          }
        }
      }
    ]
  },
  "MapKey": {
    "M": {
      "MapInnerNumKey": {
        "N": "0"
      },
      "MapInnerList": {
        "L": [
          {
            "S": "ListString1"
          }
        ]
      },
      "MapInnerKey": {
        "S": "StringValue1"
      }
    }
  },
  "StringSet": {
    "SS": [
      "StringValue0",
      "StringValue1"
    ]
  },
  "NumberKey": {
    "N": "1"
  },
  "NumberSet": {
    "NS": [
      "0",
      "3"
    ]
  },
  "BooleanKey": {
    "BOOL": false
  },
  "StringKey": {
    "S": "StringValue1"
  }
}
`````

변경된 Json
`````
Atlas atlas-a1hth3-shard-0 [primary] dynamodb> db.customerKinesis.find({customerId:'Test-001'}).pretty()
[
  {
    _id: ObjectId("61a860bf7f8c9bda5743b98c"),
    NumberSet: [ 0, 3 ],
    ListKey: [
      'StringValue0',
      'StringValue1',
      0,
      1,
      false,
      true,
      [ 'StringSet1', 'StringSet2' ],
      [ 0, 1 ],
      [ 'InnerListListValue1' ],
      {
        InnerListMapKey1: 'StringValue1',
        InnerListMapKey2: 'StringValue2'
      }
    ],
    MapKey: {
      MapInnerList: [ 'ListString1' ],
      MapInnerNumKey: 0,
      MapInnerKey: 'StringValue1'
    },
    BooleanKey: false,
    StringKey: 'StringValue1',
    StringSet: [ 'StringValue0', 'StringValue1' ],
    customerId: 'Test-001',
    NumberKey: 1
  }
]
`````



### CDC Test
DynamoDB 에서 데이터를 변경 하고 저장 합니다.
  
<img src="/images/image09.png" width="90%" height="90%">     

내부 데이터 값 수정 후 저장 (혹은 데이터 추가 생성)   

Lambda function을 선택 한후 Monitor에서 View Logs in CloudWatch를 선택 합니다. S3 에 저장된 로그를 확인 합니다.


<img src="/images/image10.png" width="90%" height="90%">     

Atlas에 변경된 데이터를 확인 합니다.

<img src="/images/image11.png" width="90%" height="90%">    