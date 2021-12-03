package com.amazonaws.lambda.demo;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LambdaFunctionHandler implements RequestHandler<KinesisEvent, Integer> {

    private static final String INSERT = "INSERT";

    private static final String MODIFY = "MODIFY";
    private static final String REMOVE = "REMOVE";
    //dynamo.5qjlg.mongodb.net/myFirstDatabase
 
    private static final String connectionString = "mongodb+srv://main_user:******@dynamo.5qjlg.mongodb.net/dynamodb?retryWrites=true&w=majority";
//cluster1.5qjlg.mongodb.net/myFirstDatabase
    @Override
    public Integer handleRequest(KinesisEvent event, Context context) {
        //context.getLogger().log("Input: " + event.getRecords().size());
    	context.getLogger().log("Loaded Data Record is "+event.getRecords().size());

    	int iCount = 0;
    	int errorCnt = 0;
    	int successInsertCnt =0;
    	int successUpdateCnt = 0;
    	int successDeleteCnt = 0;
    	String eventType = "";
        String customerId = "";
         
        try 
        {
        	MongoClient mongoClient = MongoClients.create(connectionString);

        	MongoDatabase dynamodb = mongoClient.getDatabase("dynamodb");
            MongoCollection<Document> lambdaCollection = dynamodb.getCollection("customerKinesis");
            
            Document newImage = null;  
            JsonParser parser = new JsonParser();
        	JsonElement jrecord = null;
        	JsonObject jobject = null;
        	
        	Bson filter = null;
            Gson gson = new Gson();
            
            
        	iCount = event.getRecords().size();

	        for (KinesisEventRecord record : event.getRecords()) {
	        	
	        	jrecord = parser.parse(new String(record.getKinesis().getData().array(),"UTF-8"));
	        	eventType = jrecord.getAsJsonObject().get("eventName").getAsString();
	        	customerId = jrecord.getAsJsonObject().get("dynamodb").getAsJsonObject().get("Keys").getAsJsonObject().get("customerId").getAsJsonObject().get("S").getAsString();
	        	
	        	if (INSERT.equals(eventType)) {
	        		
	        		filter = eq("customerId", customerId);
	        		jobject = jrecord.getAsJsonObject().get("dynamodb").getAsJsonObject().get("NewImage").getAsJsonObject();	        		
	        		HashMap<String, Object> myMap = gson.fromJson(jobject.toString(), HashMap.class);

	        		newImage = Document.parse(gson.toJson(mapToJson(myMap)));
	        		
	        		lambdaCollection.replaceOne(filter, newImage, new ReplaceOptions().upsert(true));
		            successInsertCnt++;
	            }
	        	else if (MODIFY.equals(eventType)) {
	        		filter = eq("customerId", customerId);
	        		jobject = jrecord.getAsJsonObject().get("dynamodb").getAsJsonObject().get("NewImage").getAsJsonObject();
	        		HashMap<String, Object> myMap = gson.fromJson(jobject.toString(), HashMap.class);
	        		
	        		newImage = Document.parse(gson.toJson(mapToJson(myMap)));
	        		lambdaCollection.replaceOne(filter, newImage);
	        		successUpdateCnt++;
	            }
	        	else if (REMOVE.equals(eventType)) {
	        		filter = eq("customerId", customerId);
		            lambdaCollection.deleteOne(filter);
		            successDeleteCnt++;
	            }
	        }
	        mongoClient.close();
        }catch (Exception e)
        {
        	errorCnt++;
        	context.getLogger().log("Error: ["+eventType+"],[customerId:"+customerId+"]" + e.toString());
        }
        if (errorCnt > 0)
        {
        	context.getLogger().log("Processing result [Error is included] : {totolCount:"+iCount+",Insert:"+successInsertCnt+",Update:"+successUpdateCnt+",Delete:"+successDeleteCnt+",errorCount:"+errorCnt+"}");
        }
        else
        {
        	context.getLogger().log("Processing Success : {totolCount:"+iCount+",successCount:"+(successInsertCnt+successUpdateCnt+successDeleteCnt)+"}");
        }

        return event.getRecords().size();
    }
    

	private static List mapToList(List list) {
        List newList = new ArrayList();
        
        for (int i=0; i< list.size();i++) {
        	Map obj = (Map) list.get(i);
        	if(obj.get("N") != null) {
        		try {
        			newList.add(Integer.parseInt((String)obj.get("N")));
                }catch (Exception e)
            	{
            		System.out.println("Number Parsing Error:"+ (String)obj.get("N"));
            	}
        	}
            else if(obj.get("S") != null)
            	newList.add(obj.get("S"));
            else if(obj.get("BOOL") != null)
            	newList.add(obj.get("BOOL"));
            else if(obj.get("SS") != null)
            	newList.add(obj.get("SS"));
            else if(obj.get("L") != null)
            {
            	newList.add(mapToList((List)obj.get("L")));
            }
            else if(obj.get("NS") != null) {
            	List tmplist = (List) obj.get("NS");
                List tmpnewList = new ArrayList();
                
                for (int i2=0; i2< tmplist.size();i2++) {
                	try
                	{
                		tmpnewList.add(Integer.parseInt((String) tmplist.get(i2)));
                	}catch(Exception e2)
                	{
                		System.out.println("NS");
                	}
                }
                newList.add(tmpnewList);
            }
            else if(obj.get("M") != null) {
                Map objtmp2 = (Map) obj.get("M");
                newList.add(mapToJson(objtmp2));
            }
        }
        return newList;
	}
	
	public static Map<String,Object> mapToJson(Map map){
		
        Map<String,Object> finalKeyValueMap = new HashMap();
        Iterator it = map.entrySet().iterator();

        while(it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            System.out.println("Entry:"+pair);

            Map obj1 = (Map) pair.getValue();
            
            // M 이 없는 항목 : S 혹은 N
            if(obj1.get("M") == null)  {
            	
                if(obj1.get("N") != null) {
                	try {
                		finalKeyValueMap.put(pair.getKey().toString(),Integer.parseInt((String)obj1.get("N")));
                	}catch (Exception e)
                	{
                		System.out.println("Number Parsing Error:"+ (String)obj1.get("N"));
                	}
                }
                else if(obj1.get("S") != null) {
                	finalKeyValueMap.put(pair.getKey().toString(),obj1.get("S"));
                }
                else if(obj1.get("BOOL") != null)
                    finalKeyValueMap.put(pair.getKey().toString(),obj1.get("BOOL"));
                else if(obj1.get("SS") != null)
                {
                	Map obj2 = (Map) pair.getValue();
                	List list = (List) obj2.get("SS");
                    finalKeyValueMap.put(pair.getKey().toString(),list);
                }	
                else if(obj1.get("NS") != null)
                {
                	Map obj2 = (Map) pair.getValue();
                	List list = (List) obj2.get("NS");
                    List newList = new ArrayList();
                    
                    for (int i=0; i< list.size();i++) {
                    	try
                    	{
                    		newList.add(Integer.parseInt((String) list.get(i)));
                    	}catch(Exception e)
                    	{
                    		System.out.println("NS");
                    	}
                    }
                    finalKeyValueMap.put(pair.getKey().toString(),newList);
                }
                else if(obj1.get("L") != null)
                {
                	Map obj2 = (Map) pair.getValue();
                	List list = (List) obj2.get("L");
                    
                    
                    finalKeyValueMap.put(pair.getKey().toString(),mapToList(list));
                }
             }
            // M 인 항목
             else {
                Map obj2 = (Map) pair.getValue();
                Map obj3 = (Map) obj2.get("M");
                finalKeyValueMap.put(pair.getKey().toString(),mapToJson(obj3));
            }
        }
        return finalKeyValueMap;
    }

}
