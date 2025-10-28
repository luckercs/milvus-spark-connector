package org.apache.spark.sql.execution.datasources;

import com.google.gson.JsonObject;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MilvusUtil {

    private String host;
    private int port;
    private String userName;
    private String password;

    private String uri;
    private String token;

    private String dbName;

    public MilvusUtil(String host, int port, String userName, String password, String dbName) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.dbName = dbName;
    }

    public MilvusUtil(String uri, String token, String dbName) {
        this.uri = uri;
        this.token = token;
        this.dbName = dbName;
    }

    public MilvusClientV2 getClient() {
        ConnectConfig connectConfig;
        if (uri != null && !uri.isEmpty()) {
            connectConfig = ConnectConfig.builder()
                    .uri(uri)
                    .token(token)
                    .dbName(dbName)
                    .connectTimeoutMs(10000)
                    .build();
        } else {
            connectConfig = ConnectConfig.builder()
                    .uri("http://" + host + ":" + port)
                    .username(userName)
                    .password(password)
                    .dbName(dbName)
                    .connectTimeoutMs(10000)
                    .build();
        }
        return new MilvusClientV2(connectConfig);
    }

    public void closeClient(MilvusClientV2 client) {
        if (client != null) {
            client.close();
        }
    }

    public boolean hasCollection(String collectionName) {
        MilvusClientV2 client = getClient();
        boolean res = client.hasCollection(HasCollectionReq.builder().collectionName(collectionName).build());
        closeClient(client);
        return res;
    }

    public DescribeCollectionResp getCollectionDesc(String collectionName) {
        MilvusClientV2 client = getClient();
        DescribeCollectionResp res = client.describeCollection(
                DescribeCollectionReq.builder().databaseName(dbName).collectionName(collectionName).build());
        closeClient(client);
        return res;
    }

    public List<String> getCollectionFunctionOutputFieldList(String collectionName) {
        List<String> functionOutputFieldNames = new ArrayList<>();
        DescribeCollectionResp collectionDesc = getCollectionDesc(collectionName);
        List<CreateCollectionReq.Function> functionList = collectionDesc.getCollectionSchema().getFunctionList();
        for (CreateCollectionReq.Function function : functionList) {
            functionOutputFieldNames.addAll(function.getOutputFieldNames());
        }
        return functionOutputFieldNames;
    }

    public List<String> getCollectionPartitions(String collectionName) {
        MilvusClientV2 client = getClient();
        List<String> partitions = client.listPartitions(ListPartitionsReq.builder().collectionName(collectionName).build());
        closeClient(client);
        return partitions;
    }

    public QueryIterator queryCollection(MilvusClientV2 client, String collectionName, String partitionName, long batchSize) {
        List<String> collectionFunctionOutputFieldList = getCollectionFunctionOutputFieldList(collectionName);

        List<String> outputFields = new ArrayList<>();
        List<CreateCollectionReq.FieldSchema> collectionSchema = getCollectionDesc(collectionName).getCollectionSchema().getFieldSchemaList();
        for (CreateCollectionReq.FieldSchema fieldSchema : collectionSchema) {
            if (!collectionFunctionOutputFieldList.contains(fieldSchema.getName())) {
                outputFields.add(fieldSchema.getName());
            }
        }
        QueryIterator res = client.queryIterator(
                QueryIteratorReq.builder().databaseName(dbName).collectionName(collectionName).partitionNames(Arrays.asList(partitionName)).batchSize(batchSize).expr("").ignoreGrowing(false).consistencyLevel(ConsistencyLevel.STRONG).outputFields(outputFields).build());
        return res;
    }

    public void insertCollection(String collectionName, String partitionName, List<JsonObject> data) {
        MilvusClientV2 client = getClient();
        client.insert(InsertReq.builder().collectionName(collectionName).partitionName(partitionName).data(data).build());
        closeClient(client);
    }
}
