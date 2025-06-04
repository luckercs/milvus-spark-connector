package com.luckercs;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;
import org.apache.spark.sql.execution.datasources.MilvusCommonUtils;
import org.apache.spark.sql.execution.datasources.MilvusUtil;

import java.nio.ByteBuffer;
import java.util.*;

public class MilvusInstance {

    public static String uri = "http://localhost:19530";
    public static String dbname = "default";
    public static String COLLECTION_NAME = "hello";

    public static void main(String[] args) {
        MilvusInstance milvusInstanceTest = new MilvusInstance();
        milvusInstanceTest.init();

        milvusInstanceTest.createCollection();
        milvusInstanceTest.insertCollection();
        milvusInstanceTest.queryCollection();
        milvusInstanceTest.close();
        milvusInstanceTest.dropCollection();
    }

    private MilvusClientV2 client;

    public void init() {
        MilvusUtil milvusUtil = new MilvusUtil(uri, "", dbname);
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(uri)
                .token("")
                .dbName(dbname)
                .connectTimeoutMs(10000)
                .build();
        client = new MilvusClientV2(connectConfig);
        System.out.println("======== milvus init ========");
    }


    public void close() {
        if (client != null) {
            client.close();
            System.out.println("======== milvus close ========");
        }
    }

    public void createCollection() {
        createCollection(client, COLLECTION_NAME);
    }

    public void insertCollection() {
        insertCollection(client, COLLECTION_NAME);
    }

    public void queryCollection() {
        queryCollection(client, COLLECTION_NAME);
    }

    public void queryIteratorCollection() {
        queryIteratorCollection(client, COLLECTION_NAME);
    }

    public void dropCollection() {
        dropCollection(client, COLLECTION_NAME);
    }

    private static void createCollection(MilvusClientV2 client, String COLLECTION_NAME) {
        if (client.hasCollection(HasCollectionReq.builder().collectionName(COLLECTION_NAME).build())) {
            client.dropCollection(DropCollectionReq.builder().collectionName(COLLECTION_NAME).build());
        }
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder().build();
        collectionSchema.addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).isPrimaryKey(true).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f2").dataType(DataType.Bool).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f3").dataType(DataType.Int8).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f4").dataType(DataType.Int16).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f5").dataType(DataType.Int32).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f6").dataType(DataType.Int64).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f7").dataType(DataType.Float).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f8").dataType(DataType.Double).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f9").dataType(DataType.VarChar).maxLength(1024).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f10").dataType(DataType.Array).maxCapacity(1024).elementType(DataType.VarChar).maxLength(1024).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f10_2").dataType(DataType.Array).maxCapacity(1024).elementType(DataType.Int32).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("f11").dataType(DataType.JSON).isNullable(true).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("vec_dense").dataType(DataType.FloatVector).dimension(8).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("vec_binary").dataType(DataType.BinaryVector).dimension(16).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("vec_sparse").dataType(DataType.SparseFloatVector).dimension(5).build());
        collectionSchema.addField(AddFieldReq.builder().fieldName("vec_fp16").dataType(DataType.Float16Vector).dimension(16).build());   //
//        collectionSchema.addField(AddFieldReq.builder().fieldName("vec_bf16").dataType(DataType.BFloat16Vector).dimension(16).build());

        List<IndexParam> indexParams = new ArrayList<>();
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("inverted_index_algo", "DAAT_MAXSCORE");
        Map<String, Object> extraParams2 = new HashMap<>();
        extraParams2.put("nlist", 64);
        indexParams.add(IndexParam.builder().fieldName("vec_dense").indexType(IndexParam.IndexType.HNSW).metricType(IndexParam.MetricType.COSINE).build());
        indexParams.add(IndexParam.builder().fieldName("vec_binary").indexType(IndexParam.IndexType.BIN_IVF_FLAT).extraParams(extraParams2).metricType(IndexParam.MetricType.HAMMING).build());
        indexParams.add(IndexParam.builder().fieldName("vec_sparse").indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX).extraParams(extraParams).metricType(IndexParam.MetricType.IP).build());
        indexParams.add(IndexParam.builder().fieldName("vec_fp16").indexType(IndexParam.IndexType.IVF_FLAT).extraParams(extraParams2).metricType(IndexParam.MetricType.COSINE).build());
//        indexParams.add(IndexParam.builder().fieldName("vec_bf16").indexType(IndexParam.IndexType.FLAT).metricType(IndexParam.MetricType.COSINE).build());

        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder().collectionName(COLLECTION_NAME).collectionSchema(collectionSchema).indexParams(indexParams).consistencyLevel(ConsistencyLevel.STRONG).build();
        client.createCollection(createCollectionReq);
        client.loadCollection(LoadCollectionReq.builder().collectionName(COLLECTION_NAME).sync(true).build());
        System.out.printf("Collection '%s' created\n", COLLECTION_NAME);
    }

    private static void insertCollection(MilvusClientV2 client, String COLLECTION_NAME) {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        Random random = new Random();
        for (int i = 0; i < 30; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.addProperty("f2", random.nextBoolean());
            row.addProperty("f3", (byte) (random.nextInt(256) - 128));
            row.addProperty("f4", (short) (random.nextInt(65536) - 32768));
            row.addProperty("f5", random.nextInt());
            row.addProperty("f6", random.nextLong());
            row.addProperty("f7", random.nextFloat());
            row.addProperty("f8", random.nextDouble());
            row.addProperty("f9", UUID.randomUUID().toString());

            List<String> strArray = new ArrayList<>();
            List<Short> shortArray = new ArrayList<>();
            int capacity = random.nextInt(5) + 5;
            for (int k = 0; k < capacity; k++) {
                strArray.add(String.format("string-%d-%d", i, k));
                shortArray.add((short) random.nextInt());
            }
            row.add("f10", JsonUtils.toJsonTree(strArray).getAsJsonArray());
            row.add("f10_2", JsonUtils.toJsonTree(shortArray).getAsJsonArray());

            JsonObject json = new JsonObject();
            json.addProperty("path", String.format("\\root/abc/path%d", i));
            json.addProperty("size", i);
            if (i % 7 == 0) {
                json.addProperty("special", true);
            }
            json.add("flags", gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            row.add("f11", json);

            row.add("vec_dense", gson.toJsonTree(MilvusCommonUtils.generateFloatVector(8)));
            row.add("vec_binary", gson.toJsonTree(MilvusCommonUtils.generateBinaryVector(16).array()));
            row.add("vec_sparse", gson.toJsonTree(MilvusCommonUtils.generateSparseVector()));
            row.add("vec_fp16", gson.toJsonTree(MilvusCommonUtils.generateFloat16Vector(16, false).array()));
//            row.add("vec_bf16", gson.toJsonTree(CommonUtils.generateFloat16Vector(16, true).array()));
            rows.add(row);
        }
        client.insert(InsertReq.builder().collectionName(COLLECTION_NAME).data(rows).build());
        System.out.println("insert data into collection " + COLLECTION_NAME);
    }

    private static void queryCollection(MilvusClientV2 client, String COLLECTION_NAME) {
        List<String> outputList = Arrays.asList("id", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f10_2", "f11", "vec_dense", "vec_binary", "vec_sparse", "vec_fp16");
        QueryResp res = client.query(QueryReq.builder().collectionName(COLLECTION_NAME).filter("").consistencyLevel(ConsistencyLevel.STRONG).outputFields(outputList).limit(5).build());
        List<QueryResp.QueryResult> queryResults = res.getQueryResults();
        System.out.println("=======");
        System.out.println(outputList);

        for (QueryResp.QueryResult result : queryResults) {
            Map<String, Object> entity = result.getEntity();
            for (String field : outputList) {
                if (field.equals("vec_binary") || field.equals("vec_fp16")) {
                    ByteBuffer vector = (ByteBuffer) entity.get(field);
                    vector.rewind();
                    System.out.print("[");
                    for (byte b : vector.array()) {
                        System.out.print(b + " ");
                    }
                    System.out.print("]");
                } else {
                    System.out.print(entity.get(field));
                }
                System.out.print("\t");
            }
            System.out.println("");
        }
        System.out.println("query from collection " + COLLECTION_NAME);
    }

    private static void queryIteratorCollection(MilvusClientV2 client, String COLLECTION_NAME) {
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder().databaseName("default").collectionName(COLLECTION_NAME).consistencyLevel(ConsistencyLevel.STRONG).batchSize(5L).outputFields(Arrays.asList("id", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f10_2", "f11", "vec_dense", "vec_binary", "vec_sparse", "vec_fp16")).build());
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                System.out.println(record);
            }
        }
    }

    private static void queryCollectionBinary(MilvusClientV2 client, String COLLECTION_NAME) {
        List<String> outputList = Arrays.asList("vec_binary", "vec_fp16", "vec_sparse");
        QueryResp res = client.query(QueryReq.builder().collectionName(COLLECTION_NAME).filter("").consistencyLevel(ConsistencyLevel.STRONG).outputFields(outputList).limit(5).build());
        List<QueryResp.QueryResult> queryResults = res.getQueryResults();

        System.out.println(outputList);
        System.out.println("=======");

        for (QueryResp.QueryResult result : queryResults) {
            ByteBuffer vector = (ByteBuffer) result.getEntity().get("vec_binary");
            vector.rewind();
            for (byte b : vector.array()) {
                System.out.print(b + " ");
            }
            System.out.print(" =====(转为二进制)======> ");
            for (byte b : vector.array()) {
                System.out.print(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
            }
            System.out.println();
        }
        System.out.println("=======");
        for (QueryResp.QueryResult result : queryResults) {
            ByteBuffer vector = (ByteBuffer) result.getEntity().get("vec_fp16");
            vector.rewind();
            for (byte b : vector.array()) {
                System.out.print(b + " ");
            }
            vector.rewind();
            System.out.print(" =====(fp16转为float)======> ");
            List<Float> decodedFpVector = MilvusCommonUtils.decodeFloat16Vector(vector, false);
            System.out.println(decodedFpVector);
        }
        System.out.println("=======");
        for (QueryResp.QueryResult result : queryResults) {
            System.out.println(result.getEntity().get("vec_sparse"));
        }
        System.out.println("query Binary from collection " + COLLECTION_NAME);
    }

    private static void dropCollection(MilvusClientV2 client, String collectionName) {
        client.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
    }
}
