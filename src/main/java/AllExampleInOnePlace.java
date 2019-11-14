import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.Key.Expressions;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Function;

public class AllExampleInOnePlace {

    private enum FlowerType {
        ROSE,
        TULIP,
        LILY,
    }

    private static RecordLayerDemoProto.Flower buildFlower(FlowerType type, RecordLayerDemoProto.Color color) {
        return RecordLayerDemoProto.Flower.newBuilder()
                .setType(type.name())
                .setColor(color)
                .build();
    }

    public static void main(String[] args) {
        /*
        An FDBRecordStore requires:

        - A FDBRecordContext, a thin transaction wrapper, gotten from an FDBDatabase instance with openContext().
        - A path for storage (a Subspace or a KeySpacePath). It is up to the application to make this unique.
        - A RecordMetaDataProvider for RecordMetaData describing the records to be stored there.
        */

        // 1.
        // opening a connection to the FDB database
        // The no-argument version of getDatabase will use the default cluster file to connect to FDB
        // If you want to use a different cluster file, you can pass a String with the path to the cluster file to this method instead.
        FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();


        // 2.
        // Define the keyspace for our application
        // The Record Layer provides the KeySpacePath API which allows us to build a logical directory structure for organizing our record stores
        KeySpace keySpace = new KeySpace(new KeySpaceDirectory("record-layer-demo", KeySpaceDirectory.KeyType.STRING, "record-layer-demo"));
        // Get the path where our record store will be rooted
        KeySpacePath path = keySpace.path("record-layer-demo");

        KeySpace keySpaceSomeMore = new KeySpace(new KeySpaceDirectory("record-layer-demo-some-more", KeySpaceDirectory.KeyType.STRING, "record-layer-demo-some-more"));
        KeySpacePath pathSomeMore = keySpaceSomeMore.path("record-layer-demo-some-more");


        // 3.
        // To build the record meta-data, first create the RecordMetaDataBuilder and add the Order record type from our proto definition
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(RecordLayerDemoProto.getDescriptor()); // based on UnionDescriptor message from proto (or RecordTypeUnion)
        // Set the primary key to order_id
        metaDataBuilder.getRecordType("Order")
                .setPrimaryKey(
                        Expressions.concat(
                               Key.Expressions.field("order_id"),
                               Key.Expressions.field("sub_order_id")
                ));
        metaDataBuilder.getRecordType("SomeMore")
                .setPrimaryKey(Key.Expressions.field("id"));
        // Add a secondary index on price
        metaDataBuilder.addIndex("Order", new Index("priceIndex", Key.Expressions.field("price")));
        metaDataBuilder.addIndex("SomeMore", new Index("nameIndex", Key.Expressions.field("name")));
        metaDataBuilder.addIndex("SomeMore", new Index("someIntIndex", Key.Expressions.field("some_int")));

        // 4.
        // We can now create an instance of our record store
        // In the Record Layer, those transactions are wrapped in an FDBRecordContext.
        // The FDBRecordStore object only has the lifetime of a single transaction, so we need to provide an FDBRecordContext each time we create one
        // штуку которая получаетс доступ к записям в базе или записывает в базу но существует только на время траназцкции, а транзакция открвается при помощи метода run
        Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context ->
                FDBRecordStore.newBuilder()
                        .setMetaDataProvider(metaDataBuilder)
                        .setContext(context)
                        .setKeySpacePath(path)
                        .createOrOpen();

        Function<FDBRecordContext, FDBRecordStore> recordStoreProviderSomeMore = context ->
                FDBRecordStore.newBuilder()
                        .setMetaDataProvider(metaDataBuilder)
                        .setContext(context)
                        .setKeySpacePath(pathSomeMore) // очень важно разделить KeySpace !!!!!!
                        .createOrOpen();
        // 5.
        // the run method runs the provided function transactionally against FDB
        // it opens a new transaction and provides it wrapped in an FDBRecordContext as an argument to the function
        // The run method handles opening the transaction for us and attempting to commit the result.
        // If we get a retriable error on commit it will automatically retry for us (up to a configurable number of maximum retry attempts)
        db.run(context -> {
            recordStoreProvider.apply(context).deleteAllRecords();
            return null;
        });

        db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);

            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(1)
                    .setSubOrderId(1)
                    .setPrice(123)
                    .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.RED))
                    .build());
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(1)
                    .setSubOrderId(2)
                    .setPrice(123)
                    .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.RED))
                    .build());

            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(23)
                    .setSubOrderId(23)
                    .setPrice(34)
                    .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.PINK))
                    .build());
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(3)
                    .setSubOrderId(33)
                    .setPrice(55)
                    .setFlower(buildFlower(FlowerType.TULIP, RecordLayerDemoProto.Color.YELLOW))
                    .build());
            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(100)
                    .setSubOrderId(101)
                    .setPrice(9)
                    .setFlower(buildFlower(FlowerType.LILY, RecordLayerDemoProto.Color.RED))
                    .build());

            return null;
        });
        db.run( context -> {
            FDBRecordStore recordStore = recordStoreProviderSomeMore.apply(context);
            recordStore.saveRecord(RecordLayerDemoProto.SomeMore.newBuilder()
                    .setName("first name")
                    .setId(1)
                    .setSomeInt(1)
                    .setColor( RecordLayerDemoProto.Color.RED )
                    .build()
            );
            recordStore.saveRecord(RecordLayerDemoProto.SomeMore.newBuilder()
                    .setName("second name")
                    .setId(2)
                    .setSomeInt(2)
                    .setColor( RecordLayerDemoProto.Color.PINK )
                    .build()
            );
            return null;
        });

        // 6.
        FDBStoredRecord<Message> storedRecord = db.run(context ->
                // load the record
                recordStoreProvider.apply(context).loadRecord(Tuple.from(1).add(1))
        );
        assert storedRecord != null;
        // a record that doesn't exist is null
        FDBStoredRecord<Message> shouldNotExist = db.run(context ->
                recordStoreProvider.apply(context).loadRecord(Tuple.from(99999).add(10))
        );
        assert shouldNotExist == null;

        FDBStoredRecord<Message> storedRecord2 = db.run(context ->
                recordStoreProviderSomeMore.apply(context).loadRecord(Tuple.from(1))
        );

        // The loadRecord method returns an FDBStoredRecord, but we want to reconstruct the original Order as defined in our meta-data.
        RecordLayerDemoProto.Order order = RecordLayerDemoProto.Order.newBuilder()
                .mergeFrom(storedRecord.getRecord())
                .build();
        System.out.println(order);

        RecordLayerDemoProto.SomeMore someMore = RecordLayerDemoProto.SomeMore.newBuilder()
                .mergeFrom(storedRecord2.getRecord())
                .build();
        System.out.println(someMore);

        Function<RecordQuery, List<RecordLayerDemoProto.Order>> RunQuery = query ->
            db.run( context -> {
                FDBRecordStore recordStore = recordStoreProvider.apply(context);
                RecordCursor<FDBQueriedRecord<Message>> coursor = recordStore.executeQuery(query);
                RecordQueryPlan plan = recordStore.planQuery(query);
                return coursor.map(msg ->
                        RecordLayerDemoProto.Order.newBuilder().mergeFrom(msg.getRecord()).build()
                ).asList().join();
            });

        // query on our data
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("Order")
                .setFilter( Query.and(
                        Query.field("price").equalsValue(34),
                        Query.field("flower").matches(Query.field("type").equalsValue(FlowerType.ROSE.name()))))
                .build();

        List<RecordLayerDemoProto.Order> orders = RunQuery.apply(query);
        orders.forEach(System.out::println);

        // query on our data
        RecordQuery query_notgood = RecordQuery.newBuilder()
                .setRecordType("Order")
                .setFilter( Query.and(
                        Query.field("sub_order_id").equalsValue(2),
                        Query.field("order_id").equalsValue(1)))
                .build();
        List<RecordLayerDemoProto.Order> orders_notgood = RunQuery.apply(query_notgood);

    }

}

