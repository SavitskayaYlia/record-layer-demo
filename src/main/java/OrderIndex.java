import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.protobuf.Message;

import java.util.List;

public class OrderIndex {
    public static FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();

    private static OrderIndex inst;
    private KeySpace keySpaceSomeMore;
    private KeySpacePath pathSomeMore;
    private RecordMetaDataBuilder metaDataBuilder;

    private OrderIndex(String keySpaceName){
        // path
        keySpaceSomeMore = new KeySpace(new KeySpaceDirectory(keySpaceName, KeySpaceDirectory.KeyType.STRING, keySpaceName));
        pathSomeMore = keySpaceSomeMore.path(keySpaceName);
        // metaData
        metaDataBuilder = RecordMetaData.newBuilder().setRecords(RecordLayerDemoProto.getDescriptor());
        metaDataBuilder.getRecordType("SomeMore").setPrimaryKey(Key.Expressions.field("id"));
        metaDataBuilder.getRecordType("Order").setPrimaryKey(Key.Expressions.field("order_id"));

        metaDataBuilder.getRecordType("OrderIndex")
                .setPrimaryKey(
                        Key.Expressions.concat(
                                Key.Expressions.field("order_id"),
                                Key.Expressions.field("sub_order_id")
                        ));

    };

    public static OrderIndex getOrderIndex(String keySpaceName){
        if (inst == null){
            inst = new OrderIndex(keySpaceName);
        }
        return inst;
    }

    public FDBRecordStore RecordStore(FDBRecordContext context) {
        return FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaDataBuilder)
                .setContext(context)
                .setKeySpacePath(pathSomeMore)
                .createOrOpen();
    }

    public void AddMassege(String name, Integer id, Integer subId ) {
        db.run(context -> {
            Message message = OrderIndexProto.OrderIndex.newBuilder()
                    .setOrderId(id)
                    .setSubOrderId(subId)
                    .setName(name)
                    .build();
            RecordStore(context).saveRecord(message);
            return null;
        });
    }


    public void Delete() {
        db.run(context -> {
            RecordStore(context).deleteAllRecords();
            return null;
        });
    }

    public List<OrderIndexProto.OrderIndex> Exacute(RecordQuery query) {
       return  db.run(context -> {
            RecordCursor<FDBQueriedRecord<Message>> coursor = RecordStore(context).executeQuery(query);
            RecordQueryPlan plan = RecordStore(context).planQuery(query);

            System.out.println("\nQuery: " + query.getFilter().toString());
            System.out.println("Plan:   ");
            System.out.println("   plan: " + plan);
            System.out.println("   hasRecordScan: " + plan.hasRecordScan());
            System.out.println("   hasFullRecordScan: " + plan.hasFullRecordScan());

            return coursor.map(msg ->
                    OrderIndexProto.OrderIndex.newBuilder().mergeFrom(msg.getRecord()).build()
            ).asList().join();
        });
    }

}

