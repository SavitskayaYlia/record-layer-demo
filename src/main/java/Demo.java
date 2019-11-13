import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

public class Demo {

    public static void main(String[] args) {
        // opening a connection to the FDB database
        FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();

        // Define the keyspace for our application and metaDataBuilder
        SomeMore someMore = SomeMore.getSomeMore("record-layer-demo-some-more");

        db.run( context -> {
            someMore.RecordStore(context).deleteAllRecords();
            return null;
        });

        db.run( context -> {
            FDBRecordStore recordStore = someMore.RecordStore(context);
            recordStore.saveRecord(someMore.newMassege("name" ,1 , 1, RecordLayerDemoProto.Color.RED));
            recordStore.saveRecord(someMore.newMassege("name 2" , 2, 2, RecordLayerDemoProto.Color.PINK));
            return null;
        });

        FDBStoredRecord<Message> storedRecord = db.run(context ->
                someMore.RecordStore(context).loadRecord(Tuple.from(1))
        );

        RecordLayerDemoProto.SomeMore someRecord = RecordLayerDemoProto.SomeMore.newBuilder()
                .mergeFrom(storedRecord.getRecord())
                .build();
        System.out.println(someRecord);
    }
}
