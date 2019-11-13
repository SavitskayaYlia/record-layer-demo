import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.google.protobuf.Message;

public class SomeMore {
    private static  SomeMore inst;
    private KeySpace keySpaceSomeMore;
    private KeySpacePath pathSomeMore;
    private RecordMetaDataBuilder metaDataBuilder;

    private SomeMore( String keySpaceName){
        // path
        keySpaceSomeMore = new KeySpace(new KeySpaceDirectory(keySpaceName, KeySpaceDirectory.KeyType.STRING, keySpaceName));
        pathSomeMore = keySpaceSomeMore.path(keySpaceName);
        // metaData
        metaDataBuilder = RecordMetaData.newBuilder().setRecords(RecordLayerDemoProto.getDescriptor());
        metaDataBuilder.getRecordType("SomeMore").setPrimaryKey(Key.Expressions.field("id"));
        metaDataBuilder.getRecordType("Order").setPrimaryKey(Key.Expressions.field("order_id"));
        metaDataBuilder.addIndex("SomeMore", new Index("nameIndex", Key.Expressions.field("name")));
        metaDataBuilder.addIndex("SomeMore", new Index("someIntIndex", Key.Expressions.field("some_int")));
    };

    public static SomeMore getSomeMore( String keySpaceName){
        if (inst == null){
            inst = new SomeMore(keySpaceName);
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
    public Message newMassege(String name, Integer id, Integer someInt, RecordLayerDemoProto.Color color){
        return RecordLayerDemoProto.SomeMore.newBuilder()
                .setName(name)
                .setId(id)
                .setSomeInt(someInt)
                .setColor(color)
                .build();
    }

 }
